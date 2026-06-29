use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use tracing::field::{Field, Visit};
use tracing::span;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// T043: A tracing layer that routes log events to per-session log files.
/// Events emitted within a span containing `session_id` are written to
/// `~/.jamsession/sessions/<session_id>/session.log` in addition to the main daemon log.
pub struct SessionFileLayer {
    base_dir: PathBuf,
    writers: Mutex<HashMap<String, fs::File>>,
}

impl Default for SessionFileLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionFileLayer {
    pub fn new() -> Self {
        let base_dir = dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".jamsession")
            .join("sessions");
        Self::new_with_base(base_dir)
    }

    pub fn new_with_base(base_dir: PathBuf) -> Self {
        let _ = fs::create_dir_all(&base_dir);
        Self {
            base_dir,
            writers: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create_writer(&self, session_id: &str) -> Option<fs::File> {
        let mut writers = self.writers.lock().ok()?;
        if let Some(file) = writers.get(session_id) {
            return file.try_clone().ok();
        }

        let session_dir = self.base_dir.join(session_id);
        fs::create_dir_all(&session_dir).ok()?;
        let log_path = session_dir.join("session.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .ok()?;
        writers.insert(session_id.to_string(), file.try_clone().ok()?);
        Some(file)
    }
}

struct SessionIdVisitor {
    session_id: Option<String>,
}

impl Visit for SessionIdVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "session_id" {
            self.session_id = Some(value.to_string());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "session_id" {
            self.session_id = Some(format!("{value:?}").trim_matches('"').to_string());
        }
    }
}

impl<S> Layer<S> for SessionFileLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        // Walk up the span tree to find a session_id field
        let mut session_id = None;

        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                let extensions = span.extensions();
                if let Some(fields) = extensions.get::<SessionFields>() {
                    session_id = Some(fields.session_id.clone());
                    break;
                }
            }
        }

        // Also check the event itself for session_id
        if session_id.is_none() {
            let mut visitor = SessionIdVisitor { session_id: None };
            event.record(&mut visitor);
            session_id = visitor.session_id;
        }

        let Some(session_id) = session_id else {
            return;
        };

        let Some(mut file) = self.get_or_create_writer(&session_id) else {
            return;
        };

        // Format the event simply
        let metadata = event.metadata();
        let mut message_visitor = MessageVisitor {
            message: String::new(),
        };
        event.record(&mut message_visitor);

        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");
        let _ = writeln!(
            file,
            "{now} [{level}] {target}: {msg}",
            level = metadata.level(),
            target = metadata.target(),
            msg = message_visitor.message,
        );
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let mut visitor = SessionIdVisitor { session_id: None };
        attrs.record(&mut visitor);

        if let Some(session_id) = visitor.session_id
            && let Some(span) = ctx.span(id)
        {
            span.extensions_mut().insert(SessionFields { session_id });
        }
    }
}

struct SessionFields {
    session_id: String,
}

struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else if !self.message.is_empty() {
            self.message.push_str(&format!(" {field}={value}"));
        } else {
            self.message = format!("{field}={value}");
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}").trim_matches('"').to_string();
        } else if !self.message.is_empty() {
            self.message.push_str(&format!(" {field}={value:?}"));
        } else {
            self.message = format!("{field}={value:?}");
        }
    }
}
