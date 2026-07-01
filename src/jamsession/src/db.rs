use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use toasty::stmt::{List, Query};

const CURRENT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, toasty::Model)]
pub struct Session {
    #[key]
    pub id: String,
    pub cwd: String,
    pub created_at: String,
    pub updated_at: String,

    #[has_many]
    pub messages: toasty::Deferred<Vec<Message>>,
}

#[derive(Debug, toasty::Model)]
pub struct Message {
    #[key]
    #[auto]
    pub id: u64,

    #[index]
    pub session_id: String,

    #[belongs_to(key = session_id, references = id)]
    pub session: toasty::Deferred<Session>,

    pub payload: String,
}

#[derive(Debug, Clone)]
pub struct SessionRecord {
    pub session_id: String,
    pub cwd: PathBuf,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct Store {
    path: Option<PathBuf>,
    db: toasty::Db,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceDirection {
    ClientToDaemon,
    DaemonToAgent,
    AgentToDaemon,
    DaemonToClient,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceKind {
    Request,
    Response,
    Notification,
    Event,
}

#[derive(Debug, Clone)]
pub struct NewTrace {
    pub session_id: Option<String>,
    pub dir: TraceDirection,
    pub role: Option<String>,
    pub kind: TraceKind,
    pub method: Option<String>,
    pub request_id: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceRecord {
    pub id: u64,
    pub ts: DateTime<Utc>,
    pub session_id: Option<String>,
    pub dir: TraceDirection,
    pub role: Option<String>,
    pub kind: TraceKind,
    pub method: Option<String>,
    pub request_id: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Default)]
pub struct TraceQuery {
    pub after_id: Option<u64>,
    pub session_id: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub method: Option<String>,
    pub dir: Option<TraceDirection>,
    pub limit: Option<u32>,
}

impl Store {
    pub async fn open(path: &Path) -> crate::error::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let needs_schema = configure_sqlite_file(path)?;

        let db = toasty::Db::builder()
            .models(toasty::models!(Session, Message))
            .build(toasty_driver_sqlite::Sqlite::open(path))
            .await?;
        if needs_schema {
            db.push_schema().await?;
        }

        Ok(Self {
            path: Some(path.to_path_buf()),
            db,
        })
    }

    pub async fn in_memory() -> crate::error::Result<Self> {
        let db = toasty::Db::builder()
            .models(toasty::models!(Session, Message))
            .build(toasty_driver_sqlite::Sqlite::in_memory())
            .await?;
        db.push_schema().await?;

        Ok(Self { path: None, db })
    }

    pub async fn list_sessions(
        &self,
        cwd: Option<&Path>,
    ) -> crate::error::Result<Vec<SessionRecord>> {
        let mut db = self.db.clone();
        let sessions = match cwd {
            Some(cwd) => {
                let cwd = cwd.to_string_lossy().into_owned();
                Session::filter(Session::fields().cwd().eq(cwd))
                    .exec(&mut db)
                    .await?
            }
            None => Query::<List<Session>>::all().exec(&mut db).await?,
        };

        sessions.into_iter().map(SessionRecord::try_from).collect()
    }

    pub async fn get_session(
        &self,
        session_id: &str,
    ) -> crate::error::Result<Option<SessionRecord>> {
        let mut db = self.db.clone();
        let session = Session::get_by_id(&mut db, session_id).await?;
        SessionRecord::try_from(session).map(Some)
    }

    pub async fn add_session(&self, session_id: &str, cwd: &Path) -> crate::error::Result<()> {
        let now = Utc::now().to_rfc3339();
        let cwd = cwd.to_string_lossy().into_owned();
        let mut db = self.db.clone();

        toasty::create!(Session {
            id: session_id.to_string(),
            cwd,
            created_at: now.clone(),
            updated_at: now,
        })
        .exec(&mut db)
        .await?;

        Ok(())
    }

    pub async fn append_message(
        &self,
        session_id: &str,
        payload: &serde_json::Value,
    ) -> crate::error::Result<()> {
        let payload = serde_json::to_string(payload)?;
        let mut db = self.db.clone();

        toasty::create!(Message {
            session_id: session_id.to_string(),
            payload,
        })
        .exec(&mut db)
        .await?;
        toasty::update!(Session::filter_by_id(session_id) {
            updated_at: Utc::now().to_rfc3339()
        })
        .exec(&mut db)
        .await?;

        Ok(())
    }

    pub async fn messages_for_session(
        &self,
        session_id: &str,
    ) -> crate::error::Result<Vec<serde_json::Value>> {
        let mut db = self.db.clone();
        let query = Message::filter(Message::fields().session_id().eq(session_id))
            .order_by(Message::fields().id().asc());

        query
            .exec(&mut db)
            .await?
            .into_iter()
            .map(|message| serde_json::from_str(&message.payload).map_err(Into::into))
            .collect()
    }

    pub async fn remove_session(&self, session_id: &str) -> crate::error::Result<()> {
        let mut db = self.db.clone();

        if let Some(path) = &self.path {
            remove_traces_for_session(path, session_id)?;
        }

        Message::filter(Message::fields().session_id().eq(session_id))
            .delete()
            .exec(&mut db)
            .await?;
        Session::filter(Session::fields().id().eq(session_id))
            .delete()
            .exec(&mut db)
            .await?;

        Ok(())
    }

    pub fn record_trace(&self, trace: NewTrace) -> crate::error::Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };

        let connection = rusqlite::Connection::open(path)?;
        insert_trace(&connection, trace)
    }

    pub fn traces(&self, query: TraceQuery) -> crate::error::Result<Vec<TraceRecord>> {
        let Some(path) = &self.path else {
            return Ok(Vec::new());
        };

        let connection = rusqlite::Connection::open(path)?;
        select_traces(&connection, query)
    }
}

impl TryFrom<Session> for SessionRecord {
    type Error = crate::error::Error;

    fn try_from(session: Session) -> Result<Self, Self::Error> {
        Ok(Self {
            session_id: session.id,
            cwd: PathBuf::from(session.cwd),
            created_at: DateTime::parse_from_rfc3339(&session.created_at)?.with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(&session.updated_at)?.with_timezone(&Utc),
        })
    }
}

fn configure_sqlite_file(path: &Path) -> crate::error::Result<bool> {
    let connection = rusqlite::Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    let needs_schema = !connection.query_row(
        "SELECT EXISTS (
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'sessions'
        )",
        [],
        |row| row.get::<_, bool>(0),
    )?;
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL
        )",
        [],
    )?;
    connection.execute(
        "INSERT INTO schema_version (id, version)
         VALUES (1, 1)
         ON CONFLICT(id) DO NOTHING",
        [],
    )?;
    let schema_version = connection.query_row(
        "SELECT version FROM schema_version WHERE id = 1",
        [],
        |row| row.get::<_, u32>(0),
    )?;
    match schema_version {
        1 => {
            create_trace_schema(&connection)?;
            connection.execute(
                "UPDATE schema_version SET version = ?1 WHERE id = 1",
                [CURRENT_SCHEMA_VERSION],
            )?;
        }
        CURRENT_SCHEMA_VERSION => {
            create_trace_schema(&connection)?;
        }
        found => {
            return Err(crate::error::Error::SchemaVersion {
                found,
                expected: CURRENT_SCHEMA_VERSION,
            });
        }
    }
    Ok(needs_schema)
}

fn create_trace_schema(connection: &rusqlite::Connection) -> crate::error::Result<()> {
    connection.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS traces (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            session_id TEXT,
            dir TEXT NOT NULL,
            role TEXT,
            kind TEXT NOT NULL,
            method TEXT,
            request_id TEXT,
            payload TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_traces_session ON traces(session_id);
        CREATE INDEX IF NOT EXISTS idx_traces_ts ON traces(ts);
        ",
    )?;
    Ok(())
}

fn insert_trace(connection: &rusqlite::Connection, trace: NewTrace) -> crate::error::Result<()> {
    connection.execute(
        "INSERT INTO traces (ts, session_id, dir, role, kind, method, request_id, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        (
            Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            trace.session_id,
            trace.dir.as_str(),
            trace.role,
            trace.kind.as_str(),
            trace.method,
            trace.request_id,
            serde_json::to_string(&trace.payload)?,
        ),
    )?;
    Ok(())
}

fn remove_traces_for_session(path: &Path, session_id: &str) -> crate::error::Result<()> {
    let connection = rusqlite::Connection::open(path)?;
    connection.execute("DELETE FROM traces WHERE session_id = ?1", [session_id])?;
    Ok(())
}

fn select_traces(
    connection: &rusqlite::Connection,
    query: TraceQuery,
) -> crate::error::Result<Vec<TraceRecord>> {
    let mut sql = String::from(
        "SELECT id, ts, session_id, dir, role, kind, method, request_id, payload FROM traces",
    );
    let mut predicates = Vec::new();
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(after_id) = query.after_id {
        predicates.push("id > ?");
        params.push(Box::new(i64::try_from(after_id).unwrap_or(i64::MAX)));
    }
    if let Some(session_id) = query.session_id {
        predicates.push("session_id = ?");
        params.push(Box::new(session_id));
    }
    if let Some(since) = query.since {
        predicates.push("ts >= ?");
        params.push(Box::new(
            since.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        ));
    }
    if let Some(method) = query.method {
        predicates.push("method = ?");
        params.push(Box::new(method));
    }
    if let Some(dir) = query.dir {
        predicates.push("dir = ?");
        params.push(Box::new(dir.as_str().to_string()));
    }

    if !predicates.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&predicates.join(" AND "));
    }
    sql.push_str(" ORDER BY id ASC LIMIT ?");
    params.push(Box::new(query.limit.unwrap_or(500).min(5000)));

    let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = connection.prepare(&sql)?;
    let rows = stmt.query_map(param_refs.as_slice(), trace_record_from_row)?;

    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn trace_record_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TraceRecord> {
    let ts: String = row.get(1)?;
    let dir: String = row.get(3)?;
    let kind: String = row.get(5)?;
    let payload: String = row.get(8)?;

    Ok(TraceRecord {
        id: row.get::<_, i64>(0)?.try_into().map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                Box::new(e),
            )
        })?,
        ts: DateTime::parse_from_rfc3339(&ts)
            .map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?
            .with_timezone(&Utc),
        session_id: row.get(2)?,
        dir: TraceDirection::parse(&dir).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })?,
        role: row.get(4)?,
        kind: TraceKind::parse(&kind).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, Box::new(e))
        })?,
        method: row.get(6)?,
        request_id: row.get(7)?,
        payload: serde_json::from_str(&payload).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(8, rusqlite::types::Type::Text, Box::new(e))
        })?,
    })
}

impl TraceDirection {
    fn as_str(&self) -> &'static str {
        match self {
            Self::ClientToDaemon => "client_to_daemon",
            Self::DaemonToAgent => "daemon_to_agent",
            Self::AgentToDaemon => "agent_to_daemon",
            Self::DaemonToClient => "daemon_to_client",
            Self::Internal => "internal",
        }
    }

    pub fn parse(value: &str) -> std::result::Result<Self, TraceParseError> {
        match value {
            "client_to_daemon" => Ok(Self::ClientToDaemon),
            "daemon_to_agent" => Ok(Self::DaemonToAgent),
            "agent_to_daemon" => Ok(Self::AgentToDaemon),
            "daemon_to_client" => Ok(Self::DaemonToClient),
            "internal" => Ok(Self::Internal),
            _ => Err(TraceParseError(value.to_string())),
        }
    }
}

impl TraceKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Response => "response",
            Self::Notification => "notification",
            Self::Event => "event",
        }
    }

    fn parse(value: &str) -> std::result::Result<Self, TraceParseError> {
        match value {
            "request" => Ok(Self::Request),
            "response" => Ok(Self::Response),
            "notification" => Ok(Self::Notification),
            "event" => Ok(Self::Event),
            _ => Err(TraceParseError(value.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct TraceParseError(String);

impl std::fmt::Display for TraceParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown trace enum value: {}", self.0)
    }
}

impl std::error::Error for TraceParseError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn records_and_filters_traces() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("jamsession.db"))
            .await
            .unwrap();

        store
            .record_trace(NewTrace {
                session_id: Some("sess-1".to_string()),
                dir: TraceDirection::ClientToDaemon,
                role: Some("acp-client".to_string()),
                kind: TraceKind::Request,
                method: Some("session/prompt".to_string()),
                request_id: Some("7".to_string()),
                payload: serde_json::json!({"prompt": "hello"}),
            })
            .unwrap();
        store
            .record_trace(NewTrace {
                session_id: Some("sess-2".to_string()),
                dir: TraceDirection::Internal,
                role: Some("daemon".to_string()),
                kind: TraceKind::Event,
                method: Some("session_created".to_string()),
                request_id: None,
                payload: serde_json::json!({}),
            })
            .unwrap();

        let traces = store
            .traces(TraceQuery {
                session_id: Some("sess-1".to_string()),
                ..TraceQuery::default()
            })
            .unwrap();

        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].dir, TraceDirection::ClientToDaemon);
        assert_eq!(traces[0].kind, TraceKind::Request);
        assert_eq!(traces[0].method.as_deref(), Some("session/prompt"));
        assert_eq!(traces[0].payload, serde_json::json!({"prompt": "hello"}));
    }

    #[tokio::test]
    async fn deleting_session_removes_its_traces() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = Store::open(&dir.path().join("jamsession.db"))
            .await
            .unwrap();
        store.add_session("sess-1", dir.path()).await.unwrap();
        store
            .record_trace(NewTrace {
                session_id: Some("sess-1".to_string()),
                dir: TraceDirection::Internal,
                role: Some("daemon".to_string()),
                kind: TraceKind::Event,
                method: Some("session_created".to_string()),
                request_id: None,
                payload: serde_json::json!({}),
            })
            .unwrap();

        store.remove_session("sess-1").await.unwrap();

        assert!(store.traces(TraceQuery::default()).unwrap().is_empty());
    }

    #[tokio::test]
    async fn open_writes_current_schema_version() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("jamsession.db");
        Store::open(&path).await.unwrap();

        let connection = rusqlite::Connection::open(path).unwrap();
        let version = connection
            .query_row(
                "SELECT version FROM schema_version WHERE id = 1",
                [],
                |row| row.get::<_, u32>(0),
            )
            .unwrap();

        assert_eq!(version, CURRENT_SCHEMA_VERSION);
    }
}
