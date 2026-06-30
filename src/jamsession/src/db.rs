use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use toasty::stmt::{List, Query};

const CURRENT_SCHEMA_VERSION: u32 = 1;

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
    db: toasty::Db,
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

        Ok(Self { db })
    }

    pub async fn in_memory() -> crate::error::Result<Self> {
        let db = toasty::Db::builder()
            .models(toasty::models!(Session, Message))
            .build(toasty_driver_sqlite::Sqlite::in_memory())
            .await?;
        db.push_schema().await?;

        Ok(Self { db })
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
    if schema_version != CURRENT_SCHEMA_VERSION {
        return Err(crate::error::Error::SchemaVersion {
            found: schema_version,
            expected: CURRENT_SCHEMA_VERSION,
        });
    }
    Ok(needs_schema)
}
