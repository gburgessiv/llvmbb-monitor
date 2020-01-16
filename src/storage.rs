use crate::Email;
use crate::FailureOr;

use serenity::model::prelude::UserId;

fn userid_to_db(uid: UserId) -> u64 {
    uid.0
}

fn db_to_userid(uid: u64) -> UserId {
    UserId(uid)
}

pub(crate) struct Storage {
    conn: rusqlite::Connection,
}

// FIXME: Add a loop while we get sqlite3's unlocked complaints, unless rusqlite gives us that for
// free.
impl Storage {
    fn init_db(conn: rusqlite::Connection) -> FailureOr<Self> {
        // NOTE: we arguably over-index things, since this stuff is going to be called on discord's
        // "main" threads. In practice, this shouldn't be a problem, but having things on the open
        // internet always makes me a bit sketched out.
        //
        // If I really cared, I'd add per-user limits and such, but that's efforttt.
        conn.execute_batch(concat!(
            // Standard boilerplate so this doesn't bite me if we start using FKs at some point.
            "PRAGMA foreign_keys=1;",

            // We explicitly support a many:many mapping between user_ids <-> emails.
            "CREATE TABLE IF NOT EXISTS email_mappings(",
            "    user_id INTEGER NOT NULL,",
            "    email TEXT NOT NULL,",
            "    UNIQUE(user_id, email)",
            ");",
            "CREATE INDEX IF NOT EXISTS email_mappings_user_id_index",
            "    ON email_mappings(user_id);",
            "CREATE INDEX IF NOT EXISTS email_mappings_email_index",
            "    ON email_mappings(email);",
        ))?;
        Ok(Self{conn})
    }

    #[cfg(test)]
    fn from_memory() -> FailureOr<Self> {
        Self::init_db(rusqlite::Connection::open_in_memory()?)
    }

    pub(crate) fn from_file(file_path: &str) -> FailureOr<Self> {
        Self::init_db(rusqlite::Connection::open(file_path)?)
    }

    pub(crate) fn add_user_email_mapping(&self, id: UserId) -> FailureOr<()> {
        // FIXME: Handle the UNIQUE constraint gracefully.
        unimplemented!();
    }

    pub(crate) fn find_userids_for(&self, emails: &[Email]) -> FailureOr<Vec<UserId>> {
        unimplemented!();
    }

    pub(crate) fn find_emails_for(&self, id: UserId) -> FailureOr<Vec<Email>> {
        unimplemented!();
    }

    pub(crate) fn remove_userid_mapping(&self, id: UserId, email: Email) -> FailureOr<bool> {
        unimplemented!();
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_db_creation_works() {
        if let Err(x) = Storage::from_memory() {
            panic!("Unexpected error making db: {}", x);
        }
    }
}
