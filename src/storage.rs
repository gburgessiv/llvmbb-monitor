use crate::Email;

use std::cmp::min;
use std::time::Duration;

use anyhow::{bail, Result};
use rusqlite::params;
use serenity::model::prelude::UserId;

fn userid_to_db(uid: UserId) -> i64 {
    uid.0 as i64
}

fn db_to_userid(uid: i64) -> UserId {
    UserId(uid as u64)
}

pub(crate) struct Storage {
    conn: rusqlite::Connection,
}

impl Storage {
    fn init_db(conn: rusqlite::Connection) -> Result<Self> {
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

        // Arbitrary busy handler, but should be good enough, especially given that we only ever
        // use this db behind a mutex anyway.
        conn.busy_handler(Some(|times_waited: i32| {
            // 100 is arbitrary, but if we go above it, something's terribly wrong.
            let keep_waiting = times_waited < 100;
            if keep_waiting {
                let wait_time = Duration::from_millis(1) * (times_waited as u32);
                let max_wait_time = Duration::from_millis(10);
                std::thread::sleep(min(wait_time, max_wait_time));
            }
            keep_waiting
        }))?;

        Ok(Self { conn })
    }

    #[cfg(test)]
    fn from_memory() -> Result<Self> {
        Self::init_db(rusqlite::Connection::open_in_memory()?)
    }

    pub(crate) fn from_file(file_path: &str) -> Result<Self> {
        Self::init_db(rusqlite::Connection::open(file_path)?)
    }

    pub(crate) fn add_user_email_mapping(&self, id: UserId, email: &Email) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO email_mappings (user_id, email) VALUES (?, ?)",
            params![userid_to_db(id), email.address()],
        )?;
        Ok(())
    }

    pub(crate) fn find_userids_for(&self, email: &Email) -> Result<Vec<UserId>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT user_id FROM email_mappings WHERE email = ?")?;
        let iter = stmt.query_map(params![email.address()], |row| {
            let val: i64 = row.get(0)?;
            Ok(db_to_userid(val))
        })?;

        let mut result = Vec::new();
        for elem in iter {
            result.push(elem?);
        }
        Ok(result)
    }

    pub(crate) fn find_emails_for(&self, id: UserId) -> Result<Vec<Email>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT email FROM email_mappings WHERE user_id = ?")?;
        let iter = stmt.query_map(params![userid_to_db(id)], |row| {
            let val: String = row.get(0)?;
            Ok(val)
        })?;

        let mut result = Vec::new();
        for elem in iter {
            let elem = elem?;
            result.push(match Email::parse(&elem) {
                Some(x) => x,
                None => bail!("Invalid email address in db: {:?}", elem),
            });
        }
        Ok(result)
    }

    pub(crate) fn remove_userid_mapping(&self, id: UserId, email: &Email) -> Result<bool> {
        let num_deleted = self.conn.execute(
            "DELETE FROM email_mappings WHERE user_id = ? AND email = ?",
            params![userid_to_db(id), email.address()],
        )?;
        Ok(num_deleted != 0)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_userid_association_empty_queries() {
        let storage = Storage::from_memory().expect("Failed making in-memory db");

        for with_row in &[false, true] {
            if *with_row {
                storage
                    .add_user_email_mapping(
                        db_to_userid(100),
                        &Email::parse("the_email@bar.com").expect("parsing email"),
                    )
                    .expect("adding mapping");
            }

            {
                let userids = storage
                    .find_userids_for(&Email::parse("foo@bar.com").expect("broken email"))
                    .expect("failed fetching userids");
                assert!(userids.is_empty());
            }

            {
                let emails = storage
                    .find_emails_for(db_to_userid(1))
                    .expect("failed fetching emails");
                assert!(emails.is_empty());
            }
        }
    }

    #[test]
    fn test_userid_mapping_one_to_one_works() {
        let storage = Storage::from_memory().expect("Failed making in-memory db");
        let email = Email::parse("foo@bar.com").expect("broken email");
        let id = db_to_userid(123);
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");

        {
            let userids = storage
                .find_userids_for(&email)
                .expect("failed fetching userids");
            assert_eq!(&userids, &[id]);
        }

        {
            let emails = storage.find_emails_for(id).expect("failed fetching emails");
            assert_eq!(&emails, &[email]);
        }
    }

    #[test]
    fn test_removal_reports_successful_removals() {
        let storage = Storage::from_memory().expect("Failed making in-memory db");
        let email = Email::parse("foo@bar.com").expect("broken email");
        let id = db_to_userid(123);
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");

        let removed = storage
            .remove_userid_mapping(db_to_userid(321), &email)
            .expect("removal of nonexistent entry failed");
        assert!(!removed);

        {
            let userids = storage
                .find_userids_for(&email)
                .expect("failed fetching userids");
            assert_eq!(&userids, &[id]);
        }

        let removed = storage
            .remove_userid_mapping(id, &email)
            .expect("removal of existing entry failed");
        assert!(removed);

        {
            let userids = storage
                .find_userids_for(&email)
                .expect("failed fetching userids");
            assert!(userids.is_empty());
        }
    }

    #[test]
    fn test_multiple_identical_mappings_work_silently() {
        let storage = Storage::from_memory().expect("Failed making in-memory db");
        let email = Email::parse("foo@bar.com").expect("broken email");
        let id = db_to_userid(123);
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");

        {
            let userids = storage
                .find_userids_for(&email)
                .expect("failed fetching userids");
            assert_eq!(&userids, &[id]);
        }

        {
            let emails = storage.find_emails_for(id).expect("failed fetching emails");
            assert_eq!(&emails, &[email]);
        }
    }

    #[test]
    fn test_userid_mapping_many_to_many_works() {
        let storage = Storage::from_memory().expect("Failed making in-memory db");
        let emails = [
            Email::parse("0@bar.com").expect("broken email"),
            Email::parse("1@bar.com").expect("broken email"),
        ];

        let ids = [db_to_userid(123), db_to_userid(321)];
        for email in &emails {
            for id in &ids {
                storage
                    .add_user_email_mapping(*id, email)
                    .expect("adding mapping");
            }
        }

        for email in &emails {
            let db_ids = storage
                .find_userids_for(email)
                .expect("failed fetching userids");
            assert_eq!(&db_ids, &ids);
        }

        for id in &ids {
            let db_emails = storage
                .find_emails_for(*id)
                .expect("failed fetching emails");
            assert_eq!(&db_emails, &emails);
        }
    }
}
