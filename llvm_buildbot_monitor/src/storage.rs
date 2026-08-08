use crate::Email;

use std::cmp::min;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Result, bail};
use rusqlite::params;
use serenity::model::prelude::UserId;

fn userid_to_db(uid: UserId) -> i64 {
    uid.get() as i64
}

fn db_to_userid(uid: i64) -> UserId {
    UserId::new(uid as u64)
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) struct EmailMappingsEpoch(u64);

pub(crate) struct Storage {
    conn: rusqlite::Connection,
    epoch: u64,
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
            "CREATE TABLE IF NOT EXISTS sent_calendar_pings(",
            "    event_id TEXT NOT NULL PRIMARY KEY",
            ");",
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

        Ok(Self { conn, epoch: 0 })
    }

    #[cfg(test)]
    fn from_memory() -> Result<Self> {
        Self::init_db(rusqlite::Connection::open_in_memory()?)
    }

    pub(crate) fn from_file(file_path: &str) -> Result<Self> {
        Self::init_db(rusqlite::Connection::open(file_path)?)
    }

    pub(crate) fn email_mappings_epoch(&self) -> EmailMappingsEpoch {
        EmailMappingsEpoch(self.epoch)
    }

    pub(crate) fn add_user_email_mapping(&mut self, id: UserId, email: &Email) -> Result<()> {
        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO email_mappings (user_id, email) VALUES (?, ?)",
            params![userid_to_db(id), email.address()],
        )?;
        if inserted > 0 {
            self.epoch += 1;
        }
        Ok(())
    }

    pub(crate) fn fetch_all_email_userids_mappings(
        &mut self,
    ) -> Result<HashMap<Email, Vec<UserId>>> {
        let mut stmt = self
            .conn
            .prepare_cached("SELECT email, user_id FROM email_mappings")?;
        let iter = stmt.query_map(params![], |row| {
            let email_str: String = row.get(0)?;
            let uid: i64 = row.get(1)?;
            Ok((email_str, db_to_userid(uid)))
        })?;

        let mut result: HashMap<Email, Vec<UserId>> = HashMap::new();
        for elem in iter {
            let (email_str, uid) = elem?;
            if let Some(email) = Email::parse(&email_str) {
                result.entry(email).or_default().push(uid);
            }
        }
        Ok(result)
    }

    pub(crate) fn find_emails_for(&mut self, id: UserId) -> Result<Vec<Email>> {
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
            let Some(x) = Email::parse(&elem) else {
                bail!("Invalid email address in db: {:?}", elem);
            };
            result.push(x);
        }
        Ok(result)
    }

    pub(crate) fn remove_userid_mapping(&mut self, id: UserId, email: &Email) -> Result<bool> {
        let num_deleted = self.conn.execute(
            "DELETE FROM email_mappings WHERE user_id = ? AND email = ?",
            params![userid_to_db(id), email.address()],
        )?;
        if num_deleted > 0 {
            self.epoch += 1;
        }
        Ok(num_deleted != 0)
    }

    pub(crate) fn add_sent_calendar_ping(&mut self, calendar_event_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO sent_calendar_pings (event_id) VALUES (?)",
            params![calendar_event_id],
        )?;
        Ok(())
    }

    pub(crate) fn load_all_sent_calendar_pings(&mut self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT event_id FROM sent_calendar_pings")?;
        let iter = stmt.query_map(params![], |row| {
            let val: String = row.get(0)?;
            Ok(val)
        })?;

        let mut result = Vec::new();
        for elem in iter {
            result.push(elem?);
        }
        Ok(result)
    }

    pub(crate) fn remove_sent_calendar_ping(&mut self, calendar_event_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM sent_calendar_pings WHERE event_id = ?",
            params![calendar_event_id],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_userid_association_empty_queries() {
        let mut storage = Storage::from_memory().expect("Failed making in-memory db");

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
                let mappings = storage
                    .fetch_all_email_userids_mappings()
                    .expect("failed fetching userids");
                assert!(
                    !mappings.contains_key(&Email::parse("foo@bar.com").expect("broken email"))
                );
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
        let mut storage = Storage::from_memory().expect("Failed making in-memory db");
        let email = Email::parse("foo@bar.com").expect("broken email");
        let id = db_to_userid(123);
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");

        {
            let mappings = storage
                .fetch_all_email_userids_mappings()
                .expect("failed fetching userids");
            assert_eq!(mappings.get(&email), Some(&vec![id]));
        }

        {
            let emails = storage.find_emails_for(id).expect("failed fetching emails");
            assert_eq!(&emails, &[email]);
        }
    }

    #[test]
    fn test_removal_reports_successful_removals() {
        let mut storage = Storage::from_memory().expect("Failed making in-memory db");
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
            let mappings = storage
                .fetch_all_email_userids_mappings()
                .expect("failed fetching userids");
            assert_eq!(mappings.get(&email), Some(&vec![id]));
        }

        let removed = storage
            .remove_userid_mapping(id, &email)
            .expect("removal of existing entry failed");
        assert!(removed);

        {
            let mappings = storage
                .fetch_all_email_userids_mappings()
                .expect("failed fetching userids");
            assert!(!mappings.contains_key(&email));
        }
    }

    #[test]
    fn test_multiple_identical_mappings_work_silently() {
        let mut storage = Storage::from_memory().expect("Failed making in-memory db");
        let email = Email::parse("foo@bar.com").expect("broken email");
        let id = db_to_userid(123);
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");
        storage
            .add_user_email_mapping(id, &email)
            .expect("adding mapping");

        {
            let mappings = storage
                .fetch_all_email_userids_mappings()
                .expect("failed fetching userids");
            assert_eq!(mappings.get(&email), Some(&vec![id]));
        }

        {
            let emails = storage.find_emails_for(id).expect("failed fetching emails");
            assert_eq!(&emails, &[email]);
        }
    }

    #[test]
    fn test_userid_mapping_many_to_many_works() {
        let mut storage = Storage::from_memory().expect("Failed making in-memory db");
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

        let mappings = storage
            .fetch_all_email_userids_mappings()
            .expect("failed fetching userids");
        for email in &emails {
            assert_eq!(mappings.get(email), Some(&ids.to_vec()));
        }

        for id in &ids {
            let db_emails = storage
                .find_emails_for(*id)
                .expect("failed fetching emails");
            assert_eq!(&db_emails, &emails);
        }
    }

    #[test]
    fn test_calendar_ping_id_operations() {
        let ids = ["a", "b", "c"];

        let mut storage = Storage::from_memory().expect("Failed making in-memory db");
        assert!(storage.load_all_sent_calendar_pings().unwrap().is_empty());

        for id in ids {
            storage.add_sent_calendar_ping(id).unwrap();
        }
        assert_eq!(&storage.load_all_sent_calendar_pings().unwrap(), &ids);

        storage.remove_sent_calendar_ping(ids[0]).unwrap();
        assert_eq!(&storage.load_all_sent_calendar_pings().unwrap(), &ids[1..]);
    }

    #[test]
    fn test_epoch_and_fetch_all_email_userids_mappings() {
        let mut storage = Storage::from_memory().expect("Failed making in-memory db");
        assert_eq!(storage.email_mappings_epoch(), EmailMappingsEpoch(0));

        let email1 = Email::parse("user1@example.com").unwrap();
        let email2 = Email::parse("user2@example.com").unwrap();
        let uid1 = db_to_userid(100);
        let uid2 = db_to_userid(200);

        storage.add_user_email_mapping(uid1, &email1).unwrap();
        assert_eq!(storage.email_mappings_epoch(), EmailMappingsEpoch(1));

        storage.add_user_email_mapping(uid2, &email1).unwrap();
        assert_eq!(storage.email_mappings_epoch(), EmailMappingsEpoch(2));

        storage.add_user_email_mapping(uid2, &email2).unwrap();
        assert_eq!(storage.email_mappings_epoch(), EmailMappingsEpoch(3));

        // Adding duplicate mapping should not increment epoch.
        storage.add_user_email_mapping(uid2, &email2).unwrap();
        assert_eq!(storage.email_mappings_epoch(), EmailMappingsEpoch(3));

        let all = storage.fetch_all_email_userids_mappings().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get(&email1), Some(&vec![uid1, uid2]));
        assert_eq!(all.get(&email2), Some(&vec![uid2]));

        storage.remove_userid_mapping(uid1, &email1).unwrap();
        assert_eq!(storage.email_mappings_epoch(), EmailMappingsEpoch(4));

        let all_after = storage.fetch_all_email_userids_mappings().unwrap();
        assert_eq!(all_after.get(&email1), Some(&vec![uid2]));
    }
}
