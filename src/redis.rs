use crate::lists::List;
use crate::rdb::KeyValue;
use crate::resp::RedisValueRef;
use crate::streams::Stream;
use crate::transactions::Transaction;
use bytes::Bytes;
use std::fmt::Write;
use tokio::sync::RwLock;

pub struct Info {
    role: RwLock<String>,
    connected_slaves: RwLock<u64>,
    master_replid: RwLock<String>,
    master_repl_offset: RwLock<u64>,
    second_repl_offset: RwLock<i64>,
    repl_backlog_active: RwLock<u64>,
    repl_backlog_size: RwLock<u64>,
    repl_backlog_first_byte_offset: RwLock<u64>,
    repl_backlog_histlen: RwLock<u64>,
}

impl Info {
    pub fn new() -> Self {
        Info {
            role: RwLock::new("master".to_string()), // default role
            connected_slaves: RwLock::new(0),
            master_replid: RwLock::new("".to_string()),
            master_repl_offset: RwLock::new(0),
            second_repl_offset: RwLock::new(0),
            repl_backlog_active: RwLock::new(0),
            repl_backlog_size: RwLock::new(0),
            repl_backlog_first_byte_offset: RwLock::new(0),
            repl_backlog_histlen: RwLock::new(0),
        }
    }

    /// Sets the server's replication role ("master" or "slave")
    pub async fn set_role(&self, role: &str) {
        let mut r = self.role.write().await;
        *r = role.to_string();
    }

    /// Increments connected slave count
    pub async fn add_slave(&self) {
        let mut count = self.connected_slaves.write().await;
        *count += 1;
    }

    /// Decrements connected slave count safely
    pub async fn remove_slave(&self) {
        let mut count = self.connected_slaves.write().await;
        if *count > 0 {
            *count -= 1;
        }
    }

    /// Sets replication id (used for partial resync)
    pub async fn set_master_replid(&self, id: &str) {
        let mut replid = self.master_replid.write().await;
        *replid = id.to_string();
    }

    /// Sets replication offset
    pub async fn set_master_repl_offset(&self, offset: u64) {
        let mut off = self.master_repl_offset.write().await;
        *off = offset;
    }

    /// Serializes `INFO replication` output like Redis
    pub async fn serialize(&self) -> RedisValueRef {
        let role = self.role.read().await.clone();
        let connected_slaves = *self.connected_slaves.read().await;
        let master_replid = self.master_replid.read().await.clone();
        let master_repl_offset = *self.master_repl_offset.read().await;
        let second_repl_offset = *self.second_repl_offset.read().await;
        let backlog_active = *self.repl_backlog_active.read().await;
        let backlog_size = *self.repl_backlog_size.read().await;
        let backlog_first_byte_offset = *self.repl_backlog_first_byte_offset.read().await;
        let backlog_histlen = *self.repl_backlog_histlen.read().await;

        let mut s = String::new();
        writeln!(s, "# Replication").unwrap();
        writeln!(s, "role:{}", role).unwrap();
        writeln!(s, "connected_slaves:{}", connected_slaves).unwrap();
        writeln!(s, "master_replid:{}", master_replid).unwrap();
        writeln!(s, "master_repl_offset:{}", master_repl_offset).unwrap();
        writeln!(s, "second_repl_offset:{}", second_repl_offset).unwrap();
        writeln!(s, "repl_backlog_active:{}", backlog_active).unwrap();
        writeln!(s, "repl_backlog_size:{}", backlog_size).unwrap();
        writeln!(
            s,
            "repl_backlog_first_byte_offset:{}",
            backlog_first_byte_offset
        )
        .unwrap();
        writeln!(s, "repl_backlog_histlen:{}", backlog_histlen).unwrap();

        RedisValueRef::BulkString(Bytes::from(s))
    }
}

pub struct Redis {
    pub kv: KeyValue,
    pub lists: List,
    pub stream: Stream,
    pub tr: Transaction,
    pub info: Info,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: List::new(),
            stream: Stream::new(),
            tr: Transaction::new(),
            info: Info::new(),
        }
    }
}
