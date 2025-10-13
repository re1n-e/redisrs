use crate::lists::List;
use crate::rdb::KeyValue;
use crate::streams::Stream;
use crate::transactions::Transaction;

pub enum Role {
    Master,
    Slave,
}

pub struct Info {
    role: Role,
    connected_slaves: u64,
    master_replid: String,
    master_repl_offset: u64,
    second_repl_offset: i64,
    repl_backlog_active: u64,
    repl_backlog_size: u64,
    repl_backlog_first_byte_offset: u64,
    repl_backlog_histlen: u64,
}

impl Info {
    pub fn new(role: Role) -> Self {
        Info {
            role,
            connected_slaves: 0,
            master_replid: String::new(),
            master_repl_offset: 0,
            second_repl_offset: 0,
            repl_backlog_active: 0,
            repl_backlog_size: 0,
            repl_backlog_first_byte_offset: 0,
            repl_backlog_histlen: 0,
        }
    }
}

pub struct Redis {
    pub kv: KeyValue,
    pub lists: List,
    pub stream: Stream,
    pub tr: Transaction,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: List::new(),
            stream: Stream::new(),
            tr: Transaction::new(),
        }
    }
}
