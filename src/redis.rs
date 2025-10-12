use crate::key_value::KeyValue;
use crate::lists::List;
use crate::streams::Stream;
use crate::transactions::Transaction;

pub struct RdbPath {
    pub dir: String,
    pub dbfilename: String,
}

impl RdbPath {
    pub fn new(dir: String, dbfilename: String) -> Self {
        RdbPath { dir, dbfilename }
    }
}

pub struct Redis {
    pub kv: KeyValue,
    pub lists: List,
    pub stream: Stream,
    pub tr: Transaction,
    pub rdb_path: RdbPath,
}

impl Redis {
    pub fn new(dir: String, dbfilename: String) -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: List::new(),
            stream: Stream::new(),
            tr: Transaction::new(),
            rdb_path: RdbPath::new(dir, dbfilename),
        }
    }

    pub fn get_dir(&self) -> String {
        self.rdb_path.dir.clone()
    }

    pub fn get_filename(&self) -> String {
        self.rdb_path.dbfilename.clone()
    }
}
