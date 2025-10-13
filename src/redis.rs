use crate::lists::List;
use crate::rdb::KeyValue;
use crate::streams::Stream;
use crate::transactions::Transaction;

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
