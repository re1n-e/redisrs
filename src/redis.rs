use crate::key_value::KeyValue;
use crate::list::List;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use tokio::sync::RwLock;

pub struct Redis {
    pub kv: KeyValue,
    pub lists: List,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: List::new(),
        }
    }
}
