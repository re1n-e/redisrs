use std::collections::{HashMap, VecDeque};

use crate::{key_value::KeyValue, resp::RedisValueRef};
use bytes::Bytes;
use tokio::sync::RwLock;

pub struct Redis {
    pub kv: KeyValue,
    pub lists: RwLock<HashMap<Bytes, VecDeque<RedisValueRef>>>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_list(&self, key: &Bytes, value: RedisValueRef) {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            list.push_back(value);
        } else {
            lists.insert(key.clone(), VecDeque::from(vec![value]));
        }
    }
}
