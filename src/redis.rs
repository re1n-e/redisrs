use std::collections::{HashMap, VecDeque};

use crate::{key_value::KeyValue, resp::RedisValueRef};
use bytes::Bytes;
use tokio::sync::RwLock;

pub struct Redis {
    pub kv: KeyValue,
    pub lists: RwLock<HashMap<Bytes, VecDeque<Bytes>>>,
}

impl Redis {
    pub fn new() -> Self {
        Redis {
            kv: KeyValue::new(),
            lists: RwLock::new(HashMap::new()),
        }
    }

    pub async fn add_list(&self, key: &Bytes, value: Bytes) -> i64 {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            list.push_back(value);
            list.len() as i64
        } else {
            lists.insert(key.clone(), VecDeque::from(vec![value]));
            1
        }
    }

    pub async fn lrange(&self, key: &Bytes, start: usize, end: usize) -> Vec<RedisValueRef> {
        let mut res: Vec<RedisValueRef> = Vec::new();
        let lists = self.lists.read().await;
        if let Some(list) = lists.get(key) {
            let si = (list.len() + start) % list.len();
            let mut ei = (list.len() + end) % list.len();
            if ei >= list.len() {
                ei = list.len() - 1;
            }
            if si < lists.len() && si <= ei {
                for i in si..=ei {
                    res.push(RedisValueRef::BulkString(list[i].clone()));
                }
            }
        }
        res
    }
}
