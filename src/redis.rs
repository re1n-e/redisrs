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

    pub async fn lrange(&self, key: &Bytes, start: isize, end: isize) -> Vec<RedisValueRef> {
        let mut res: Vec<RedisValueRef> = Vec::new();
        let lists = self.lists.read().await;

        if let Some(list) = lists.get(key) {
            let len = list.len() as isize;

            // Handle empty list
            if len == 0 {
                return res;
            }

            // Convert negative indices
            let mut start = if start < 0 { len + start } else { start };
            let mut end = if end < 0 { len + end } else { end };

            // Clamp to valid range
            if start < 0 {
                start = 0;
            }
            if end >= len {
                end = len - 1;
            }

            // If range is valid, collect items
            if start <= end {
                for i in start..=end {
                    if let Some(item) = list.get(i as usize) {
                        res.push(RedisValueRef::BulkString(item.clone()));
                    }
                }
            }
        }

        res
    }
}
