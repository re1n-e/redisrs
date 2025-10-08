use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use tokio::sync::RwLock;

pub struct List {
    lists: RwLock<HashMap<Bytes, VecDeque<Bytes>>>,
}

impl List {
    pub fn new() -> Self {
        Self {
            lists: RwLock::new(HashMap::new()),
        }
    }

    pub async fn rpush(&self, key: &Bytes, value: Bytes) -> i64 {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            list.push_back(value);
            list.len() as i64
        } else {
            lists.insert(key.clone(), VecDeque::from(vec![value]));
            1
        }
    }

    pub async fn lpush(&self, key: &Bytes, value: Bytes) -> i64 {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            list.push_front(value);
            list.len() as i64
        } else {
            lists.insert(key.clone(), VecDeque::from(vec![value]));
            1
        }
    }

    pub async fn llen(&self, key: &Bytes) -> i64 {
        let lists = self.lists.read().await;
        if let Some(list) = lists.get(key) {
            list.len() as i64
        } else {
            0
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

    pub async fn lpop(&self, key: &Bytes, count: usize) -> Option<Vec<RedisValueRef>> {
        let mut res: Vec<RedisValueRef> = Vec::new();
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            if !list.is_empty() {
                for _ in 0..count {
                    let element = list.pop_front();
                    if element.is_none() {
                        break;
                    }
                    res.push(RedisValueRef::BulkString(element.unwrap()));
                }
            } else {
                return None;
            }
        } else {
            return None;
        }

        Some(res)
    }
}
