use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use tokio::sync::{oneshot, RwLock};
use tokio::time::{timeout, Duration};
type BlockedClientsMap = HashMap<Bytes, VecDeque<oneshot::Sender<bool>>>;

pub struct List {
    blocked: RwLock<BlockedClientsMap>,
    lists: RwLock<HashMap<Bytes, VecDeque<Bytes>>>,
}

impl List {
    pub fn new() -> Self {
        Self {
            lists: RwLock::new(HashMap::new()),
            blocked: RwLock::new(HashMap::new()),
        }
    }

    pub async fn rpush(&self, key: &Bytes, value: Bytes) -> i64 {
        let mut lists = self.lists.write().await;
        let mut items_added = 1;
        if let Some(list) = lists.get_mut(key) {
            list.push_back(value);
            items_added = list.len() as i64
        } else {
            lists.insert(key.clone(), VecDeque::from(vec![value]));
        }

        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(key) {
            for _ in 0..items_added {
                if let Some(notifier) = notifiers.pop_front() {
                    let _ = notifier.send(true);
                } else {
                    break;
                }
            }
        }

        items_added
    }

    pub async fn lpush(&self, key: &Bytes, value: Bytes) -> i64 {
        let mut lists = self.lists.write().await;
        let mut items_added = 1;
        if let Some(list) = lists.get_mut(key) {
            list.push_front(value);
            items_added = list.len() as i64
        } else {
            lists.insert(key.clone(), VecDeque::from(vec![value]));
        }
        drop(lists);

        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(key) {
            for _ in 0..items_added {
                if let Some(notifier) = notifiers.pop_front() {
                    let _ = notifier.send(true);
                } else {
                    break;
                }
            }
        }

        items_added
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

    pub async fn blpop(&self, key: &Bytes, duration: Duration) -> RedisValueRef {
        let mut lists = self.lists.write().await;
        if let Some(list) = lists.get_mut(key) {
            if list.len() > 0 {
                return RedisValueRef::BulkString(list.pop_front().unwrap());
            }
        }
        drop(lists);
        let (tx, rx) = oneshot::channel::<bool>();
        {
            let mut blocked_clients = self.blocked.write().await;
            blocked_clients
                .entry(key.clone())
                .or_default()
                .push_back(tx);
        }
        match timeout(duration, rx).await {
            Ok(Ok(_)) => {
                let mut lists = self.lists.write().await;
                if let Some(list) = lists.get_mut(key) {
                    if let Some(value) = list.pop_front() {
                        if list.is_empty() {
                            lists.remove(key);
                        }
                        return RedisValueRef::Array(vec![
                            RedisValueRef::BulkString(key.clone()),
                            RedisValueRef::BulkString(value),
                        ]);
                    } else {
                        lists.remove(key);
                    }
                }
            }
            Ok(Err(_)) => {
                let mut blocked_clients = self.blocked.write().await;
                if let Some(notifiers) = blocked_clients.get_mut(key) {
                    if notifiers.is_empty() {
                        blocked_clients.remove(key);
                    }
                }
            }
            Err(_) => {
                let mut blocked_clients = self.blocked.write().await;
                if let Some(notifiers) = blocked_clients.get_mut(key) {
                    if notifiers.is_empty() {
                        blocked_clients.remove(key);
                    }
                }
            }
        }
        RedisValueRef::NullArray
    }
}
