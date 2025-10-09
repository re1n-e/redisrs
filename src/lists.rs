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

        let list = lists.entry(key.clone()).or_insert_with(VecDeque::new);
        list.push_back(value);
        let new_len = list.len() as i64;

        drop(lists);

        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(key) {
            if let Some(notifier) = notifiers.pop_front() {
                let _ = notifier.send(true);

                if notifiers.is_empty() {
                    blocked_clients.remove(key);
                }
            }
        }

        new_len
    }

    pub async fn lpush(&self, key: &Bytes, value: Bytes) -> i64 {
        let mut lists = self.lists.write().await;

        let list = lists.entry(key.clone()).or_insert_with(VecDeque::new);
        list.push_front(value);
        let new_len = list.len() as i64;

        drop(lists);

        let mut blocked_clients = self.blocked.write().await;
        if let Some(notifiers) = blocked_clients.get_mut(key) {
            if let Some(notifier) = notifiers.pop_front() {
                println!("Wake up client");
                let _ = notifier.send(true);
                if notifiers.is_empty() {
                    blocked_clients.remove(key);
                }
            }
        }

        new_len
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
        let lists = self.lists.read().await;

        let Some(list) = lists.get(key) else {
            return Vec::new();
        };

        let len = list.len() as isize;

        if len == 0 {
            return Vec::new();
        }

        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len - 1)
        };

        let end = if end < 0 {
            (len + end).max(-1)
        } else {
            end.min(len - 1)
        };

        if start > end || end < 0 {
            return Vec::new();
        }

        (start..=end)
            .filter_map(|i| list.get(i as usize))
            .map(|item| RedisValueRef::BulkString(item.clone()))
            .collect()
    }

    pub async fn lpop(&self, key: &Bytes, count: usize) -> Option<Vec<RedisValueRef>> {
        let mut lists = self.lists.write().await;

        let list = lists.get_mut(key)?;

        if list.is_empty() {
            return None;
        }

        let mut res = Vec::new();
        for _ in 0..count {
            if let Some(element) = list.pop_front() {
                res.push(RedisValueRef::BulkString(element));
            } else {
                break;
            }
        }

        if list.is_empty() {
            lists.remove(key);
        }

        Some(res)
    }

    pub async fn blpop(&self, key: &Bytes, duration: Duration) -> RedisValueRef {
        {
            let mut lists = self.lists.write().await;
            if let Some(list) = lists.get_mut(key) {
                if let Some(value) = list.pop_front() {
                    // Clean up empty list
                    if list.is_empty() {
                        lists.remove(key);
                    }
                    return RedisValueRef::Array(vec![
                        RedisValueRef::BulkString(key.clone()),
                        RedisValueRef::BulkString(value),
                    ]);
                }
            }
        }

        let (tx, rx) = oneshot::channel::<bool>();

        {
            let mut blocked_clients = self.blocked.write().await;
            blocked_clients
                .entry(key.clone())
                .or_default()
                .push_back(tx);
            println!("Blocked clients len: {}", blocked_clients.len());
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
                    }
                }
                println!("Ok");
                RedisValueRef::NullArray
            }
            Ok(Err(_)) | Err(_) => {
                println!("Not good");
                let mut blocked_clients = self.blocked.write().await;
                if let Some(notifiers) = blocked_clients.get_mut(key) {
                    if notifiers.is_empty() {
                        blocked_clients.remove(key);
                    }
                }
                RedisValueRef::NullArray
            }
        }
    }

    pub async fn contains(&self, key: &Bytes) -> bool {
        let lists = self.lists.read().await;
        lists.contains_key(key)
    }
}
