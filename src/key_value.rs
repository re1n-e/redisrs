use crate::resp::RedisValueRef;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

struct Set {
    value: RedisValueRef,
    expiry: Option<Instant>,
}

pub struct KeyValue {
    entries: RwLock<HashMap<RedisValueRef, Set>>,
}

impl KeyValue {
    pub fn new() -> Self {
        KeyValue {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub async fn insert_entry(
        &self,
        key: RedisValueRef,
        value: RedisValueRef,
        expiry: Option<(&[u8], i64)>,
    ) {
        let mut entries = self.entries.write().await;

        let set = if let Some((ty, time)) = expiry {
            let duration = match ty {
                b"EX" => Duration::from_secs(time as u64),
                b"PX" => Duration::from_millis(time as u64),
                _ => Duration::from_secs(0), // invalid
            };
            Set {
                value,
                expiry: Some(Instant::now() + duration),
            }
        } else {
            Set {
                value,
                expiry: None,
            }
        };

        entries.insert(key, set);
    }

    pub async fn get_entry(&self, key: &RedisValueRef) -> Option<RedisValueRef> {
        let mut entries = self.entries.write().await;

        // Clone the value so we can safely remove the key later
        if let Some(set) = entries.get(key) {
            if let Some(expiry) = set.expiry {
                if Instant::now() > expiry {
                    // Expired: remove it and return None
                    entries.remove(key);
                    return None;
                }
            }
            return Some(set.value.clone());
        }

        None
    }
}
