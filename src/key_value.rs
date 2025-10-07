use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

struct Set {
    value: RedisValueRef,
    expiry: Option<Instant>,
}

pub struct KeyValue {
    entries: RwLock<HashMap<Bytes, Set>>,
}

impl KeyValue {
    pub fn new() -> Self {
        KeyValue {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub async fn insert_entry(
        &self,
        key: Bytes,
        value: RedisValueRef,
        expiry: Option<(&[u8], i64)>,
    ) {
        let mut entries = self.entries.write().await;
        
        let set = if let Some((ty, time)) = expiry {
            let duration = match ty {
                b"EX" => Duration::from_secs(time as u64),
                b"PX" => Duration::from_millis(time as u64),
                b"EXAT" => {
                    let target = Instant::now() + Duration::from_secs(time as u64);
                    entries.insert(
                        key,
                        Set {
                            value,
                            expiry: Some(target),
                        },
                    );
                    return;
                }
                b"PXAT" => {
                    // Unix timestamp in milliseconds
                    let target = Instant::now() + Duration::from_millis(time as u64);
                    entries.insert(
                        key,
                        Set {
                            value,
                            expiry: Some(target),
                        },
                    );
                    return;
                }
                _ => Duration::from_secs(0), // invalid, will expire immediately
            };
            println!("Duration: {:?}", duration);
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

    pub async fn get_entry(&self, key: &Bytes) -> Option<RedisValueRef> {
        let entries = self.entries.read().await;

        if let Some(set) = entries.get(key) {
            if let Some(expiry) = set.expiry {
                if Instant::now() > expiry {
                    // Entry has expired
                    drop(entries); // Release read lock

                    // Acquire write lock to remove expired entry
                    let mut entries = self.entries.write().await;
                    entries.remove(key);
                    return None;
                }
            }
            return Some(set.value.clone());
        }

        None
    }

    pub async fn delete_entry(&self, key: &Bytes) -> bool {
        let mut entries = self.entries.write().await;
        entries.remove(key).is_some()
    }

    pub async fn exists(&self, key: &Bytes) -> bool {
        let entries = self.entries.read().await;

        if let Some(set) = entries.get(key) {
            if let Some(expiry) = set.expiry {
                if Instant::now() > expiry {
                    return false;
                }
            }
            return true;
        }

        false
    }
}

impl Default for KeyValue {
    fn default() -> Self {
        Self::new()
    }
}
