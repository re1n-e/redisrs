use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

struct Set {
    value: Bytes,
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

    pub async fn insert_entry(&self, key: Bytes, value: Bytes, expiry: Option<(&Bytes, i64)>) {
        let mut entries = self.entries.write().await;

        let set = if let Some((ty, time)) = expiry {
            println!("IN");
            let ty: &[u8] = &ty;
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
                _ => Duration::from_secs(0),
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

    pub async fn get_entry(&self, key: &Bytes) -> Option<RedisValueRef> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(key) {
            if let Some(expiry) = entry.expiry {
                if Instant::now() >= expiry {
                    entries.remove(key);
                    return None;
                }
            }
            return Some(RedisValueRef::String(entry.value.clone()));
        }
        None
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

    pub async fn contains(&self, key: &Bytes) -> bool {
        let entries = self.entries.read().await;
        entries.contains_key(key)
    }

    pub async fn incr(&self, key: &Bytes) -> RedisValueRef {
        let mut entries = self.entries.write().await;

        let set = Set {
            value: Bytes::from((1).to_string()),
            expiry: None,
        };

        if let Some(entry) = entries.get_mut(key) {
            if let Some(expiry) = entry.expiry {
                if Instant::now() >= expiry {
                    entries.remove(key);
                }
                entries.insert(key.clone(), set);
            } else {
                let value = match std::str::from_utf8(&entry.value).unwrap().parse::<i64>() {
                    Ok(val) => val + 1,
                    Err(_) => {
                        return RedisValueRef::Error(Bytes::from(
                            "ERR value is not an integer or out of range",
                        ))
                    }
                };
                entry.value = Bytes::from(value.to_string());
                return RedisValueRef::Int(value);
            }
        } else {
            entries.insert(key.clone(), set);
        }
        RedisValueRef::Int(1)
    }
}

impl Default for KeyValue {
    fn default() -> Self {
        Self::new()
    }
}
