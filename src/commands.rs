use crate::redis::Redis;
use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::sync::Arc;
use tokio::time::Duration;

enum Command<'a> {
    Ping,
    Echo(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expiry: Option<(&'a Bytes, i64)>,
    },
    Get(Bytes),
    RPUSH(Bytes),
    LPUSH(Bytes),
    LLEN(Bytes),
    LRANGE {
        key: Bytes,
        start: isize,
        end: isize,
    },
    LPOP {
        key: Bytes,
        count: usize,
    },
    BLPOP {
        key: Bytes,
        timeout: Duration,
    },
    TYPE(Bytes),
    XADD {
        key: Bytes,
        id: Bytes,
    },
    XRANGE {
        key: Bytes,
        start: Bytes,
        end: Bytes,
    },
    XREAD(Bytes),
}

fn parse_command(arr: &[RedisValueRef]) -> Option<Command> {
    let cmd_name = match arr.get(0)? {
        RedisValueRef::String(cmd) => std::str::from_utf8(cmd).ok()?.to_uppercase(),
        _ => return None,
    };

    match cmd_name.as_str() {
        "PING" => Some(Command::Ping),

        "ECHO" => {
            if let Some(RedisValueRef::String(arg)) = arr.get(1) {
                Some(Command::Echo(arg.clone()))
            } else {
                None
            }
        }

        "SET" => {
            if arr.len() >= 3 {
                let key = match &arr[1] {
                    RedisValueRef::String(k) => k.clone(),
                    _ => return None,
                };
                let value = match &arr[2] {
                    RedisValueRef::String(v) => v.clone(),
                    _ => return None,
                };
                let expiry = if arr.len() == 5 {
                    match (&arr[3], &arr[4]) {
                        (RedisValueRef::String(s), RedisValueRef::String(i)) => {
                            Some((s, std::str::from_utf8(i).unwrap().parse::<i64>().unwrap()))
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                Some(Command::Set { key, value, expiry })
            } else {
                None
            }
        }

        "GET" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::Get(k.clone()))
            } else {
                None
            }
        }

        "RPUSH" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::RPUSH(k.clone()))
            } else {
                None
            }
        }
        "LRANGE" => {
            if arr.len() == 4 {
                match (&arr[1], &arr[2], &arr[3]) {
                    (
                        RedisValueRef::String(key),
                        RedisValueRef::String(start),
                        RedisValueRef::String(end),
                    ) => Some(Command::LRANGE {
                        key: key.clone(),
                        start: std::str::from_utf8(start)
                            .unwrap()
                            .parse::<isize>()
                            .unwrap(),
                        end: std::str::from_utf8(end).unwrap().parse::<isize>().unwrap(),
                    }),
                    _ => None,
                }
            } else {
                None
            }
        }
        "LPUSH" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::LPUSH(k.clone()))
            } else {
                None
            }
        }
        "LLEN" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::LLEN(k.clone()))
            } else {
                None
            }
        }
        "LPOP" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                if arr.len() == 3 {
                    match &arr[2] {
                        RedisValueRef::String(count) => Some(Command::LPOP {
                            key: k.clone(),
                            count: std::str::from_utf8(count)
                                .unwrap()
                                .parse::<usize>()
                                .unwrap(),
                        }),
                        _ => None,
                    }
                } else {
                    Some(Command::LPOP {
                        key: k.clone(),
                        count: 1,
                    })
                }
            } else {
                None
            }
        }
        "BLPOP" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                let duration_f64 = match arr.get(2) {
                    Some(val) => match val {
                        RedisValueRef::String(s) => {
                            std::str::from_utf8(s).unwrap().parse::<f64>().unwrap()
                        }
                        _ => return None,
                    },
                    None => return None,
                };
                let duration = Duration::from_secs_f64(if duration_f64 == 0.0 {
                    86400.0
                } else {
                    duration_f64
                });
                return Some(Command::BLPOP {
                    key: k.clone(),
                    timeout: duration,
                });
            }
            None
        }
        "TYPE" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::TYPE(k.clone()))
            } else {
                None
            }
        }
        "XADD" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                if arr.len() >= 3 {
                    match &arr[2] {
                        RedisValueRef::String(id) => Some(Command::XADD {
                            key: k.clone(),
                            id: id.clone(),
                        }),
                        _ => None,
                    }
                } else {
                    None
                }
            } else {
                None
            }
        }
        "XRANGE" => {
            if arr.len() == 4 {
                match (&arr[1], &arr[2], &arr[3]) {
                    (
                        RedisValueRef::String(key),
                        RedisValueRef::String(start),
                        RedisValueRef::String(end),
                    ) => {
                        return Some(Command::XRANGE {
                            key: key.clone(),
                            start: start.clone(),
                            end: end.clone(),
                        })
                    }
                    _ => return None,
                }
            }
            None
        }
        "XREAD" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::XREAD(k.clone()))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub async fn handle_command(value: RedisValueRef, redis: &Arc<Redis>) -> Option<RedisValueRef> {
    let arr = match value {
        RedisValueRef::Array(ref a) => a,
        _ => return Some(RedisValueRef::Error(Bytes::from("ERR expected array"))),
    };

    match parse_command(arr)? {
        Command::Ping => Some(RedisValueRef::String(Bytes::from("PONG"))),

        Command::Echo(message) => Some(RedisValueRef::String(message)),

        Command::Set { key, value, expiry } => {
            redis
                .kv
                .insert_entry(key, RedisValueRef::String(value), expiry)
                .await;
            Some(RedisValueRef::String(Bytes::from("OK")))
        }

        Command::Get(key) => match redis.kv.get_entry(&key).await {
            Some(RedisValueRef::String(s)) => Some(RedisValueRef::BulkString(s)),
            _ => Some(RedisValueRef::NullBulkString),
        },

        Command::RPUSH(key) => {
            let mut i = 0;
            for j in 2..arr.len() {
                match &arr[j] {
                    RedisValueRef::String(s) => {
                        i = redis.lists.rpush(&key, s.clone()).await;
                    }
                    _ => return None,
                }
            }
            Some(RedisValueRef::Int(i))
        }

        Command::LPUSH(key) => {
            let mut i = 0;
            for j in 2..arr.len() {
                match &arr[j] {
                    RedisValueRef::String(s) => {
                        i = redis.lists.lpush(&key, s.clone()).await;
                    }
                    _ => return None,
                }
            }
            Some(RedisValueRef::Int(i))
        }

        Command::LRANGE { key, start, end } => Some(RedisValueRef::Array(
            redis.lists.lrange(&key, start, end).await,
        )),

        Command::LLEN(key) => Some(RedisValueRef::Int(redis.lists.llen(&key).await)),

        Command::LPOP { key, count } => match redis.lists.lpop(&key, count).await {
            Some(v) => {
                if v.len() == 1 {
                    return Some(v[0].clone());
                } else {
                    return Some(RedisValueRef::Array(v));
                }
            }
            None => Some(RedisValueRef::NullBulkString),
        },

        Command::BLPOP { key, timeout } => Some(redis.lists.blpop(&key, timeout).await),
        Command::TYPE(key) => {
            if redis.kv.contains(&key).await {
                Some(RedisValueRef::String(Bytes::from("string")))
            } else if redis.stream.contains(&key).await {
                Some(RedisValueRef::String(Bytes::from("stream")))
            } else {
                Some(RedisValueRef::String(Bytes::from("none")))
            }
        }
        Command::XADD { key, id } => {
            let mut kv: Vec<Bytes> = Vec::new();
            for i in 3..arr.len() {
                match &arr[i] {
                    RedisValueRef::String(b) => kv.push(b.clone()),
                    _ => return None,
                }
            }
            Some(redis.stream.xadd(key, id, kv).await)
        }
        Command::XRANGE { key, start, end } => Some(RedisValueRef::Array(
            redis.stream.xrange(&key, &start, &end).await,
        )),
        Command::XREAD(to_block) => {
            let mut key_stream_start: Vec<Bytes> = Vec::new();
            let start = match to_block.as_ref() {
                b"block" => 4,
                _ => 2,
            };
            println!("Start: {start}");
            let n = (arr.len() - start) / 2;
            for i in start..(start + n) {
                match (&arr[i], &arr[n + i]) {
                    (RedisValueRef::String(stream_key), RedisValueRef::String(stream_start)) => {
                        key_stream_start.push(stream_key.clone());
                        key_stream_start.push(stream_start.clone());
                    }
                    _ => return None,
                }
            }
            println!("key_stream_start: {}", key_stream_start.len());
            println!("{:?}", key_stream_start);
            if start == 4 {
                let duration_f64 = match arr.get(2) {
                    Some(val) => match val {
                        RedisValueRef::String(s) => {
                            std::str::from_utf8(s).unwrap().parse::<f64>().unwrap()
                        }
                        _ => return None,
                    },
                    None => return None,
                };
                let duration = Duration::from_secs_f64(if duration_f64 == 0.0 {
                    86400.0
                } else {
                    duration_f64
                });
                Some(RedisValueRef::Array(
                    redis
                        .stream
                        .blocking_xread(&key_stream_start, duration)
                        .await,
                ))
            } else {
                Some(RedisValueRef::Array(
                    redis.stream.xread(&key_stream_start).await,
                ))
            }
        }
    }
}
