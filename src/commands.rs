use crate::redis::Redis;
use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::sync::Arc;

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
    LRANGE {
        key: Bytes,
        start: usize,
        end: usize,
    },
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
                            .parse::<usize>()
                            .unwrap(),
                        end: std::str::from_utf8(end).unwrap().parse::<usize>().unwrap(),
                    }),
                    _ => None,
                }
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
                        i = redis.add_list(&key, s.clone()).await;
                    }
                    _ => return None,
                }
            }
            Some(RedisValueRef::Int(i))
        }

        Command::LRANGE { key, start, end } => {
            Some(RedisValueRef::Array(redis.lrange(&key, start, end).await))
        }
    }
}
