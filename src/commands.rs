use crate::redis::Redis;
use crate::resp::RedisValueRef;
use bytes::Bytes;
use std::sync::Arc;

enum Command {
    Ping,
    Echo(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expiry: Option<(String, i64)>,
    },
    Get(Bytes),
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
                if let Some(_) = arr.get(3) {
                    println!("Some");
                }
                if let Some(_) = arr.get(4) {
                    println!("2Some");
                }
                let expiry = if arr.len() == 5 {
                    if let (RedisValueRef::String(ty), RedisValueRef::Int(time)) =
                        (&arr[3], &arr[4])
                    {
                        let ty_str = std::str::from_utf8(ty).ok()?.to_uppercase();
                        Some((ty_str, *time))
                    } else {
                        None
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
                .insert_entry(
                    key,
                    RedisValueRef::String(value),
                    expiry.as_ref().map(|(ty, t)| (ty.as_bytes(), *t)),
                )
                .await;
            Some(RedisValueRef::String(Bytes::from("OK")))
        }

        Command::Get(key) => match redis.kv.get_entry(&key).await {
            Some(RedisValueRef::String(s)) => Some(RedisValueRef::BulkString(s)),
            _ => Some(RedisValueRef::NullBulkString),
        },
    }
}
