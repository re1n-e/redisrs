use crate::redis::Redis;
use crate::resp::RedisValueRef;
use bytes::Bytes;
use core::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;

pub enum Command {
    Ping,
    Echo(Bytes),
    Set {
        key: Bytes,
        value: Bytes,
        expiry: Option<(Bytes, i64)>,
    },
    Get(Bytes),
    RPUSH {
        key: Bytes,
        values: Vec<Bytes>,
    },
    LPUSH {
        key: Bytes,
        values: Vec<Bytes>,
    },
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
        kv: Vec<Bytes>,
    },
    XRANGE {
        key: Bytes,
        start: Bytes,
        end: Bytes,
    },
    XREAD {
        to_block: Bytes,
        timeout: Option<Duration>,
        key_stream_start: Vec<Bytes>,
    },
    INCR(Bytes),
    MULTI,
    EXEC,
    DISCARD,
    CONFIG {
        dir: bool,
        dbfilename: bool,
    },
    KEYS(Bytes),
    INFO(Bytes),
    REPLCONF(Bytes),
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
                            let s = s.clone();
                            Some((
                                Bytes::from(s),
                                std::str::from_utf8(i).unwrap().parse::<i64>().unwrap(),
                            ))
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
                let mut values = Vec::new();
                for j in 2..arr.len() {
                    match &arr[j] {
                        RedisValueRef::String(s) => values.push(s.clone()),
                        _ => return None,
                    }
                }
                Some(Command::RPUSH {
                    key: k.clone(),
                    values,
                })
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
                let mut values = Vec::new();
                for j in 2..arr.len() {
                    match &arr[j] {
                        RedisValueRef::String(s) => values.push(s.clone()),
                        _ => return None,
                    }
                }
                Some(Command::LPUSH {
                    key: k.clone(),
                    values,
                })
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
                        RedisValueRef::String(id) => {
                            let mut kv: Vec<Bytes> = Vec::new();
                            for i in 3..arr.len() {
                                match &arr[i] {
                                    RedisValueRef::String(b) => kv.push(b.clone()),
                                    _ => return None,
                                }
                            }
                            Some(Command::XADD {
                                key: k.clone(),
                                id: id.clone(),
                                kv,
                            })
                        }
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
                let mut key_stream_start: Vec<Bytes> = Vec::new();
                let to_block = k.clone();
                let start = match to_block.as_ref() {
                    b"block" => 4,
                    _ => 2,
                };

                let timeout = if start == 4 {
                    let duration_u64 = match arr.get(2) {
                        Some(val) => match val {
                            RedisValueRef::String(s) => {
                                std::str::from_utf8(s).unwrap().parse::<u64>().unwrap()
                            }
                            _ => return None,
                        },
                        None => return None,
                    };
                    Some(Duration::from_millis(if duration_u64 == 0 {
                        86400
                    } else {
                        duration_u64
                    }))
                } else {
                    None
                };

                let n = (arr.len() - start) / 2;
                for i in start..(start + n) {
                    match (&arr[i], &arr[n + i]) {
                        (
                            RedisValueRef::String(stream_key),
                            RedisValueRef::String(stream_start),
                        ) => {
                            key_stream_start.push(stream_key.clone());
                            key_stream_start.push(stream_start.clone());
                        }
                        _ => return None,
                    }
                }

                Some(Command::XREAD {
                    to_block,
                    timeout,
                    key_stream_start,
                })
            } else {
                None
            }
        }

        "INCR" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::INCR(k.clone()))
            } else {
                None
            }
        }

        "CONFIG" => {
            if let Some(RedisValueRef::String(k)) = arr.get(2) {
                let (mut dir, mut dbfilename) = (false, false);
                match k.as_ref() {
                    b"dir" => dir = true,
                    b"dbfilename" => dbfilename = true,
                    _ => (),
                }
                Some(Command::CONFIG { dir, dbfilename })
            } else {
                None
            }
        }

        "KEYS" => {
            if let Some(RedisValueRef::String(k)) = arr.get(1) {
                Some(Command::KEYS(k.clone()))
            } else {
                None
            }
        }

        "INFO" => {
            if let Some(RedisValueRef::String(repl)) = arr.get(1) {
                Some(Command::INFO(repl.clone()))
            } else {
                None
            }
        }

        "REPLCONF" => {
            if let Some(RedisValueRef::String(port)) = arr.get(2) {
                Some(Command::REPLCONF(port.clone()))
            } else {
                None
            }
        }

        "MULTI" => Some(Command::MULTI),
        "EXEC" => Some(Command::EXEC),
        "DISCARD" => Some(Command::DISCARD),
        _ => None,
    }
}

async fn execute_command(cmd: Command, redis: &Arc<Redis>) -> Option<RedisValueRef> {
    match cmd {
        Command::Ping => Some(RedisValueRef::String(Bytes::from("PONG"))),

        Command::Echo(message) => Some(RedisValueRef::String(message)),

        Command::Set { key, value, expiry } => {
            redis.kv.insert_entry(key, value, expiry).await;
            Some(RedisValueRef::String(Bytes::from("OK")))
        }

        Command::Get(key) => match redis.kv.get_entry(&key).await {
            Some(s) => Some(RedisValueRef::BulkString(s)),
            _ => Some(RedisValueRef::NullBulkString),
        },

        Command::RPUSH { key, values } => {
            let mut i = 0;
            for value in values {
                i = redis.lists.rpush(&key, value).await;
            }
            Some(RedisValueRef::Int(i))
        }

        Command::LPUSH { key, values } => {
            let mut i = 0;
            for value in values {
                i = redis.lists.lpush(&key, value).await;
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
                    Some(v[0].clone())
                } else {
                    Some(RedisValueRef::Array(v))
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

        Command::XADD { key, id, kv } => Some(redis.stream.xadd(key, id, kv).await),

        Command::XRANGE { key, start, end } => Some(RedisValueRef::Array(
            redis.stream.xrange(&key, &start, &end).await,
        )),

        Command::XREAD {
            to_block,
            timeout,
            key_stream_start,
        } => {
            if let Some(duration) = timeout {
                Some(
                    redis
                        .stream
                        .blocking_xread(&key_stream_start, duration)
                        .await,
                )
            } else {
                Some(RedisValueRef::Array(
                    redis.stream.xread(&key_stream_start).await,
                ))
            }
        }

        Command::CONFIG { dir, dbfilename } => {
            if dir {
                let cmd = "dir";
                Some(RedisValueRef::Array(vec![
                    RedisValueRef::BulkString(Bytes::from(cmd)),
                    RedisValueRef::BulkString(Bytes::from(redis.kv.get_dir().await)),
                ]))
            } else if dbfilename {
                let cmd = "dbfilename";
                Some(RedisValueRef::Array(vec![
                    RedisValueRef::BulkString(Bytes::from(cmd)),
                    RedisValueRef::BulkString(Bytes::from(redis.kv.get_filename().await)),
                ]))
            } else {
                None
            }
        }

        Command::INCR(key) => Some(match redis.kv.incr(&key).await {
            Ok(num) => RedisValueRef::Int(num),
            Err(e) => RedisValueRef::Error(Bytes::from(e)),
        }),

        Command::KEYS(pattern) => Some(redis.kv.keys(pattern).await),

        Command::INFO(_repl) => Some(redis.info.serialize().await),

        Command::REPLCONF(_) => Some(RedisValueRef::String(Bytes::from(String::from("OK")))),

        // Transaction commands should never reach here
        Command::MULTI | Command::EXEC | Command::DISCARD => None,
    }
}

pub async fn handle_command(
    value: RedisValueRef,
    addr: SocketAddr,
    redis: &Arc<Redis>,
) -> Option<RedisValueRef> {
    let arr = match value {
        RedisValueRef::Array(ref a) => a,
        _ => return Some(RedisValueRef::Error(Bytes::from("ERR expected array"))),
    };

    let parsed_command = parse_command(arr)?;

    // Handle transaction control commands immediately
    match parsed_command {
        Command::MULTI => {
            return Some(redis.tr.start_transaction(addr).await);
        }
        Command::EXEC => {
            let cmds = redis.tr.exec_transaction(addr).await;
            if let Some(cmds) = cmds {
                let mut results = Vec::new();
                for cmd in cmds {
                    if let Some(result) = execute_command(cmd, redis).await {
                        results.push(result);
                    }
                }
                return Some(RedisValueRef::Array(results));
            } else {
                return Some(RedisValueRef::Error(Bytes::from("ERR EXEC without MULTI")));
            }
        }
        Command::DISCARD => {
            return Some(redis.tr.discard_transaction(addr).await);
        }
        _ => {}
    }

    if redis.tr.in_transaction(addr).await {
        return Some(redis.tr.queue_command(addr, parsed_command).await);
    }

    execute_command(parsed_command, redis).await
}
