use bytes::Bytes;
use futures::{SinkExt, StreamExt}; // Add this dependency
use redis::redis::Redis;
use redis::resp::{RedisValueRef, RespParser};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::{Encoder, Framed};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let redis = Arc::new(Redis::new());
    loop {
        let stream = listener.accept().await;
        let redis = redis.clone();
        match stream {
            Ok((stream, addr)) => {
                println!("accepted new connection from: {addr}");

                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser);

                    while let Some(result) = framed.next().await {
                        match result {
                            Ok(value) => {
                                if let Some(response) = handle_command(value, &redis).await {
                                    if let Err(e) = framed.send(response).await {
                                        eprintln!("Failed to send response: {:?}", e);
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Parse error: {:?}", e);
                                break;
                            }
                        }
                    }

                    println!("Connection closed: {addr}");
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_command(value: RedisValueRef, redis: &Arc<Redis>) -> Option<RedisValueRef> {
    match value {
        RedisValueRef::Array(ref arr) if !arr.is_empty() => {
            // Extract the command (first element)
            if let RedisValueRef::String(ref cmd_bytes) = arr[0] {
                let cmd = std::str::from_utf8(cmd_bytes).ok()?.to_uppercase();

                match cmd.as_str() {
                    "PING" => Some(RedisValueRef::String(Bytes::from("PONG"))),
                    "ECHO" => {
                        // ECHO command should have an argument
                        if arr.len() >= 2 {
                            if let RedisValueRef::String(ref message) = arr[1] {
                                // Echo back the message
                                Some(RedisValueRef::String(message.clone()))
                            } else {
                                Some(RedisValueRef::Error(Bytes::from("ERR invalid argument")))
                            }
                        } else {
                            Some(RedisValueRef::Error(Bytes::from(
                                "ERR wrong number of arguments for 'echo' command",
                            )))
                        }
                    }
                    "SET" => {
                        if arr.len() == 3 {
                            redis
                                .kv
                                .insert_entry(arr[1].clone(), arr[2].clone(), None)
                                .await;
                        } else if arr.len() == 5 {
                            let ty = match &arr[3] {
                                RedisValueRef::String(ty) => {
                                    String::from_utf8(ty.to_vec()).unwrap()
                                }
                                _ => {
                                    return Some(RedisValueRef::Error(Bytes::from(
                                        "Invalid type for set command",
                                    )))
                                }
                            };
                            let time = match arr[4] {
                                RedisValueRef::Int(i) => i,
                                _ => {
                                    return Some(RedisValueRef::Error(Bytes::from(
                                        "Invalid value for time in set command",
                                    )))
                                }
                            };
                            redis
                                .kv
                                .insert_entry(arr[1].clone(), arr[2].clone(), Some((&ty, time)))
                                .await;
                        } else {
                            return Some(RedisValueRef::Error(Bytes::from("ERR invalid argument")));
                        }
                        Some(RedisValueRef::String(Bytes::from("OK")))
                    }
                    "GET" => {
                        if let Some(key) = arr.get(1) {
                            match redis.kv.get_entry(key).await {
                                Some(value) => match value {
                                    RedisValueRef::String(s) => {
                                        return Some(RedisValueRef::BulkString(s))
                                    }
                                    _ => return None,
                                },
                                None => return Some(RedisValueRef::NullBulkString),
                            }
                        }
                        Some(RedisValueRef::NullBulkString)
                    }
                    _ => Some(RedisValueRef::Error(Bytes::from(format!(
                        "ERR unknown command '{}'",
                        cmd
                    )))),
                }
            } else {
                Some(RedisValueRef::Error(Bytes::from(
                    "ERR invalid command format",
                )))
            }
        }
        _ => Some(RedisValueRef::Error(Bytes::from("ERR expected array"))),
    }
}
