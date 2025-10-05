use bytes::Bytes;
use futures::{SinkExt, StreamExt}; // Add this dependency
use redis::resp::{RedisValueRef, RespParser};
use tokio::net::TcpListener;
use tokio_util::codec::{Encoder, Framed};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, addr)) => {
                println!("accepted new connection from: {addr}");

                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser);

                    while let Some(result) = framed.next().await {
                        match result {
                            Ok(value) => {
                                if let Some(response) = handle_command(value) {
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

fn handle_command(value: RedisValueRef) -> Option<RedisValueRef> {
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
