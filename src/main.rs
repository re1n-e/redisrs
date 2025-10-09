use futures::{SinkExt, StreamExt}; // Add this dependency
use redis::commands::handle_command;
use redis::redis::Redis;
use redis::resp::RespParser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

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
