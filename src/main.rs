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
                    // Wrap the stream with the RespParser codec
                    let mut framed = Framed::new(stream, RespParser);

                    // Read parsed Redis values from the stream
                    while let Some(result) = framed.next().await {
                        match result {
                            Ok(value) => {
                                framed
                                    .send(RedisValueRef::String(Bytes::from("PONG")))
                                    .await
                                    .unwrap();
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
