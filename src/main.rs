use clap::Parser;
use futures::{SinkExt, StreamExt}; // Add this dependency
use redis::commands::handle_command;
use redis::redis::Redis;
use redis::resp::RespParser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

/// Simple CLI demo
#[derive(Parser, Debug)]
#[command(name = "myapp", version = "1.0", about = "port")]
struct Args {
    /// Input file
    #[arg(short, long)]
    port: Option<String>,
    #[arg(short, long)]
    dir: Option<String>,
    #[arg(short, long)]
    dbfilename: Option<String>,
    #[arg(short, long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let port = match args.port {
        Some(port) => port,
        _ => String::from("6379"),
    };

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let redis = Arc::new(Redis::new());
    loop {
        let stream = listener.accept().await;
        let redis = redis.clone();
        let _ = match (&args.dir, &args.dbfilename) {
            (Some(dir), Some(filename)) => {
                redis
                    .kv
                    .load_from_rdb_file(dir.clone(), filename.clone())
                    .await
            }
            _ => Ok(()),
        };
        let _ = match &args.replicaof {
            Some(_) => redis.info.set_role("slave").await,
            _ => (),
        };
        match stream {
            Ok((stream, addr)) => {
                println!("accepted new connection from: {addr}");

                tokio::spawn(async move {
                    let mut framed = Framed::new(stream, RespParser);

                    while let Some(result) = framed.next().await {
                        match result {
                            Ok(value) => {
                                if let Some(response) = handle_command(value, addr, &redis).await {
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
