use clap::Parser;
use futures::{SinkExt, StreamExt};
use redis::commands::handle_command;
use redis::redis::Redis;
use redis::resp::RespParser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

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

    // Load RDB
    let _ = match (&args.dir, &args.dbfilename) {
        (Some(dir), Some(filename)) => {
            redis
                .kv
                .load_from_rdb_file(dir.clone(), filename.clone())
                .await
        }
        _ => Ok(()),
    };

    // Connect to master
    if let Some(replicaof) = &args.replicaof {
        let addr = replicaof.clone();
        redis.info.set_role("slave").await;
        let redis_clone = redis.clone();
        let port_clone = port.clone();
        tokio::spawn(async move {
            connect_to_master(redis_clone, &addr, &port_clone).await;
        });
    }

    //accept connections in a loop
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("accepted new connection from: {addr}");
                let redis = redis.clone();
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

use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn connect_to_master(_redis: Arc<Redis>, master_addr: &str, port: &str) {
    let (host, mport) = master_addr.split_once(' ').unwrap();
    let addr = format!("{host}:{mport}");
    println!("Connecting to master at {}", addr);

    match tokio::net::TcpStream::connect(&addr).await {
        Ok(mut stream) => {
            println!("Connected to master");

            //Send PING
            stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = stream.read(&mut buf).await.unwrap();
            println!("Master replied: {}", String::from_utf8_lossy(&buf[..n]));

            //Send REPLCONF listening-port
            let cmd = format!(
                "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
                port.len(),
                port
            );
            stream.write_all(cmd.as_bytes()).await.unwrap();
            let n = stream.read(&mut buf).await.unwrap();
            println!("Master replied: {}", String::from_utf8_lossy(&buf[..n]));

            //Send REPLCONF capa psync2
            stream
                .write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
                .await
                .unwrap();
            let n = stream.read(&mut buf).await.unwrap();
            println!("Master replied: {}", String::from_utf8_lossy(&buf[..n]));

            //Send PSYNC ? -1
            stream
                .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
                .await
                .unwrap();
            let n = stream.read(&mut buf).await.unwrap();
            println!("Master replied: {}", String::from_utf8_lossy(&buf[..n]));

            // TODO: Parse and load the RDB file sent after FULLRESYNC
            let empty_rdb = hex::decode("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap();
            let rdb_response = format!("${}\r\n", empty_rdb.len());
            stream.write_all(rdb_response.as_bytes()).await.unwrap();
            stream.write_all(&empty_rdb).await.unwrap();
        }
        Err(e) => {
            eprintln!("Failed to connect to master: {}", e);
        }
    }
}
