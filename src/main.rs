#![allow(unused_imports)]
use std::{
    io::{Read, Write},
    net::TcpListener,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let mut buf = [0; 512];
                loop {
                    println!("accepted new connection");
                    let read_count = stream
                        .read(&mut buf)
                        .expect("Failed to read the stream to buffer");
                    if read_count == 0 {
                        break;
                    }
                    stream
                        .write_all(b"+PONG\r\n")
                        .expect("Failed to write back to the stream");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
