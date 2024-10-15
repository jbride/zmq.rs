mod async_helpers;

use std::error::Error;
use std::fs;
use chrono;

use zeromq::{Socket, SocketRecv};

#[async_helpers::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut socket = zeromq::SubSocket::new();
    socket
        .connect("tcp://127.0.0.1:29001")
        .await
        .expect("Failed to connect");

    socket.subscribe("").await?;

    let mut i = 0;
    loop {
        println!("Message {}", i);
        let repl = socket.recv().await?;
        //dbg!(repl);
        let path = format!("/tmp/{:?}", chrono::offset::Local::now());
        fs::write(path, repl).expect("unable to write to file");
        i += 1;
    }
    Ok(())
}
