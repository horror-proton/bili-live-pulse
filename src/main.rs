use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let url = "wss://echo.websocket.org";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    let (mut write, mut read) = ws_stream.split();

    tokio::spawn(async move {
        loop {
            write
                .send(Message::Text("Hello WebSocket".into()))
                .await
                .expect("Failed to send message");

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    while let Some(msg) = read.next().await {
        let msg = msg.expect("Failed to read message");
        println!("Received: {:?}", msg);
    }

    println!("WebSocket handshake has been successfully completed");
}
