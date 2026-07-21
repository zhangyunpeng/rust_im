use crate::connect::session::process_in_packet;
use crate::connect::state::{CometState, ConnSender};
use crate::pb::Packet;
use axum::{
    Router,
    extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade},
    routing::get,
};
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

pub fn build_ws_router(state: CometState) -> Router {
    Router::new().route(
        "/ws",
        get(move |upgrade: WebSocketUpgrade| async move {
            upgrade.on_upgrade(|socket| async move {
                if let Err(e) = handle_ws_conn(socket, state).await {
                    tracing::warn!("ws connection closed err: {}", e);
                }
            })
        }),
    )
}

async fn handle_ws_conn(ws: WebSocket, state: CometState) -> anyhow::Result<()> {
    let (mut ws_write, mut ws_read) = ws.split();

    let (tx, mut rx): (ConnSender, UnboundedReceiver<Packet>) = unbounded_channel();
    let mut uid: Option<i64> = None;
    let mut last_hb = std::time::Instant::now();
    let hb_timeout = state.heartbeat_timeout();

    // 下行写任务
    let write_task = tokio::spawn(async move {
        while let Some(pkt) = rx.recv().await {
            let bin = pkt.encode_to_vec();
            if ws_write.send(WsMessage::Binary(bin)).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            // ✅ Stream 使用 .next() 不是 recv()
            msg = ws_read.next() => {
                match msg {
                    Some(Ok(WsMessage::Binary(data))) => {
                        last_hb = std::time::Instant::now();
                        let pkt = Packet::decode(&data[..])?;
                        process_in_packet(pkt, &state, &tx, &mut uid).await?;
                    }
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    _ => {}
                }
            }
            _ = tokio::time::sleep(hb_timeout) => {
                if std::time::Instant::now() - last_hb > hb_timeout {
                    tracing::info!("ws heartbeat timeout, close connection");
                    break;
                }
            }
        }
    }

    if let Some(u) = uid {
        state.remove_conn(u, &tx);
    }
    drop(tx);
    let _ = write_task.await;
    Ok(())
}
