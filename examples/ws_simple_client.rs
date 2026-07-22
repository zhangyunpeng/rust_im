use anyhow::Result;
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio::{spawn, sync::mpsc, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

use rust_im::pb::{HandshakeReq, Op, Packet};

#[tokio::main]
async fn main() -> Result<()> {
    // ========= 配置区 =========
    const WS_URL: &str = "ws://127.0.0.1:8091/ws";
    const LOGIN_UID: i64 = 10002;
    const HEARTBEAT_INTERVAL: u64 = 3; // 心跳间隔 秒
    // ==========================

    println!("正在连接 {}", WS_URL);
    let (ws_stream, _resp) = connect_async(WS_URL).await?;
    println!("WebSocket 连接成功");

    let (mut write, mut read) = ws_stream.split();
    let (tx, mut tx_rx) = mpsc::unbounded_channel::<Packet>();

    // 网络写任务：统一发包
    spawn(async move {
        while let Some(pkt) = tx_rx.recv().await {
            let bin = pkt.encode_to_vec();
            if write.send(WsMessage::Binary(bin)).await.is_err() {
                println!("❌ WS写通道断开");
                break;
            }
        }
    });

    // 1.发送握手包
    let handshake_req = HandshakeReq {
        uid: LOGIN_UID,
        token: "debug_token_123".to_string(),
        device_id: "debug_id".to_string(),
    };
    let handshake_pkt = Packet {
        op: Op::Handshake as u32,
        body: handshake_req.encode_to_vec(),
        len: 0,
    };
    tx.send(handshake_pkt).unwrap();
    println!("✅ 已发送登录握手 uid={LOGIN_UID}");

    // 2.后台心跳任务
    let tx_hb = tx.clone();
    spawn(async move {
        loop {
            sleep(std::time::Duration::from_secs(HEARTBEAT_INTERVAL)).await;
            let hb_pkt = Packet {
                op: Op::Heartbeat as u32,
                body: vec![],
                len: 0,
            };
            if tx_hb.send(hb_pkt).is_err() {
                println!("💔心跳通道关闭");
                break;
            }
            println!("💓发送心跳包");
        }
    });

    // 3.主线接收下行推送
    loop {
        match read.next().await {
            Some(Ok(WsMessage::Binary(payload))) => {
                let pkt = match Packet::decode(&payload[..]) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("❌ Packet解码失败: {e}");
                        continue;
                    }
                };

                println!("\n📩 收到数据包 op={}", pkt.op);

                if pkt.op == Op::PushMsg as u32 {
                    match rust_im::pb::Message::decode(&pkt.body[..]) {
                        Ok(push_msg) => {
                            println!("===== 推送消息内容 =====");
                            println!("from_uid: {}", push_msg.from_uid);
                            println!("content: {}", push_msg.content);
                            println!("========================");
                        }
                        Err(e) => eprintln!("❌ Push消息体解码失败: {e}"),
                    }
                }
            }
            Some(Ok(WsMessage::Close(_))) => {
                println!("连接被服务端关闭");
                break;
            }
            Some(Err(e)) => {
                eprintln!("❌ 网络异常: {e}");
                break;
            }
            None => {
                println!("流结束");
                break;
            }
            _ => {}
        }
    }

    Ok(())
}