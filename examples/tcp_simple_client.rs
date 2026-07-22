use anyhow::Result;
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio::{spawn, sync::mpsc, time::sleep};
use tokio_util::codec::Framed;

use rust_im::connect::codec::Codec;
use rust_im::pb::{HandshakeReq, Op, Packet};

#[tokio::main]
async fn main() -> Result<()> {
    const ADDR: &str = "127.0.0.1:8090";
    const LOGIN_UID: i64 = 10002;

    println!("连接tcp {}", ADDR);
    let stream = tokio::net::TcpStream::connect(ADDR).await?;
    let mut framed = Framed::new(stream, Codec {});

    // 发送队列
    let (tx, mut rx) = mpsc::unbounded_channel::<Packet>();

    // 心跳后台任务 → 只往通道丢包，不操作framed
    let tx_hb = tx.clone();
    spawn(async move {
        loop {
            sleep(std::time::Duration::from_secs(3)).await;
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

    // 发送握手包
    let handshake_req = HandshakeReq {
        uid: LOGIN_UID,
        token: "debug_token_123".to_string(),
        device_id: "debug_id".to_string(),
    };
    let pkt = Packet {
        op: Op::Handshake as u32,
        body: handshake_req.encode_to_vec(),
        len: 0,
    };
    tx.send(pkt).unwrap();
    println!("✅ TCP客户端登录成功 uid={LOGIN_UID}");

    // 【重点】读写全部在主线程同一个协程，不split
    loop {
        tokio::select! {
            // 接收服务端下行推送
            res = framed.next() => {
                match res {
                    Some(Ok(packet)) => {
                        println!("\n📩 TCP收到包 op={}", packet.op);
                        if packet.op == Op::PushMsg as u32 {
                            match rust_im::pb::Message::decode(&packet.body[..]) {
                                Ok(m) => println!("推送内容: {}", m.content),
                                Err(e) => eprintln!("解码失败: {e}"),
                            }
                        }
                    }
                    Some(Err(e)) => {
                        eprintln!("连接异常 {e}");
                        break;
                    }
                    None => {
                        println!("连接断开");
                        break;
                    }
                }
            }
            // 发送队列往外写网络
            out_pkt = rx.recv() => {
                if let Some(pkt) = out_pkt {
                    if framed.send(pkt).await.is_err() {
                        println!("下行写入失败，连接断开");
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    Ok(())
}
