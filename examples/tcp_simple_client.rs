use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Result;
use futures::{SinkExt, StreamExt};
use prost::Message;
use tokio::{spawn, sync::mpsc, time::sleep};
use tokio_util::codec::Framed;
use rust_im::pb::Message as ImMessage;
use rust_im::connect::codec::Codec;
use rust_im::pb::{HandshakeReq, Op, Packet};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct ClientArgs {
    #[arg(short = 'u', long = "login uid", default_value = "0")]
    login_uid: i64,
    #[arg(short = 'f', long = "friend uids", value_delimiter = ',',  default_value = "0")]
    friend_uids: Vec<i64>,
}

#[tokio::main]
async fn main() -> Result<()> {

    // 解析启动参数
    let args = ClientArgs::parse();
    println!("服务启动参数配置：{:#?}", args);
    if args.login_uid < 0 || args.friend_uids.is_empty() {
        return Err(anyhow::Error::msg("u|f参数错误"));
    }

    const ADDR: &str = "127.0.0.1:8090";

    println!("连接tcp {}", ADDR);
    let stream = tokio::net::TcpStream::connect(ADDR).await?;
    let mut framed = Framed::new(stream, Codec {});

    // 发送队列
    let (tx, mut rx) = mpsc::unbounded_channel::<Packet>();

    let tx_hb = tx.clone();
    tokio::spawn(async move {
        // 1. 在循环外部创建定时器，保持状态
        let mut hb_interval = tokio::time::interval(std::time::Duration::from_secs(3));
        let mut msg_interval = tokio::time::interval(std::time::Duration::from_secs(4));

        loop {
            tokio::select! {
            // 2. 使用 &mut 引用，防止 select! 在分支触发时 drop 掉定时器状态
            _ = hb_interval.tick() => {
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
            },
            _ = msg_interval.tick() => {
                let msg = ImMessage {
                    from_uid: 10001,
                    to_uid_list: args.friend_uids.clone(),
                    content: format!("你好10002, {}", now_ts_sec()),
                    room_id: 0,
                };
                let pkt = Packet {
                    len: 0,
                    op: Op::SendMsg as u32,
                    body: msg.encode_to_vec(),
                };
                if tx_hb.send(pkt).is_err() {
                    println!("发送消息失败");
                    break;
                }
                println!("发送消息成功");
            }
        }
        }
    });

    // 发送握手包
    let handshake_req = HandshakeReq {
        uid: args.login_uid,
        token: "debug_token_123".to_string(),
        device_id: "debug_id".to_string(),
    };
    let pkt = Packet {
        op: Op::Handshake as u32,
        body: handshake_req.encode_to_vec(),
        len: 0,
    };
    tx.send(pkt).unwrap();
    println!("✅ TCP客户端登录成功 uid={}", args.login_uid);

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

pub fn now_ts_sec() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}