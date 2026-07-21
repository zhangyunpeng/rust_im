use crate::connect::codec::{Codec, CodecError};
use crate::connect::state::{CometState, ConnSender};
use crate::pb::{HandshakeReq, HandshakeResp, Op, Packet};
use futures::{SinkExt, StreamExt};
use prost::Message;
use std::time::Instant;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio_util::codec::Framed;

pub async fn handle_tcp_stream(
    stream: tokio::net::TcpStream,
    state: CometState,
) -> anyhow::Result<()> {
    let framed = Framed::new(stream, Codec {});
    let (mut writer, mut reader) = framed.split();
    let (tx, mut rx): (ConnSender, UnboundedReceiver<Packet>) = mpsc::unbounded_channel();

    let mut uid: Option<i64> = None;
    let mut last_hb = Instant::now();
    let hb_timeout = state.heartbeat_timeout();

    // 下行写任务
    let write_task = tokio::spawn(async move {
        while let Some(pkt) = rx.recv().await {
            if writer.send(pkt).await.is_err() {
                break;
            }
        }
    });

    loop {
        tokio::select! {
            res = reader.next() => {
                match res {
                    Some(Ok(pkt)) => {
                        last_hb = Instant::now();
                        process_in_packet(pkt, &state, &tx, &mut uid).await?;
                    }
                    Some(Err(CodecError::Io(_))) | None => break,
                    Some(Err(e)) => {
                        tracing::warn!("tcp decode error:{}", e);
                        break;
                    },
                }
            },
            _ = tokio::time::sleep(hb_timeout) => {
                if Instant::now() - last_hb > hb_timeout {
                    tracing::info!("heartbeat timeout, close connection");
                    break;
                }
            }
        }
    }

    // 连接销毁清理
    if let Some(u) = uid {
        state.remove_conn(u, &tx);
    }
    drop(tx);
    let _ = write_task.await;
    Ok(())
}

/// 鉴权模拟，生产替换为 gRPC/http 调用logic服务
async fn verify_token(_uid: i64, _token: &str) -> bool {
    true
}

/// 数据公共处理函数 (TCP/WS复用)
pub async fn process_in_packet(
    pkt: Packet,
    state: &CometState,
    tx: &ConnSender,
    uid: &mut Option<i64>,
) -> anyhow::Result<()> {
    match Op::try_from(pkt.op as i32) {
        Ok(Op::Handshake) => {
            let req = HandshakeReq::decode(&pkt.body[..])?;
            let auth_ok = verify_token(req.uid, &req.token).await;
            let resp = if auth_ok {
                *uid = Some(req.uid);
                state.add_conn(req.uid, tx.clone());
                HandshakeResp {
                    code: 0,
                    msg: "ok".to_string(),
                    heartbeat_ms: state.heartbeat_timeout().as_millis() as i64 / 2,
                }
            } else {
                HandshakeResp {
                    code: -1,
                    msg: "invalid token".to_string(),
                    heartbeat_ms: 0,
                }
            };
            let resp_pkt = Packet {
                op: Op::Handshake as u32,
                body: resp.encode_to_vec(),
                len: 0,
            };
            let _ = tx.send(resp_pkt);
        }
        Ok(Op::Heartbeat) => {
            let resp_pkt = Packet {
                op: Op::Heartbeat as u32,
                body: vec![],
                len: 0,
            };
            let _ = tx.send(resp_pkt);
        }
        Ok(Op::SendMsg) => {
            state.send_job_kafka(pkt).await?;
        }
        Ok(_) => {}
        Err(_) => {}
    }
    Ok(())
}
