use crate::connect::codec::Codec;
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
    let mut framed = Framed::new(stream, Codec {});
    let (tx, mut rx): (ConnSender, UnboundedReceiver<Packet>) = mpsc::unbounded_channel();

    let mut uid: Option<i64> = None;
    let mut last_hb = Instant::now();
    let hb_timeout = state.heartbeat_timeout();

    loop {
        tokio::select! {
            // 上行读取
            res = framed.next() => {
                match res {
                    Some(Ok(pkt)) => {
                        last_hb = Instant::now();
                        tracing::debug!("收到客户端上行数据包");
                        process_in_packet(pkt, &state, &tx, &mut uid).await?;
                    }
                    Some(Err(e)) => {
                        tracing::error!("decode error:{}", e);
                        break;
                    }
                    None => {
                        tracing::info!("tcp stream closed");
                        break;
                    }
                }
            }
            // 下行发送队列
            out_pkt = rx.recv() => {
                if let Some(pkt) = out_pkt {
                    if framed.send(pkt).await.is_err() {
                        tracing::warn!("下行发送失败，关闭连接");
                        break;
                    }
                } else {
                    tracing::info!("下行通道关闭");
                    break;
                }
            }
            // 心跳检测
            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                if Instant::now() - last_hb > hb_timeout {
                    tracing::info!("⚠️心跳超时，关闭tcp连接");
                    break;
                }
            }
        }
    }

    // 清理
    if let Some(u) = uid {
        state.remove_conn(u, &tx);
        tracing::info!("用户{}下线清理", u);
    }
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
