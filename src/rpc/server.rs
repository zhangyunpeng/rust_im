use crate::connect::state::CometState;
use crate::pb::{CometPushService, CometPushServiceServer, Packet, RemotePushReq, RemotePushResp};
use anyhow::Result;
use prost::Message;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct RpcServer {
    state: Arc<CometState>,
}

impl RpcServer {
    pub fn new(state: Arc<CometState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl CometPushService for RpcServer {
    async fn remote_push(
        &self,
        request: Request<RemotePushReq>,
    ) -> Result<Response<RemotePushResp>, Status> {
        let body = request.into_inner();
        let uid = body.uid;

        let packet = Packet::decode(body.packet_bin.as_slice())
            .map_err(|e| Status::invalid_argument(format!("数据包解析失败: {e}")))?;

        if let Some(channels) = self.state.online.get(&uid) {
            for ch in channels.iter() {
                let _ = ch.send(packet.clone());
            }
            return Ok(Response::new(RemotePushResp {
                success: true,
                msg: "ok".to_string(),
            }));
        }

        // 用户离线
        Ok(Response::new(RemotePushResp {
            success: false,
            msg: format!("uid {uid} 当前无在线连接"),
        }))
    }
}

/// 启动gRPC服务
pub async fn start_rpc_server(state: Arc<CometState>, grpc_listen: &str) -> Result<()> {
    let service = RpcServer::new(state);
    tonic::transport::Server::builder()
        // 注册生成的服务
        .add_service(CometPushServiceServer::new(service))
        .serve(grpc_listen.parse()?)
        .await?;
    Ok(())
}
