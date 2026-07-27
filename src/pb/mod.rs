pub mod generated {
    pub mod connect {
        include!("generated/connect.rs");
    }
    pub mod comet {
        include!("generated/comet.rs");
    }
}

pub use generated::{
    comet::RemotePushReq, comet::RemotePushResp,
    comet::comet_push_service_client::CometPushServiceClient,
    comet::comet_push_service_server::CometPushService,
    comet::comet_push_service_server::CometPushServiceServer, connect::HandshakeReq,
    connect::HandshakeResp, connect::KickNotify, connect::Message, connect::Op, connect::Packet,
};
