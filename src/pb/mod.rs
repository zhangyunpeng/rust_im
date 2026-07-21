pub mod generated {
    include!("generated/connect.rs");
}

pub use generated::{HandshakeReq, HandshakeResp, KickNotify, Message, Op, Packet};
