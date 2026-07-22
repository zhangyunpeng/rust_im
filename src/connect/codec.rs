use crate::pb::Packet;
use bytes::{Buf, BufMut, BytesMut};
use prost::{DecodeError, Message};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protobuf decode error: {0}")]
    Protobuf(#[from] DecodeError),
}

#[derive(Clone)]
pub struct Codec {}

impl Decoder for Codec {
    type Item = Packet;
    type Error = CodecError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < 4 {
            return Ok(None);
        }
        // 【修改】小端读取长度
        let body_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let total_len = body_len + 4;
        if buf.len() < total_len {
            return Ok(None);
        }
        buf.advance(4);
        let body_data = buf.split_to(body_len);
        let pkt = Packet::decode(body_data.as_ref())?;
        Ok(Some(pkt))
    }
}

impl Encoder<Packet> for Codec {
    type Error = CodecError;
    fn encode(&mut self, item: Packet, buf: &mut BytesMut) -> Result<(), Self::Error> {
        let raw = item.encode_to_vec();
        // 小端写入，与decode配对
        buf.put_u32_le(raw.len() as u32);
        buf.put_slice(&raw);
        Ok(())
    }
}