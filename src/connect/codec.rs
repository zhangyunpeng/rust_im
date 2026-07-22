use crate::pb::Packet;
use bytes::{Buf, BufMut, BytesMut};
use prost::{DecodeError, Message};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Error, Debug)]
pub enum CodecError {
    #[error("io error: {0}")]
    Ip(#[from] std::io::Error),
    #[error("protobuf decode error: {0}")]
    ProtoBuf(#[from] DecodeError),
}

#[derive(Clone)]
pub struct Codec {}

impl Decoder for Codec {
    type Item = Packet;
    type Error = CodecError;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 4 {
            return Ok(None);
        }

        let body_len = u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let total_len = body_len + 4;
        if src.len() < total_len {
            return Ok(None);
        }

        src.advance(4);
        let body_data = src.split_to(body_len);
        let pkt = Packet::decode(body_data)?;
        Ok(Some(pkt))
    }
}

impl Encoder<Packet> for Codec {
    type Error = CodecError;
    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let raw = item.encode_to_vec();
        dst.put_u32_le(raw.len() as u32);
        dst.put_slice(raw.as_slice());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{Packet, Op};
    use bytes::BytesMut;
    use tokio_util::codec::{Encoder, Decoder};

    fn create_test_packet(len: u32, op: Op) -> Packet {
        Packet {
            len,
            op: op as u32,
            body: b"hello".to_vec(),
        }
    }

    #[test]
    fn test_encode_decode_single() {
        let mut codec = Codec {};
        let mut buf = BytesMut::new();
        let pkt = create_test_packet(10, Op::Handshake);

        codec.encode(pkt.clone(), &mut buf).unwrap();
        let full_len = buf.len();
        assert_eq!(full_len, 4 + pkt.encoded_len());

        let decoded_pkt = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_pkt.len, 10);
        assert_eq!(decoded_pkt.op, Op::Handshake as u32);

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_partial_packet() {
        let mut codec = Codec {};
        let mut buf = BytesMut::new();
        let pkt = create_test_packet(10, Op::Handshake);

        codec.encode(pkt.clone(), &mut buf).unwrap();

        let full_len = buf.len();
        let mut partial_buf = buf.split_to(full_len/2);
        let decoded_pkt = codec.decode(&mut partial_buf).unwrap();
        assert!(decoded_pkt.is_none());

        partial_buf.unsplit(buf);
        let decoded_pkt = codec.decode(&mut partial_buf).unwrap().unwrap();
        assert_eq!(decoded_pkt.len, 10);
        assert_eq!(decoded_pkt.op, Op::Handshake as u32);

        assert!(partial_buf.is_empty());
    }

    #[test]
    fn test_decode_multiple_packets() {
        let mut codec = Codec {};
        let mut buf = BytesMut::new();
        let pkt1 = create_test_packet(10, Op::Handshake);
        let pkt2 = create_test_packet(100, Op::Heartbeat);
        let pkt3 = create_test_packet(1000, Op::PushMsg);

        codec.encode(pkt1, &mut buf).unwrap();
        codec.encode(pkt2, &mut buf).unwrap();
        codec.encode(pkt3, &mut buf).unwrap();

        let decoded_pkt1 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_pkt1.len, 10);
        assert_eq!(decoded_pkt1.op, Op::Handshake as u32);

        let decoded_pkt2 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_pkt2.len, 100);
        assert_eq!(decoded_pkt2.op, Op::Heartbeat as u32);

        let decoded_pkt3 = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded_pkt3.len, 1000);
        assert_eq!(decoded_pkt3.op, Op::PushMsg as u32);

        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_empty_packet() {
        let mut codec = Codec {};
        let mut buf = BytesMut::new();

        let pkt = codec.decode(&mut buf).unwrap();
        assert!(pkt.is_none());
    }
}
