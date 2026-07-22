use super::state::CometState;
use crate::pb::{Message as ImMessage, Op, Packet};
use prost::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as rdMessage};

pub async fn start_push_consumer(state: CometState) -> anyhow::Result<()> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .set("group.id", "rust-comet-push")
        .set("auto.offset.reset", "earliest")
        .create()?;
    consumer.subscribe(&["im-push"])?;
    tracing::info!("push kafka consumer running");

    loop {
        let msg = consumer.recv().await?;
        let payload = msg.payload().ok_or(anyhow::anyhow!("empty payload"))?;
        let im_msg = ImMessage::decode(payload)?;
        let pkt = Packet {
            op: Op::PushMsg as u32,
            body: im_msg.encode_to_vec(),
            len: 0,
        };
        let _ = state.push_users(&im_msg.to_uid_list, pkt).await;
    }
}
