use anyhow::Result;
use prost::Message;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing_subscriber::fmt::time;
// 引用项目内部protobuf
use rust_im::pb::Message as ImMessage;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:9092")
        .create()?;
    loop {
        let timeStr = now_ts_ms().to_string();
        // 推送消息：修改to_uid_list为你客户端握手登录的uid
        let push_msg = ImMessage {
            from_uid: 10001,
            to_uid_list: vec![10002],
            content: format!("消息：{:?}", timeStr),
            room_id: 0,
        };
        let msg_clone = push_msg.clone();

        let payload = push_msg.encode_to_vec();

        let record = FutureRecord::to("im-push").payload(&payload).key(b"msg");

        // println!("准备发送消息至 topic im-push");
        match producer
            .send(record, std::time::Duration::from_secs(1))
            .await
        {
            Ok(_) => println!("消息发送成功: {:?}", msg_clone),
            Err((err, _msg)) => eprintln!("消息发送失败: {err}"),
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    Ok(())
}

/// 获取当前 毫秒级时间戳 u128
pub fn now_ts_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
