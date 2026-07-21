IM Connect 层，核心依赖：
1. tokio：异步 runtime、TCP、WebSocket
2. tokio-util：编解码、帧处理
3. serde/serde_json：协议序列化
4. dashmap：线程安全在线连接表
5. kafka：消息队列转发
6. axum + tokio-tungstenite：ws 服务
7. prost：protobuf 协议