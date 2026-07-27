#[allow(unused)]
use crate::config::AppConfig;
use dashmap::DashMap;
use redis;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

pub struct Room {
    // room_id: i64,
    // room_name: String,
    uids: HashSet<i64>,
    user_count: u64,
}

#[derive(Clone)]
pub struct RoomState {
    rooms: Arc<DashMap<i64, RwLock<Room>>>,
    redis: Arc<redis::Client>,
    cfg: Arc<AppConfig>,
}

impl RoomState {
    pub fn new(cfg: Arc<AppConfig>) -> Self {
        let redis_client =
            redis::Client::open(cfg.redis.addr.as_str()).expect("Failed to open redis client");
        Self {
            rooms: Arc::new(DashMap::new()),
            redis: Arc::new(redis_client),
            cfg,
        }
    }

    // pub fn add_room(&self, room_id: i64, room_name: &str) {
    //     self.rooms
    //         .entry(room_id)
    //         .or_insert_with(|| RwLock::new(Room::new(room_id, room_name)));
    // }
    //
    // pub fn remove_room(&self, room_id: i64) {
    //     self.rooms.remove(&room_id);
    // }
    //
    // pub fn user_put_room(&self, room_id: i64, uid: i64) {
    //     let entry = match self.rooms.get(&room_id) {
    //         Some(v) => v,
    //         None => return,
    //     };
    //
    //     let mut room = match entry.write() {
    //         Ok(v) => v,
    //         Err(_) => return,
    //     };
    //
    //     if room.uids.insert(uid) {
    //         room.user_count += 1;
    //     }
    // }
    //
    // pub fn user_out_room(&self, room_id: i64, uid: &[i64]) {
    //     let entry = match self.rooms.get(&room_id) {
    //         Some(v) => v,
    //         None => return,
    //     };
    //
    //     let mut room = match entry.write() {
    //         Ok(v) => v,
    //         Err(_) => return,
    //     };
    //
    //     room.uids.retain(|u| !uid.contains(u));
    //     room.user_count = room.uids.len() as u64;
    //
    //     // 提前释放锁
    //     drop(room);
    //
    //     let entry_after = self.rooms.get(&room_id);
    //     if let Some(lock) = entry_after {
    //         let room_data = match lock.read() {
    //             Ok(v) => v,
    //             Err(_) => return,
    //         };
    //         if room_data.uids.is_empty() {
    //             self.remove_room(room_id);
    //         }
    //     }
    // }
    //
    pub fn room_uids(&self, room_id: i64) -> Vec<i64> {
        let entry = match self.rooms.get(&room_id) {
            Some(v) => v,
            None => return vec![],
        };

        let room = match entry.read() {
            Ok(v) => v,
            Err(_) => return vec![],
        };
        room.uids.iter().cloned().collect()
    }
}

impl Room {
    fn new(_room_id: i64, _room_name: &str) -> Self {
        Self {
            // room_id,
            // room_name: room_name.to_string(),
            uids: HashSet::new(),
            user_count: 0,
        }
    }
}
