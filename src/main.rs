use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::panic;

use omnipaxos::macros::Entry;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};

// ==========================================
// 1. KV 命令定义
// ==========================================
#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub enum KVCommand {
    Read  { key: u64, msg_id: u64, client: String },
    Write { key: u64, value: u64, msg_id: u64, client: String },
    Cas   { key: u64, from: u64, to: u64, msg_id: u64, client: String },
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n').parse::<u64>().map(|id| id + 1).unwrap_or(1)
}

// ==========================================
// 2. Maelstrom 协议
// ==========================================
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub src:  String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paxos_data: Option<Vec<u8>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub enum Event {
    Message(Message),
    Tick,
    Shutdown, 
}

// ==========================================
// 3. 构建函数 (禁用截断以保全日志)
// ==========================================
fn build_omnipaxos(my_pid: NodeId, all_pids: Vec<NodeId>) -> OmniPaxos<KVCommand, PersistentStorage<KVCommand>> {
    let base_path = format!("storage_node_{}", my_pid);
    let log_path  = format!("{}/logs", base_path);
    let _ = std::fs::create_dir_all(&log_path);

    let make_storage = || {
        let commitlog_opts = commitlog::LogOptions::new(&log_path);
        let sled_opts = sled::Config::default().path(&base_path);
        PersistentStorage::open(PersistentStorageConfig::with(base_path.clone(), commitlog_opts, sled_opts))
    };

    let mut node_config = OmniPaxosConfig::default();
    node_config.server_config.pid = my_pid;
    node_config.cluster_config.nodes = all_pids;
    node_config.cluster_config.configuration_id = 1;
    node_config.server_config.batch_size = 100000; // 禁止物理截断

    node_config.build(make_storage()).expect("build failed")
}

fn main() {
    let (tx, rx) = mpsc::channel();
    let tx_in = tx.clone();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines().flatten() {
            if let Ok(msg) = serde_json::from_str::<Message>(&line) { let _ = tx_in.send(Event::Message(msg)); }
        }
        let _ = tx_in.send(Event::Shutdown);
    });

    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(10));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId = 0;
    let mut kv_store = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;
    let start_time = std::time::Instant::now();
    
    // 恢复状态机核心变量
    let mut is_recovering = false;
    let mut recovery_target = 0;
    let mut simulated_crash_done = false;

    for event in rx {
        match event {
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {
                    "init" => {
                        my_node_id_str = msg.body.extra.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        my_pid = parse_node_id(&my_node_id_str);
                        let all_pids = msg.body.extra.get("node_ids").and_then(|v| v.as_array()).unwrap_or(&vec![]).iter().map(|v| parse_node_id(v.as_str().unwrap())).collect();
                        omnipaxos = Some(build_omnipaxos(my_pid, all_pids));
                        
                        println!("{}", serde_json::to_string(&Message { src: my_node_id_str.clone(), dest: msg.src, body: Body { msg_type: "init_ok".to_string(), msg_id: Some(0), in_reply_to: msg.body.msg_id, key: None, value: None, from: None, to: None, paxos_data: None, extra: serde_json::Map::new() } }).unwrap());
                    }
                    "paxos_net" => {
                        if let (Some(data), Some(op)) = (msg.body.paxos_data, &mut omnipaxos) {
                            if let Ok(p_msg) = bincode::deserialize(&data) { let _ = op.handle_incoming(p_msg); }
                        }
                    }
                    "read" | "write" | "cas" => {
                        if let Some(op) = &mut omnipaxos {
                            let client = msg.src.clone();
                            let cmd = match msg.body.msg_type.as_str() {
                                "read" => Some(KVCommand::Read { key: msg.body.key.unwrap_or(0), msg_id: msg.body.msg_id.unwrap_or(0), client }),
                                "write" => Some(KVCommand::Write { key: msg.body.key.unwrap_or(0), value: msg.body.value.unwrap_or(0), msg_id: msg.body.msg_id.unwrap_or(0), client }),
                                "cas" => Some(KVCommand::Cas { key: msg.body.key.unwrap_or(0), from: msg.body.from.unwrap_or(0), to: msg.body.to.unwrap_or(0), msg_id: msg.body.msg_id.unwrap_or(0), client }),
                                _ => None,
                            };
                            if let Some(c) = cmd { let _ = op.append(c); }
                        }
                    }
                    _ => {}
                }
            }
            Event::Tick => {
                // 💣 触发模拟断电
                if !simulated_crash_done && start_time.elapsed().as_secs() > 15 && my_pid == 1 {
                    eprintln!("💥 [Node n0] RAM清空，开始异步热重启...");
                    kv_store.clear();
                    applied_idx = 0;
                    if let Some(op) = &omnipaxos { recovery_target = op.get_decided_idx(); }
                    is_recovering = true;
                    simulated_crash_done = true;
                }

                if let Some(op) = &mut omnipaxos {
                    // 🌟 异步温和恢复逻辑
                    if is_recovering {
                        if applied_idx < recovery_target {
                            // 每次 Tick 只尝试读取 30 条，有效防止 ErrHelper
                            if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))) {
                                for entry in entries.into_iter().take(30) {
                                    if let LogEntry::Decided(cmd) = entry {
                                        match cmd {
                                            KVCommand::Write { key, value, .. } => { kv_store.insert(key, value); }
                                            KVCommand::Cas { key, from, to, .. } => { if kv_store.get(&key) == Some(&from) { kv_store.insert(key, to); } }
                                            _ => {}
                                        }
                                        applied_idx += 1;
                                    }
                                }
                            }
                        } else {
                            eprintln!("✅ [Node n0] 全量日志重放完成！idx: {}", applied_idx);
                            is_recovering = false;
                        }
                    }

                    let _ = op.tick();
                    for out_msg in op.outgoing_messages() {
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            println!("{}", serde_json::to_string(&Message { src: my_node_id_str.clone(), dest: format!("n{}", out_msg.get_receiver() - 1), body: Body { msg_type: "paxos_net".to_string(), msg_id: None, in_reply_to: None, key: None, value: None, from: None, to: None, paxos_data: Some(data), extra: serde_json::Map::new() } }).unwrap());
                        }
                    }

                    // 只有在非恢复状态下才处理状态机应用，保证线性一致性
                    if !is_recovering {
                        let d_idx = op.get_decided_idx();
                        if d_idx > applied_idx {
                            if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))) {
                                for entry in entries {
                                    if let LogEntry::Decided(cmd) = entry {
                                        match cmd {
                                            KVCommand::Write { key, value, msg_id, client } => {
                                                kv_store.insert(key, value);
                                                send_res(&my_node_id_str, &client, "write_ok", msg_id, None);
                                            }
                                            KVCommand::Read { key, msg_id, client } => {
                                                let val = kv_store.get(&key).copied();
                                                if let Some(v) = val { send_res(&my_node_id_str, &client, "read_ok", msg_id, Some(v)); }
                                                else { send_err(&my_node_id_str, &client, msg_id, 20, "key does not exist"); }
                                            }
                                            KVCommand::Cas { key, from, to, msg_id, client } => {
                                                if kv_store.get(&key) == Some(&from) {
                                                    kv_store.insert(key, to);
                                                    send_res(&my_node_id_str, &client, "cas_ok", msg_id, None);
                                                } else { send_err(&my_node_id_str, &client, msg_id, 22, "CAS mismatch"); }
                                            }
                                        }
                                        applied_idx += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Event::Shutdown => break,
        }
    }
}

fn send_res(src: &str, dest: &str, t: &str, in_rep: u64, val: Option<u64>) {
    println!("{}", serde_json::to_string(&Message { src: src.to_string(), dest: dest.to_string(), body: Body { msg_type: t.to_string(), msg_id: Some(0), in_reply_to: Some(in_rep), key: None, value: val, from: None, to: None, paxos_data: None, extra: serde_json::Map::new() } }).unwrap());
}

fn send_err(src: &str, dest: &str, in_rep: u64, code: u32, text: &str) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    println!("{}", serde_json::to_string(&Message { src: src.to_string(), dest: dest.to_string(), body: Body { msg_type: "error".to_string(), msg_id: Some(0), in_reply_to: Some(in_rep), key: None, value: None, from: None, to: None, paxos_data: None, extra } }).unwrap());
}