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
// 1. KV 命令与持久化身份定义
// ==========================================
#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub enum KVCommand {
    Read  { key: u64, msg_id: u64, client: String },
    Write { key: u64, value: u64, msg_id: u64, client: String },
    Cas   { key: u64, from: u64, to: u64, msg_id: u64, client: String },
}

// 🌟 核心新增：身份证明文件结构，用于抵御冷重启导致的信息丢失
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Identity {
    pub my_node_id_str: String,
    pub my_pid: NodeId,
    pub all_pids: Vec<NodeId>,
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n').parse::<u64>().map(|id| id + 1).unwrap_or(1)
}

// ==========================================
// 2. Maelstrom 协议定义
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
// 3. 构建引擎 (防截断 + 高频物理刷盘)
// ==========================================
fn build_omnipaxos(my_pid: NodeId, all_pids: Vec<NodeId>, keep_storage: bool) -> (OmniPaxos<KVCommand, PersistentStorage<KVCommand>>, bool) {
    let base_path = format!("storage_node_{}", my_pid);
    let log_path  = format!("{}/logs", base_path);
    if !keep_storage { let _ = std::fs::remove_dir_all(&base_path); }
    let _ = std::fs::create_dir_all(&log_path);

    let make_storage = || {
        let commitlog_opts = commitlog::LogOptions::new(&log_path);
        // 🌟 核心改进：强化 Sled 的刷盘频率，应对不可预知的物理强杀
        let sled_opts = sled::Config::default().path(&base_path).flush_every_ms(Some(10));
        PersistentStorage::open(PersistentStorageConfig::with(base_path.clone(), commitlog_opts, sled_opts))
    };

    let mut node_config = OmniPaxosConfig::default();
    node_config.server_config.pid = my_pid;
    node_config.cluster_config.nodes = all_pids;
    node_config.cluster_config.configuration_id = 1;
    node_config.server_config.batch_size = 100000; // 禁止物理截断

    let op = node_config.build(make_storage()).expect("build failed");
    (op, keep_storage)
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
    let mut all_pids_cache: Vec<NodeId> = Vec::new();
    let mut kv_store = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;

    // =========================================================================
    // 🧱 宇宙级开机自检：检查硬盘中是否存在身份文件（物理冷重启恢复逻辑）
    // =========================================================================
    if let Ok(data) = std::fs::read_to_string("identity.json") {
        if let Ok(id) = serde_json::from_str::<Identity>(&data) {
            my_node_id_str = id.my_node_id_str;
            my_pid = id.my_pid;
            all_pids_cache = id.all_pids;
            
            eprintln!("🔄 [Node {}] 检测到物理冷重启！身份已恢复，准备带盘重建...", my_node_id_str);
            
            let (op, _) = build_omnipaxos(my_pid, all_pids_cache.clone(), true);
            let target = op.get_decided_idx();
            
            eprintln!("📂 [Node {}] 正在从硬盘重放历史日志，目标进度: {}", my_node_id_str, target);
            
            // 全量日志重放，构建内存状态机
            while applied_idx < target {
                if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))) {
                    if entries.is_empty() { break; }
                    for entry in entries {
                        if let LogEntry::Decided(cmd) = entry {
                            match cmd {
                                KVCommand::Write { key, value, .. } => { kv_store.insert(key, value); }
                                KVCommand::Cas { key, from, to, .. } => { 
                                    if kv_store.get(&key).copied() == Some(from) { kv_store.insert(key, to); } 
                                }
                                _ => {}
                            }
                            applied_idx += 1;
                        }
                    }
                } else { break; }
            }
            omnipaxos = Some(op);
            eprintln!("✅ [Node {}] 物理重塑完成！当前 Key 数: {}, 最终进度: {}", my_node_id_str, kv_store.len(), applied_idx);
        }
    }

    // =========================================================================
    // 🔁 事件主循环
    // =========================================================================
    for event in rx {
        match event {
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {
                    "init" => {
                        // 只有在没有被持久化身份文件唤醒时，才需要处理 init
                        if omnipaxos.is_none() {
                            my_node_id_str = msg.body.extra.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                            my_pid = parse_node_id(&my_node_id_str);
                            all_pids_cache = msg.body.extra.get("node_ids").and_then(|v| v.as_array()).unwrap_or(&vec![]).iter().map(|v| parse_node_id(v.as_str().unwrap())).collect();
                            
                            // 🌟 落笔成规：将自己的身份永久刻入硬盘
                            let id = Identity {
                                my_node_id_str: my_node_id_str.clone(),
                                my_pid,
                                all_pids: all_pids_cache.clone(),
                            };
                            let _ = std::fs::write("identity.json", serde_json::to_string(&id).unwrap());
                            
                            let (op, _) = build_omnipaxos(my_pid, all_pids_cache.clone(), false);
                            omnipaxos = Some(op);
                        }
                        // 无论如何，向 Maelstrom 确认开机就绪
                        send_res(&my_node_id_str, &msg.src, "init_ok", msg.body.msg_id.unwrap_or(0), None);
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
                                "cas" => {
                                    // 完美兼容 Maelstrom 各种边界情况
                                    let from = msg.body.from.unwrap_or(0);
                                    let to = msg.body.to.unwrap_or_else(|| msg.body.extra.get("to").and_then(|v| v.as_u64()).unwrap_or(0));
                                    Some(KVCommand::Cas { key: msg.body.key.unwrap_or(0), from, to, msg_id: msg.body.msg_id.unwrap_or(0), client })
                                },
                                _ => None,
                            };
                            if let Some(c) = cmd { let _ = op.append(c); }
                        }
                    }
                    _ => {}
                }
            }
            Event::Tick => {
                if let Some(op) = &mut omnipaxos {
                    let _ = op.tick();
                    
                    for out_msg in op.outgoing_messages() {
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            println!("{}", serde_json::to_string(&Message { src: my_node_id_str.clone(), dest: format!("n{}", out_msg.get_receiver() - 1), body: Body { msg_type: "paxos_net".to_string(), msg_id: None, in_reply_to: None, key: None, value: None, from: None, to: None, paxos_data: Some(data), extra: serde_json::Map::new() } }).unwrap());
                        }
                    }

                    // 推进状态机应用
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
                                            if let Some(v) = kv_store.get(&key).copied() {
                                                send_res(&my_node_id_str, &client, "read_ok", msg_id, Some(v));
                                            } else {
                                                send_err(&my_node_id_str, &client, msg_id, 20, "key does not exist");
                                            }
                                        }
                                        KVCommand::Cas { key, from, to, msg_id, client } => {
                                            if kv_store.get(&key).copied() == Some(from) {
                                                kv_store.insert(key, to);
                                                send_res(&my_node_id_str, &client, "cas_ok", msg_id, None);
                                            } else {
                                                send_err(&my_node_id_str, &client, msg_id, 22, "CAS mismatch");
                                            }
                                        }
                                    }
                                    applied_idx += 1;
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