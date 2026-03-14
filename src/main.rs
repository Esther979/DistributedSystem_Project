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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Identity {
    pub my_node_id_str: String,
    pub my_pid: NodeId,
    pub all_pids: Vec<NodeId>,
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n').parse::<u64>().map(|id| id + 1).unwrap_or(1)
}

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

fn build_omnipaxos(my_pid: NodeId, all_pids: Vec<NodeId>) -> OmniPaxos<KVCommand, PersistentStorage<KVCommand>> {
    let base_path = format!("storage_node_{}", my_pid);
    let log_path  = format!("{}/logs", base_path);
    let _ = std::fs::create_dir_all(&log_path);

    let make_storage = || {
        let commitlog_opts = commitlog::LogOptions::new(&log_path);
        let sled_opts = sled::Config::default().path(&base_path).flush_every_ms(Some(10));
        PersistentStorage::open(PersistentStorageConfig::with(base_path.clone(), commitlog_opts, sled_opts))
    };

    let mut node_config = OmniPaxosConfig::default();
    node_config.server_config.pid = my_pid;
    node_config.cluster_config.nodes = all_pids;
    node_config.cluster_config.configuration_id = 1;
    node_config.server_config.batch_size = 100000; 

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
    let mut all_pids_cache: Vec<NodeId> = Vec::new();
    let mut kv_store = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;

    let start_time = std::time::Instant::now();
    let mut simulated_crash_done = false;
    let mut is_recovering = false;
    let mut recovery_target = 0;

    for event in rx {
        match event {
            Event::Message(msg) => {
                // 🧱 节点处于失忆状态（刚开机，或刚经历过宕机炸弹）
                if omnipaxos.is_none() {
                    if msg.body.msg_type == "init" {
                        // 1. 正常开机：记录身份并写下物理遗书
                        my_node_id_str = msg.body.extra.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        my_pid = parse_node_id(&my_node_id_str);
                        all_pids_cache = msg.body.extra.get("node_ids").and_then(|v| v.as_array()).unwrap_or(&vec![]).iter().map(|v| parse_node_id(v.as_str().unwrap())).collect();
                        
                        let id = Identity {
                            my_node_id_str: my_node_id_str.clone(),
                            my_pid,
                            all_pids: all_pids_cache.clone(),
                        };
                        let identity_file = format!("identity_{}.json", my_node_id_str);
                        let _ = std::fs::write(&identity_file, serde_json::to_string(&id).unwrap());
                        
                        omnipaxos = Some(build_omnipaxos(my_pid, all_pids_cache.clone()));
                        send_res(&my_node_id_str, &msg.src, "init_ok", msg.body.msg_id.unwrap_or(0), None);
                        continue;
                    } else {
                        // 2. 被宕机炸弹清空内存后，收到了新请求，准备复活！
                        my_node_id_str = msg.dest.clone();
                        my_pid = parse_node_id(&my_node_id_str);
                        let identity_file = format!("identity_{}.json", my_node_id_str);
                        
                        if let Ok(data) = std::fs::read_to_string(&identity_file) {
                            if let Ok(id) = serde_json::from_str::<Identity>(&data) {
                                all_pids_cache = id.all_pids;
                                eprintln!("🔄 [Node {}] 从黑暗中苏醒！成功读取身份文件，正在重组引擎...", my_node_id_str);
                                
                                let op = build_omnipaxos(my_pid, all_pids_cache.clone());
                                recovery_target = op.get_decided_idx();
                                omnipaxos = Some(op);
                                is_recovering = true;
                            }
                        }
                    }
                }

                // 处理所有的正常请求
                match msg.body.msg_type.as_str() {
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
                // 💣 内部定时炸弹 (代替 nemesis kill，满足 Maelstrom CLI 限制)
                if !simulated_crash_done && start_time.elapsed().as_secs() > 15 && my_pid == 2 {
                    eprintln!("💥 [Node n1] 触发内核崩溃模拟！内存引擎已彻底销毁！");
                    omnipaxos = None; // 将引擎置空，进入失忆状态
                    kv_store.clear();
                    applied_idx = 0;
                    simulated_crash_done = true;
                    is_recovering = false;
                }

                if let Some(op) = &mut omnipaxos {
                    // 🌟 异步温和恢复逻辑（防 ErrHelper）
                    if is_recovering {
                        if applied_idx < recovery_target {
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
                            eprintln!("✅ [Node {}] 全量日志重放完成！重归战场！idx: {}", my_node_id_str, applied_idx);
                            is_recovering = false;
                        }
                    }

                    let _ = op.tick();
                    for out_msg in op.outgoing_messages() {
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            println!("{}", serde_json::to_string(&Message { src: my_node_id_str.clone(), dest: format!("n{}", out_msg.get_receiver() - 1), body: Body { msg_type: "paxos_net".to_string(), msg_id: None, in_reply_to: None, key: None, value: None, from: None, to: None, paxos_data: Some(data), extra: serde_json::Map::new() } }).unwrap());
                        }
                    }

                    // 正常推进状态机
                    if !is_recovering {
                        let d_idx = op.get_decided_idx();
                        if d_idx > applied_idx {
                            if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))) {
                                for entry in entries.into_iter().take(50) {
                                    if let LogEntry::Decided(cmd) = entry {
                                        match cmd {
                                            KVCommand::Write { key, value, msg_id, client } => {
                                                kv_store.insert(key, value);
                                                send_res(&my_node_id_str, &client, "write_ok", msg_id, None);
                                            }
                                            KVCommand::Read { key, msg_id, client } => {
                                                if let Some(v) = kv_store.get(&key).copied() { send_res(&my_node_id_str, &client, "read_ok", msg_id, Some(v)); }
                                                else { send_err(&my_node_id_str, &client, msg_id, 20, "key does not exist"); }
                                            }
                                            KVCommand::Cas { key, from, to, msg_id, client } => {
                                                if kv_store.get(&key).copied() == Some(from) {
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