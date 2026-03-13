use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use omnipaxos::macros::Entry;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{OmniPaxos, OmniPaxosConfig};
// 引入持久化存储相关的组件
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
// ==========================================
// 1. 定义共识日志条目 (必须实现 Entry trait)
// ==========================================
#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub enum KVCommand {
    Read { key: u64, msg_id: u64, client: String },
    Write { key: u64, value: u64, msg_id: u64, client: String },
    Cas { key: u64, from: u64, to: u64, msg_id: u64, client: String },
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n').parse::<u64>().unwrap_or(0)
}

// ==========================================
// 2. Maelstrom 协议数据结构
// ==========================================
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Body {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub msg_id: Option<u64>,
    pub in_reply_to: Option<u64>,
    pub key: Option<u64>,
    pub value: Option<u64>,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub node_id: Option<String>,
    pub node_ids: Option<Vec<String>>,
    pub paxos_data: Option<Vec<u8>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub enum Event {
    Message(Message),
    Tick,
}

// ==========================================
// 3. 主程序逻辑
// ==========================================
fn main() {
    let (tx, rx) = mpsc::channel();

    // --- 线程 1: 异步读取 STDIN (避免阻塞 OmniPaxos Tick) ---
    let tx_in = tx.clone();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            if let Ok(line) = line {
                if let Ok(msg) = serde_json::from_str::<Message>(&line) {
                    let _ = tx_in.send(Event::Message(msg));
                }
            }
        }
    });

    // --- 线程 2: 定时器心跳 (10ms) ---
    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(10));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    // --- 节点内部状态 ---
    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId = 0;
    let mut kv_store: HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0; 
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;

    for event in rx {
        match event {
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {
                    // --- 场景 A: 初始化 & 崩溃恢复 ---
                    "init" => {
                        if let (Some(nid), Some(raw_nodes)) = (msg.body.node_id.clone(), msg.body.node_ids.clone()) {
                            my_node_id_str = nid;
                            my_pid = parse_node_id(&my_node_id_str);
                            let all_pids: Vec<NodeId> = raw_nodes.iter().map(|s| parse_node_id(s)).collect();

                            // 1. 配置持久化路径 (Maelstrom 环境下每个节点有独立目录)
                            let base_path = format!("storage_node_{}", my_pid);
                            let log_path = format!("{}/logs", base_path); // commitlog 需要自己的子目录

                            // --- 修正后的配置逻辑 ---
                            // commitlog::LogOptions 使用 new()
                            let commitlog_options = commitlog::LogOptions::new(log_path);
                            
                            // sled 必须使用方法链式调用设置路径
                            let sled_options = sled::Config::default()
                                .path(base_path.clone()); 

                            let storage_config = PersistentStorageConfig::with(base_path, commitlog_options, sled_options);
                            let storage = PersistentStorage::open(storage_config);

                            let mut node_config = OmniPaxosConfig::default();
                            node_config.server_config.pid = my_pid;
                            node_config.cluster_config.nodes = all_pids;
                            node_config.cluster_config.configuration_id = 1;

                            let op = node_config.build(storage).expect("Failed to build OmniPaxos");

                            // 重播 (Replay) 
                            if let Some(decided_entries) = op.read_decided_suffix(0) {
                                for entry in decided_entries {
                                    if let LogEntry::Decided(cmd) = entry {
                                        match cmd {
                                            KVCommand::Write { key, value, .. } => { kv_store.insert(key, value); },
                                            KVCommand::Cas { key, from, to, .. } => {
                                                if kv_store.get(&key) == Some(&from) { kv_store.insert(key, to); }
                                            },
                                            _ => {}
                                        }
                                        applied_idx += 1;
                                    }
                                }
                            }

                            omnipaxos = Some(op);
                            send_reply(&my_node_id_str, &msg.src, "init_ok", msg.body.msg_id, None);
                            eprintln!("✅ Node {} recovered/initialized. Applied Index: {}", my_node_id_str, applied_idx);
                        }
                    },

                    // --- 场景 B: 处理读/写/CAS (线性一致性) ---
                    "read" | "write" | "cas" => {
                        if let Some(op) = &mut omnipaxos {
                            let client = msg.src.clone();
                            if let Some(m_id) = msg.body.msg_id {
                                let cmd = match msg.body.msg_type.as_str() {
                                    "read" => msg.body.key.map(|k| KVCommand::Read { key: k, msg_id: m_id, client }),
                                    "write" => msg.body.key.and_then(|k| msg.body.value.map(|v| KVCommand::Write { key: k, value: v, msg_id: m_id, client })),
                                    "cas" => {
                                        if let (Some(k), Some(f), Some(t)) = (msg.body.key, msg.body.from, msg.body.to) {
                                            Some(KVCommand::Cas { key: k, from: f, to: t, msg_id: m_id, client })
                                        } else { None }
                                    },
                                    _ => None,
                                };
                                // 将读写一律视为提案，确保线性一致性
                                if let Some(c) = cmd { let _ = op.append(c); }
                            }
                        }
                    },

                    // --- 场景 C: 处理 Paxos 网络消息 ---
                    "paxos_net" => {
                        if let (Some(op), Some(data)) = (&mut omnipaxos, msg.body.paxos_data) {
                            if let Ok(p_msg) = bincode::deserialize(&data) { op.handle_incoming(p_msg); }
                        }
                    },
                    _ => {}
                }
            },

            // --- 场景 D: 定时器 Tick & 日志提交 ---
            Event::Tick => {
                if let Some(op) = &mut omnipaxos {
                    op.tick();
                    
                    // 1. 发送 Paxos 内部消息
                    for out_msg in op.outgoing_messages() {
                        let dest = format!("n{}", out_msg.get_receiver());
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            let net_msg = Message {
                                src: my_node_id_str.clone(), dest,
                                body: Body {
                                    msg_type: "paxos_net".to_string(), msg_id: None, in_reply_to: None,
                                    key: None, value: None, from: None, to: None, node_id: None, node_ids: None,
                                    paxos_data: Some(data), extra: serde_json::Map::new(),
                                }
                            };
                            println!("{}", serde_json::to_string(&net_msg).unwrap());
                        }
                    }

                    // 2. 应用已决定的日志到本地状态机 (HashMap)
                    if let Some(entries) = op.read_decided_suffix(applied_idx) {
                        for entry in entries {
                            if let LogEntry::Decided(cmd) = entry {
                                match cmd {
                                    KVCommand::Write { key, value, msg_id, client } => {
                                        kv_store.insert(key, value);
                                        send_reply(&my_node_id_str, &client, "write_ok", Some(msg_id), None);
                                    },
                                    KVCommand::Read { key, msg_id, client } => {
                                        let val = kv_store.get(&key).copied();
                                        send_reply(&my_node_id_str, &client, "read_ok", Some(msg_id), val);
                                    },
                                    KVCommand::Cas { key, from, to, msg_id, client } => {
                                        let current = kv_store.get(&key).copied();
                                        if current == Some(from) {
                                            kv_store.insert(key, to);
                                            send_reply(&my_node_id_str, &client, "cas_ok", Some(msg_id), None);
                                        } else {
                                            send_error(&my_node_id_str, &client, msg_id, 22, "CAS mismatch");
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
    }
}

// ==========================================
// 4. 辅助发送函数
// ==========================================
fn send_reply(src: &str, dest: &str, msg_type: &str, in_reply_to: Option<u64>, value: Option<u64>) {
    let msg = Message {
        src: src.to_string(), dest: dest.to_string(),
        body: Body {
            msg_type: msg_type.to_string(), msg_id: None, in_reply_to,
            key: None, value, from: None, to: None, node_id: None, node_ids: None, paxos_data: None, extra: serde_json::Map::new(),
        }
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_error(src: &str, dest: &str, in_reply_to: u64, code: u32, text: &str) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    let msg = Message {
        src: src.to_string(), dest: dest.to_string(),
        body: Body {
            msg_type: "error".to_string(), msg_id: None, in_reply_to: Some(in_reply_to),
            key: None, value: None, from: None, to: None, node_id: None, node_ids: None, paxos_data: None, extra,
        }
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}