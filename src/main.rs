use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{self, BufRead};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::panic; // 引入 panic 拦截机制

use omnipaxos::macros::Entry;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{OmniPaxos, OmniPaxosConfig};
// 引入持久化存储相关的组件
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};

// ==========================================
// 1. 定义共识日志条目
// ==========================================
#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub enum KVCommand {
    Read { key: u64, msg_id: u64, client: String },
    Write { key: u64, value: u64, msg_id: u64, client: String },
    Cas { key: u64, from: u64, to: u64, msg_id: u64, client: String },
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n')
     .parse::<u64>()
     .map(|id| id + 1) 
     .unwrap_or(1) 
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
    pub node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(10));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId = 0;
    let mut kv_store: HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut reply_msg_id: u64 = 0;  // ← 新增：reply 的 msg_id 计数器
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;

    for event in rx {
        match event {
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {
                   "init" => {
                        if let (Some(nid), Some(raw_nodes)) = (msg.body.node_id.clone(), msg.body.node_ids.clone()) {
                            my_node_id_str = nid;
                            my_pid = parse_node_id(&my_node_id_str);
                            let all_pids: Vec<NodeId> = raw_nodes.iter().map(|s| parse_node_id(s)).collect();

                            let base_path = format!("storage_node_{}", my_pid);
                            let log_path = format!("{}/logs", base_path);
                            
                            // 🔥 核心修复 1：强制清理上一轮测试的脏数据，防止跨测试污染导致 ErrHelper
                            let _ = std::fs::remove_dir_all(&base_path);
                            let _ = std::fs::create_dir_all(&log_path);

                            let commitlog_options = commitlog::LogOptions::new(log_path);
                            let sled_options = sled::Config::default().path(base_path.clone());
                            let storage_config = PersistentStorageConfig::with(base_path, commitlog_options, sled_options);
                            let storage = PersistentStorage::open(storage_config);

                            let mut node_config = OmniPaxosConfig::default();
                            node_config.server_config.pid = my_pid;
                            node_config.cluster_config.nodes = all_pids;
                            node_config.cluster_config.configuration_id = 1;

                            let op = node_config.build(storage).expect("Failed to build OmniPaxos");

                            applied_idx = 0;
                            let d_idx = op.get_decided_idx(); 
                            
                            if d_idx > 0 {
                                // 🔥 核心修复 2：用 catch_unwind 拦截官方库的 unwrap 报错
                                let read_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                                    op.read_decided_suffix(0)
                                }));

                                if let Ok(Some(decided_entries)) = read_result {
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
                            }

                            omnipaxos = Some(op);
                            reply_msg_id += 1;
                            send_reply(&my_node_id_str, &msg.src, "init_ok", msg.body.msg_id, reply_msg_id, None);
                            eprintln!("✅ Node {} recovered. Applied Index: {}", my_node_id_str, applied_idx);
                        }
                    },
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
                                if let Some(c) = cmd { let _ = op.append(c); }
                            }
                        }
                    },
                    "paxos_net" => {
                        if let (Some(op), Some(data)) = (&mut omnipaxos, msg.body.paxos_data) {
                            if let Ok(p_msg) = bincode::deserialize(&data) { op.handle_incoming(p_msg); }
                        }
                    },
                    _ => {}
                }
            },

            Event::Tick => {
                if let Some(op) = &mut omnipaxos {
                    op.tick();
                    
                    for out_msg in op.outgoing_messages() {
                        let receiver_id = out_msg.get_receiver();
                        let dest = format!("n{}", receiver_id - 1); 
                        
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            let net_msg = Message {
                                src: my_node_id_str.clone(),
                                dest,
                                body: Body {
                                    msg_type: "paxos_net".to_string(),
                                    msg_id: None,
                                    in_reply_to: None,
                                    key: None,
                                    value: None,
                                    from: None,
                                    to: None,
                                    node_id: None,
                                    node_ids: None,
                                    paxos_data: Some(data),
                                    extra: serde_json::Map::new(),
                                },
                            };
                            println!("{}", serde_json::to_string(&net_msg).unwrap());
                        }
                    }

                    let d_idx = op.get_decided_idx();
                    
                    if d_idx > applied_idx {
                        // 🔥 核心修复 3：实时读取时也加装防弹护盾
                        let read_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                            op.read_decided_suffix(applied_idx)
                        }));

                        match read_result {
                            Ok(Some(entries)) => {
                                for entry in entries {
                                    if let LogEntry::Decided(cmd) = entry {
                                        match cmd {
                                            KVCommand::Write { key, value, msg_id, client } => {
                                                kv_store.insert(key, value);
                                                reply_msg_id += 1;
                                                send_reply(&my_node_id_str, &client, "write_ok", Some(msg_id), reply_msg_id, None);
                                            },
                                            KVCommand::Read { key, msg_id, client } => {
                                                // let val = kv_store.get(&key).copied();
                                                // reply_msg_id += 1;
                                                // send_reply(&my_node_id_str, &client, "read_ok", Some(msg_id), reply_msg_id, val);
                                                reply_msg_id += 1; // 统一递增，确保每个回复都有独立的 msg_id

                                                if let Some(&val) = kv_store.get(&key) {
                                                    // 场景 1：如果这个 key 之前被写入过，正常返回 value
                                                    send_reply(&my_node_id_str, &client, "read_ok", Some(msg_id), reply_msg_id, Some(val));
                                                } else {
                                                    // 场景 2：如果这个 key 压根不存在，遵循 Maelstrom 协议返回 20 号错误
                                                    // （注意：如果你在辅助函数里也给 send_error 加上了 reply_msg_id 参数，请把它传进去；如果没有加，保持下面这样即可）
                                                    send_error(&my_node_id_str, &client, msg_id, 20, "Key does not exist");
                                                }
                                            },
                                            KVCommand::Cas { key, from, to, msg_id, client } => {
                                                let current = kv_store.get(&key).copied();
                                                if current == Some(from) {
                                                    kv_store.insert(key, to);
                                                    reply_msg_id += 1;
                                                    send_reply(&my_node_id_str, &client, "cas_ok", Some(msg_id), reply_msg_id, None);
                                                } else {
                                                    send_error(&my_node_id_str, &client, msg_id, 22, "CAS mismatch");
                                                }
                                            }
                                        }
                                        applied_idx += 1;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(_) => {
                                // 如果触发了 ErrHelper，我们只记录日志，节点继续存活，等下个 Tick 再试
                                eprintln!("⚠️ Caught internal storage ErrHelper panic. Retrying next tick...");
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
fn send_reply(src: &str, dest: &str, msg_type: &str, in_reply_to: Option<u64>, msg_id: u64, value: Option<u64>) {
    let msg = Message {
        src: src.to_string(), dest: dest.to_string(),
        body: Body {
            msg_type: msg_type.to_string(), msg_id: Some(msg_id), in_reply_to,
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