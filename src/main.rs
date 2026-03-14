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
    s.trim_start_matches('n')
     .parse::<u64>()
     .map(|id| id + 1)
     .unwrap_or(1)
}

// ==========================================
// 2. Maelstrom 协议结构体
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
    Shutdown, 
}

// ==========================================
// 3. 构建 OmniPaxos 实例
// ==========================================
fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
    keep_storage: bool,
) -> (OmniPaxos<KVCommand, PersistentStorage<KVCommand>>, bool) {
    let base_path = format!("storage_node_{}", my_pid);
    let log_path  = format!("{}/logs", base_path);

    if !keep_storage {
        let _ = std::fs::remove_dir_all(&base_path);
    }
    let _ = std::fs::create_dir_all(&log_path);

    let make_storage = || -> PersistentStorage<KVCommand> {
        let commitlog_opts = commitlog::LogOptions::new(&log_path);
        let sled_opts      = sled::Config::default().path(&base_path);
        let cfg = PersistentStorageConfig::with(
            base_path.clone(), commitlog_opts, sled_opts,
        );
        PersistentStorage::open(cfg)
    };

    let mut node_config = OmniPaxosConfig::default();
    node_config.server_config.pid               = my_pid;
    node_config.cluster_config.nodes            = all_pids.clone();
    node_config.cluster_config.configuration_id = 1;

    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        node_config.clone().build(make_storage()).expect("build failed")
    }));

    match result {
        Ok(op) => {
            let recovered = keep_storage && op.get_decided_idx() > 0;
            (op, recovered)
        }
        Err(_) => {
            eprintln!("⚠️ Storage corrupted on open, clearing and rebuilding fresh...");
            let _ = std::fs::remove_dir_all(&base_path);
            let _ = std::fs::create_dir_all(&log_path);
            let op = node_config.build(make_storage()).expect("rebuild after clear failed");
            (op, false)
        }
    }
}

// ==========================================
// 4. 主程序
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
        let _ = tx_in.send(Event::Shutdown);
    });

    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(10));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId  = 0;
    let mut all_pids_cache: Vec<NodeId> = Vec::new();
    let mut kv_store: HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut reply_msg_id: u64 = 0;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;

    let start_time = std::time::Instant::now();
    let mut simulated_crash_done = false;

    for event in rx {
        match event {
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {
                    "init" => {
                        if let (Some(nid), Some(raw_nodes)) =
                            (msg.body.node_id.clone(), msg.body.node_ids.clone())
                        {
                            my_node_id_str = nid;
                            my_pid = parse_node_id(&my_node_id_str);
                            all_pids_cache = raw_nodes.iter().map(|s| parse_node_id(s)).collect();
                            let marker_path = format!("storage_node_{}/.initialized", my_pid);
                            let is_crash_restart = std::path::Path::new(&marker_path).exists();

                            let (op, recovered) = build_omnipaxos(my_pid, all_pids_cache.clone(), is_crash_restart);
                            kv_store.clear();
                            applied_idx = 0;

                            if recovered {
                                if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(0))) {
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
                                }
                            }
                            omnipaxos = Some(op);
                            let _ = std::fs::create_dir_all(format!("storage_node_{}", my_pid));
                            let _ = std::fs::write(&marker_path, b"1");
                            reply_msg_id += 1;
                            send_reply(&my_node_id_str, &msg.src, "init_ok", msg.body.msg_id, reply_msg_id, None);
                        }
                    }

                    "read" | "write" | "cas" => {
                        if let Some(op) = &mut omnipaxos {
                            let client = msg.src.clone();
                            if let Some(m_id) = msg.body.msg_id {
                                let cmd = match msg.body.msg_type.as_str() {
                                    "read" => Some(KVCommand::Read { key: msg.body.key.unwrap_or(0), msg_id: m_id, client }),
                                    "write" => Some(KVCommand::Write { key: msg.body.key.unwrap_or(0), value: msg.body.value.unwrap_or(0), msg_id: m_id, client }),
                                    "cas" => Some(KVCommand::Cas { key: msg.body.key.unwrap_or(0), from: msg.body.from.unwrap_or(0), to: msg.body.to.unwrap_or(0), msg_id: m_id, client }),
                                    _ => None,
                                };
                                if let Some(c) = cmd { let _ = op.append(c); }
                            }
                        }
                    }

                    "paxos_net" => {
                        if let Some(data) = msg.body.paxos_data.clone() {
                            if let Ok(p_msg) = bincode::deserialize::<omnipaxos::messages::Message<KVCommand>>(&data) {
                                if let Some(op) = &mut omnipaxos {
                                    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| op.handle_incoming(p_msg)));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }

            Event::Tick => {
                // =========================================================
                // 💣 核心修正：强化版状态机热重启
                // =========================================================
                if !simulated_crash_done && start_time.elapsed().as_secs() > 15 && my_pid == 1 {
                    eprintln!("💥 [Node n0] 触发模拟断电！内存已清空...");
                    kv_store.clear();
                    applied_idx = 0;
                    
                    std::thread::sleep(std::time::Duration::from_secs(2));
                    
                    if let Some(op) = &mut omnipaxos {
                        let target_decided = op.get_decided_idx();
                        eprintln!("🔄 [Node n0] 重启中... 目标恢复 idx: {}", target_decided);
                        
                        // 🌟 循环读取，直到追上 target_decided
                        let mut current_pos = 0;
                        while applied_idx < target_decided {
                            if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(current_pos))) {
                                if entries.is_empty() { break; } // 无更多可读
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
                                current_pos = applied_idx;
                            } else { break; }
                        }
                        eprintln!("✅ [Node n0] 恢复完成！最终 applied_idx: {}", applied_idx);
                    }
                    simulated_crash_done = true;
                }

                if let Some(op) = &mut omnipaxos {
                    let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| op.tick()));
                    let out_msgs = op.outgoing_messages();
                    for out_msg in out_msgs {
                        let dest = format!("n{}", out_msg.get_receiver() - 1);
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            let net_msg = Message {
                                src: my_node_id_str.clone(),
                                dest,
                                body: Body {
                                    msg_type: "paxos_net".to_string(),
                                    msg_id: None, in_reply_to: None, key: None, value: None, from: None, to: None, node_id: None, node_ids: None,
                                    paxos_data: Some(data), extra: serde_json::Map::new(),
                                },
                            };
                            println!("{}", serde_json::to_string(&net_msg).unwrap());
                        }
                    }

                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        if let Ok(Some(entries)) = panic::catch_unwind(panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))) {
                            for entry in entries {
                                if let LogEntry::Decided(cmd) = entry {
                                    match cmd {
                                        KVCommand::Write { key, value, msg_id, client } => {
                                            kv_store.insert(key, value);
                                            reply_msg_id += 1;
                                            send_reply(&my_node_id_str, &client, "write_ok", Some(msg_id), reply_msg_id, None);
                                        }
                                        KVCommand::Read { key, msg_id, client } => {
                                            reply_msg_id += 1;
                                            if let Some(&val) = kv_store.get(&key) {
                                                send_reply(&my_node_id_str, &client, "read_ok", Some(msg_id), reply_msg_id, Some(val));
                                            } else {
                                                send_error(&my_node_id_str, &client, msg_id, reply_msg_id, 20, "key does not exist");
                                            }
                                        }
                                        KVCommand::Cas { key, from, to, msg_id, client } => {
                                            reply_msg_id += 1;
                                            if kv_store.get(&key).copied() == Some(from) {
                                                kv_store.insert(key, to);
                                                send_reply(&my_node_id_str, &client, "cas_ok", Some(msg_id), reply_msg_id, None);
                                            } else {
                                                send_error(&my_node_id_str, &client, msg_id, reply_msg_id, 22, "CAS mismatch");
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

fn send_reply(src: &str, dest: &str, msg_type: &str, in_reply_to: Option<u64>, msg_id: u64, value: Option<u64>) {
    let msg = Message {
        src: src.to_string(), dest: dest.to_string(),
        body: Body {
            msg_type: msg_type.to_string(), msg_id: Some(msg_id), in_reply_to, key: None, value, from: None, to: None, node_id: None, node_ids: None, paxos_data: None, extra: serde_json::Map::new(),
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_error(src: &str, dest: &str, in_reply_to: u64, msg_id: u64, code: u32, text: &str) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    let msg = Message {
        src: src.to_string(), dest: dest.to_string(),
        body: Body {
            msg_type: "error".to_string(), msg_id: Some(msg_id), in_reply_to: Some(in_reply_to), key: None, value: None, from: None, to: None, node_id: None, node_ids: None, paxos_data: None, extra,
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}