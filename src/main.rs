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
    // 必须加上 skip_serializing_if，否则返回的 JSON 会带有 null 字段，导致 Maelstrom 协议报错
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

    let mut handle_incoming_panic_count: u32 = 0;
    const PANIC_REBUILD_THRESHOLD: u32 = 2; 

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
                            all_pids_cache = raw_nodes.iter()
                                .map(|s| parse_node_id(s))
                                .collect();

                            let marker_path = format!("storage_node_{}/.initialized", my_pid);
                            let is_crash_restart = std::path::Path::new(&marker_path).exists();

                            eprintln!(
                                "{} Node {} {}",
                                if is_crash_restart { "🔄" } else { "🆕" },
                                my_node_id_str,
                                if is_crash_restart { "restarting, keeping storage." } else { "started fresh." }
                            );

                            let (op, recovered) =
                                build_omnipaxos(my_pid, all_pids_cache.clone(), is_crash_restart);

                            kv_store.clear();
                            applied_idx = 0;
                            handle_incoming_panic_count = 0;

                            if recovered {
                                eprintln!("📂 Recovering state from persistent log (decided_idx={})...",
                                    op.get_decided_idx());
                                let replay = panic::catch_unwind(
                                    panic::AssertUnwindSafe(|| op.read_decided_suffix(0))
                                );
                                match replay {
                                    Ok(Some(entries)) => {
                                        for entry in entries {
                                            if let LogEntry::Decided(cmd) = entry {
                                                match cmd {
                                                    KVCommand::Write { key, value, .. } => {
                                                        kv_store.insert(key, value);
                                                    }
                                                    KVCommand::Cas { key, from, to, .. } => {
                                                        if kv_store.get(&key).copied() == Some(from) {
                                                            kv_store.insert(key, to);
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                        applied_idx = op.get_decided_idx();
                                        eprintln!("✅ Recovery done: {} keys, applied_idx={}",
                                            kv_store.len(), applied_idx);
                                        omnipaxos = Some(op);
                                    }
                                    Ok(None) => {
                                        applied_idx = 0;
                                        omnipaxos = Some(op);
                                        eprintln!("✅ Recovery: empty log, starting fresh.");
                                    }
                                    Err(_) => {
                                        eprintln!("⚠️ Recovery replay panicked, falling back to fresh start...");
                                        drop(op); 
                                        let _ = std::fs::remove_dir_all(format!("storage_node_{}", my_pid));
                                        let _ = std::fs::create_dir_all(format!("storage_node_{}/logs", my_pid));
                                        let (fresh_op, _) = build_omnipaxos(my_pid, all_pids_cache.clone(), false);
                                        kv_store.clear();
                                        applied_idx = 0;
                                        omnipaxos = Some(fresh_op);
                                        eprintln!("✅ Fresh start after replay failure, will re-sync from cluster.");
                                    }
                                }
                            } else {
                                omnipaxos = Some(op);
                            }

                            let _ = std::fs::create_dir_all(format!("storage_node_{}", my_pid));
                            let _ = std::fs::write(&marker_path, b"1");

                            reply_msg_id += 1;
                            send_reply(
                                &my_node_id_str, &msg.src,
                                "init_ok", msg.body.msg_id, reply_msg_id, None,
                            );
                        }
                    }

                    "read" | "write" | "cas" => {
                        if let Some(op) = &mut omnipaxos {
                            let client = msg.src.clone();
                            if let Some(m_id) = msg.body.msg_id {
                                let cmd = match msg.body.msg_type.as_str() {
                                    "read" => msg.body.key.map(|k| KVCommand::Read {
                                        key: k, msg_id: m_id, client,
                                    }),
                                    "write" => msg.body.key.and_then(|k| {
                                        msg.body.value.map(|v| KVCommand::Write {
                                            key: k, value: v, msg_id: m_id, client,
                                        })
                                    }),
                                    "cas" => {
                                        if let (Some(k), Some(f), Some(t)) =
                                            (msg.body.key, msg.body.from, msg.body.to)
                                        {
                                            Some(KVCommand::Cas {
                                                key: k, from: f, to: t, msg_id: m_id, client,
                                            })
                                        } else { None }
                                    }
                                    _ => None,
                                };
                                if let Some(c) = cmd { let _ = op.append(c); }
                            }
                        }
                    }

                    "paxos_net" => {
                        if let Some(data) = msg.body.paxos_data.clone() {
                            if let Ok(p_msg) = bincode::deserialize::<
                                omnipaxos::messages::Message<KVCommand>
                            >(&data) {
                                let panicked = if let Some(op) = &mut omnipaxos {
                                    panic::catch_unwind(panic::AssertUnwindSafe(|| {
                                        op.handle_incoming(p_msg);
                                    })).is_err()
                                } else { false };

                                if panicked {
                                    handle_incoming_panic_count += 1;
                                    eprintln!("⚠️ handle_incoming panic #{}/{}",
                                        handle_incoming_panic_count, PANIC_REBUILD_THRESHOLD);

                                    if handle_incoming_panic_count >= PANIC_REBUILD_THRESHOLD {
                                        eprintln!("🔄 Instance corrupted (commitlog ErrHelper), rebuilding fresh...");
                                        drop(omnipaxos.take());  
                                        let (new_op, _) = build_omnipaxos(
                                            my_pid, all_pids_cache.clone(), false,
                                        );
                                        kv_store.clear();
                                        applied_idx = 0;
                                        handle_incoming_panic_count = 0;
                                        omnipaxos = Some(new_op);
                                        eprintln!("✅ Instance rebuilt fresh (will re-sync from cluster).");
                                    }
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }

            Event::Tick => {
                if let Some(op) = &mut omnipaxos {
                    if panic::catch_unwind(
                        panic::AssertUnwindSafe(|| op.tick())
                    ).is_err() {
                        eprintln!("⚠️ panic in tick(), continuing to process messages...");
                    }

                    let out_msgs: Vec<omnipaxos::messages::Message<KVCommand>> =
                        panic::catch_unwind(
                            panic::AssertUnwindSafe(|| {
                                op.outgoing_messages().into_iter().collect::<Vec<_>>()
                            })
                        ).unwrap_or_default();

                    for out_msg in out_msgs {
                        let dest = format!("n{}", out_msg.get_receiver() - 1);
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            let net_msg = Message {
                                src:  my_node_id_str.clone(),
                                dest,
                                body: Body {
                                    msg_type:    "paxos_net".to_string(),
                                    msg_id:      None,
                                    in_reply_to: None,
                                    key:         None,
                                    value:       None,
                                    from:        None,
                                    to:          None,
                                    node_id:     None,
                                    node_ids:    None,
                                    paxos_data:  Some(data),
                                    extra:       serde_json::Map::new(),
                                },
                            };
                            println!("{}", serde_json::to_string(&net_msg).unwrap());
                        }
                    }

                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        match panic::catch_unwind(
                            panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))
                        ) {
                            Ok(Some(entries)) => {
                                for entry in entries {
                                    if let LogEntry::Decided(cmd) = entry {
                                        match cmd {
                                            KVCommand::Write { key, value, msg_id, client } => {
                                                kv_store.insert(key, value);
                                                reply_msg_id += 1;
                                                send_reply(
                                                    &my_node_id_str, &client,
                                                    "write_ok", Some(msg_id), reply_msg_id, None,
                                                );
                                            }
                                            KVCommand::Read { key, msg_id, client } => {
                                                reply_msg_id += 1;
                                                // 完美修复：确保找不到 Key 时返回标准协议错误
                                                if let Some(&val) = kv_store.get(&key) {
                                                    send_reply(
                                                        &my_node_id_str, &client,
                                                        "read_ok", Some(msg_id), reply_msg_id, Some(val),
                                                    );
                                                } else {
                                                    send_error(
                                                        &my_node_id_str, &client,
                                                        msg_id, reply_msg_id, 20, "key does not exist",
                                                    );
                                                }
                                            }
                                            KVCommand::Cas { key, from, to, msg_id, client } => {
                                                reply_msg_id += 1;
                                                if kv_store.get(&key).copied() == Some(from) {
                                                    kv_store.insert(key, to);
                                                    send_reply(
                                                        &my_node_id_str, &client,
                                                        "cas_ok", Some(msg_id), reply_msg_id, None,
                                                    );
                                                } else {
                                                    send_error(
                                                        &my_node_id_str, &client,
                                                        msg_id, reply_msg_id, 22, "CAS mismatch",
                                                    );
                                                }
                                            }
                                        }
                                        applied_idx += 1;
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(_) => {
                                eprintln!("⚠️ panic in read_decided_suffix, retrying next tick...");
                            }
                        }
                    }
                }
            }

            Event::Shutdown => {
                eprintln!("🛑 stdin closed, shutting down cleanly (exit 0)...");
                break;
            }
        } 
    } 
}

// ==========================================
// 5. 辅助发送函数
// ==========================================
fn send_reply(
    src: &str, dest: &str, msg_type: &str,
    in_reply_to: Option<u64>, msg_id: u64, value: Option<u64>,
) {
    let msg = Message {
        src:  src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type:    msg_type.to_string(),
            msg_id:      Some(msg_id),
            in_reply_to,
            key: None, value,
            from: None, to: None,
            node_id: None, node_ids: None, paxos_data: None,
            extra: serde_json::Map::new(),
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_error(
    src: &str, dest: &str,
    in_reply_to: u64, msg_id: u64, code: u32, text: &str,
) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    let msg = Message {
        src:  src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type:    "error".to_string(),
            msg_id:      Some(msg_id),
            in_reply_to: Some(in_reply_to),
            key: None, value: None,
            from: None, to: None,
            node_id: None, node_ids: None, paxos_data: None,
            extra,
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}