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
}

// ==========================================
// 3. 构建 OmniPaxos 实例（带恢复逻辑）
// ==========================================
fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
    is_restart: bool,  // 是否是 crash 后重启
) -> (OmniPaxos<KVCommand, PersistentStorage<KVCommand>>, bool) {
    // is_recovered: true = 从持久化存储恢复，false = 全新启动
    let base_path = format!("storage_node_{}", my_pid);
    let log_path  = format!("{}/logs", base_path);

    // 🔥 FIX 3：区分"全新启动"和"crash 后重启"
    //   - 全新启动（is_restart=false）：清掉旧数据，防止跨测试污染
    //   - crash 重启（is_restart=true）：保留磁盘数据，尝试恢复
    if !is_restart {
        let _ = std::fs::remove_dir_all(&base_path);
    }
    let _ = std::fs::create_dir_all(&log_path);

    let make_storage = || {
        let commitlog_options = commitlog::LogOptions::new(&log_path);
        let sled_options      = sled::Config::default().path(&base_path);
        let storage_config    = PersistentStorageConfig::with(
            base_path.clone(), commitlog_options, sled_options,
        );
        PersistentStorage::open(storage_config)
    };

    let mut node_config = OmniPaxosConfig::default();
    node_config.server_config.pid            = my_pid;
    node_config.cluster_config.nodes         = all_pids.clone();
    node_config.cluster_config.configuration_id = 1;

    // 先尝试用现有存储 build，如果 panic 说明存储损坏，清掉重建
    let build_result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        let storage = make_storage();
        node_config.clone().build(storage).expect("Failed to build OmniPaxos")
    }));

    match build_result {
        Ok(op) => {
            let recovered = is_restart && op.get_decided_idx() > 0;
            (op, recovered)
        }
        Err(_) => {
            // 存储损坏，清掉重建
            eprintln!("⚠️ Storage corrupted, clearing and restarting fresh...");
            let _ = std::fs::remove_dir_all(&base_path);
            let _ = std::fs::create_dir_all(&log_path);
            let storage = make_storage();
            let op = node_config.build(storage).expect("Failed to build OmniPaxos after reset");
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
    });

    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(10));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId  = 0;
    let mut kv_store: HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut reply_msg_id: u64 = 0;
    let mut is_initialized = false;  // 区分首次 init 和 crash 重启
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;

    for event in rx {
        match event {

            // ======================================
            // 消息处理
            // ======================================
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {

                    "init" => {
                        if let (Some(nid), Some(raw_nodes)) =
                            (msg.body.node_id.clone(), msg.body.node_ids.clone())
                        {
                            my_node_id_str = nid;
                            my_pid = parse_node_id(&my_node_id_str);
                            let all_pids: Vec<NodeId> =
                                raw_nodes.iter().map(|s| parse_node_id(s)).collect();

                            // 🔥 FIX 3：is_initialized=true 表示 crash 后重启，保留存储
                            let is_restart = is_initialized;
                            let (op, recovered) =
                                build_omnipaxos(my_pid, all_pids, is_restart);

                            // 重置 kv_store 和 applied_idx，然后从持久化日志重放
                            kv_store.clear();
                            applied_idx = 0;

                            if recovered {
                                // 从磁盘恢复已 commit 的状态
                                let read_result = panic::catch_unwind(
                                    panic::AssertUnwindSafe(|| op.read_decided_suffix(0))
                                );
                                if let Ok(Some(entries)) = read_result {
                                    for entry in entries {
                                        if let LogEntry::Decided(cmd) = entry {
                                            match cmd {
                                                KVCommand::Write { key, value, .. } => {
                                                    kv_store.insert(key, value);
                                                }
                                                KVCommand::Cas { key, from, to, .. } => {
                                                    if kv_store.get(&key) == Some(&from) {
                                                        kv_store.insert(key, to);
                                                    }
                                                }
                                                _ => {}
                                            }
                                            applied_idx += 1;
                                        }
                                    }
                                }
                                eprintln!(
                                    "✅ Node {} RECOVERED from persistent storage. Applied Index: {}",
                                    my_node_id_str, applied_idx
                                );
                            } else {
                                eprintln!("🆕 Node {} started fresh.", my_node_id_str);
                            }

                            omnipaxos    = Some(op);
                            is_initialized = true;
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
                                    "read" => msg.body.key.map(|k| {
                                        KVCommand::Read { key: k, msg_id: m_id, client }
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
                        if let (Some(op), Some(data)) =
                            (&mut omnipaxos, msg.body.paxos_data)
                        {
                            if let Ok(p_msg) = bincode::deserialize(&data) {
                                op.handle_incoming(p_msg);
                            }
                        }
                    }

                    _ => {}
                }
            }

            // ======================================
            // Tick：驱动 OmniPaxos 心跳 + apply 日志
            // ======================================
            Event::Tick => {
                if let Some(op) = &mut omnipaxos {

                    // 🔥 FIX 1：tick() 也包进 catch_unwind
                    //    leader 选举时 send_accsync 会读 log suffix，可能 panic
                    let tick_ok = panic::catch_unwind(
                        panic::AssertUnwindSafe(|| op.tick())
                    ).is_ok();

                    if !tick_ok {
                        eprintln!("⚠️ Caught panic in tick(), skipping this tick...");
                        continue;
                    }

                    // 🔥 FIX 2：outgoing_messages() 也包起来
                    let out_msgs: Vec<omnipaxos::messages::Message<KVCommand>> = panic::catch_unwind(
                        panic::AssertUnwindSafe(|| op.outgoing_messages().into_iter().collect::<Vec<_>>())
                    ).unwrap_or_default();

                    for out_msg in out_msgs {
                        let receiver_id = out_msg.get_receiver();
                        let dest = format!("n{}", receiver_id - 1);
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            let net_msg = Message {
                                src:  my_node_id_str.clone(),
                                dest,
                                body: Body {
                                    msg_type:   "paxos_net".to_string(),
                                    msg_id:     None,
                                    in_reply_to: None,
                                    key:        None,
                                    value:      None,
                                    from:       None,
                                    to:         None,
                                    node_id:    None,
                                    node_ids:   None,
                                    paxos_data: Some(data),
                                    extra:      serde_json::Map::new(),
                                },
                            };
                            println!("{}", serde_json::to_string(&net_msg).unwrap());
                        }
                    }

                    // Apply 已 commit 的日志
                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        let read_result = panic::catch_unwind(
                            panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))
                        );

                        match read_result {
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
                                                if let Some(&val) = kv_store.get(&key) {
                                                    send_reply(
                                                        &my_node_id_str, &client,
                                                        "read_ok", Some(msg_id), reply_msg_id, Some(val),
                                                    );
                                                } else {
                                                    // key 不存在：返回 error code 20
                                                    send_error(
                                                        &my_node_id_str, &client,
                                                        msg_id, reply_msg_id, 20, "key does not exist",
                                                    );
                                                }
                                            }
                                            KVCommand::Cas { key, from, to, msg_id, client } => {
                                                let current = kv_store.get(&key).copied();
                                                if current == Some(from) {
                                                    kv_store.insert(key, to);
                                                    reply_msg_id += 1;
                                                    send_reply(
                                                        &my_node_id_str, &client,
                                                        "cas_ok", Some(msg_id), reply_msg_id, None,
                                                    );
                                                } else {
                                                    reply_msg_id += 1;
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
                                eprintln!("⚠️ Caught storage ErrHelper in apply loop. Retrying next tick...");
                            }
                        }
                    }
                }
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
        src: src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type: msg_type.to_string(),
            msg_id: Some(msg_id),   // 永远是整数，不是 null
            in_reply_to,
            key: None, value, from: None, to: None,
            node_id: None, node_ids: None, paxos_data: None,
            extra: serde_json::Map::new(),
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

// 🔥 FIX 4：send_error 也带上 msg_id，符合 Maelstrom 协议
fn send_error(
    src: &str, dest: &str,
    in_reply_to: u64, msg_id: u64, code: u32, text: &str,
) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    let msg = Message {
        src: src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type: "error".to_string(),
            msg_id: Some(msg_id),   // 🔥 之前是 None，现在修复
            in_reply_to: Some(in_reply_to),
            key: None, value: None, from: None, to: None,
            node_id: None, node_ids: None, paxos_data: None,
            extra,
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}
