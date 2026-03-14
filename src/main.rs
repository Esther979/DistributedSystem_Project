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
// 3. KV Snapshot 持久化
//
// 存储路径: kv_snap_N/  (完全独立于 OmniPaxos storage)
// 每次 apply 一条 decided entry 时原子写入:
//   key(u64 BE) -> value(u64 BE)
//   b"__idx__"  -> applied_idx(u64 BE)
//
// 这是节点重启后恢复 KV 状态的唯一数据源。
// OmniPaxos commitlog 在进程被 SIGKILL 后可能损坏，
// 因此我们不依赖它恢复 KV 状态。
// ==========================================
const IDX_KEY: &[u8] = b"__idx__";

fn snap_path(pid: NodeId) -> String {
    format!("kv_snap_{}", pid)
}

fn open_snap(pid: NodeId) -> sled::Db {
    sled::Config::default()
        .path(snap_path(pid))
        .flush_every_ms(Some(100))  // 100ms 后台 flush，崩溃最多丢 ~100ms 数据
        .open()
        .expect("failed to open kv_snap db")
}

fn load_snap(db: &sled::Db) -> (HashMap<u64, u64>, u64) {
    let mut kv  = HashMap::new();
    let mut idx = 0u64;
    for item in db.iter() {
        if let Ok((k, v)) = item {
            if k.as_ref() == IDX_KEY {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0u8; 8]);
                idx = u64::from_be_bytes(bytes);
            } else if k.len() == 8 {
                let key_b: [u8; 8] = k.as_ref().try_into().unwrap();
                let val_b: [u8; 8] = v.as_ref().try_into().unwrap_or([0u8; 8]);
                kv.insert(u64::from_be_bytes(key_b), u64::from_be_bytes(val_b));
            }
        }
    }
    (kv, idx)
}

/// 原子写入一条 decided 命令到 sled，同时更新内存 kv。
/// 返回值：对 CAS 命令返回 true=成功 / false=from 不匹配；其他命令返回 true。
fn snap_apply(db: &sled::Db, cmd: &KVCommand, new_idx: u64, kv: &mut HashMap<u64, u64>) -> bool {
    let mut batch = sled::Batch::default();
    let mut cas_ok = true; // Write / Read 总是"成功"
    match cmd {
        KVCommand::Write { key, value, .. } => {
            kv.insert(*key, *value);
            batch.insert(key.to_be_bytes().as_ref(), value.to_be_bytes().as_ref());
        }
        KVCommand::Cas { key, from, to, .. } => {
            if kv.get(key).copied() == Some(*from) {
                kv.insert(*key, *to);
                batch.insert(key.to_be_bytes().as_ref(), to.to_be_bytes().as_ref());
                cas_ok = true;
            } else {
                // from 不匹配：CAS 失败，kv 不变，applied_idx 仍推进
                cas_ok = false;
            }
        }
        KVCommand::Read { .. } => {}
    }
    // 总是更新 applied_idx
    batch.insert(IDX_KEY, new_idx.to_be_bytes().as_ref());
    let _ = db.apply_batch(batch);
    cas_ok
}

// ==========================================
// 4. 构建 OmniPaxos 实例
//
// 策略：
//   - 总是使用空白的 paxos storage（不保留旧 commitlog）
//   - 旧 commitlog 在进程被 SIGKILL 后几乎必然损坏
//   - KV 状态通过 kv_snap 独立持久化，不需要 commitlog 来恢复
//   - 节点加入集群后，leader 会通过 AcceptSync 消息补齐缺少的 log entries
// ==========================================
fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
) -> OmniPaxos<KVCommand, PersistentStorage<KVCommand>> {
    let base = format!("storage_node_{}", my_pid);
    let logs = format!("{}/logs", base);

    // 每次都清空旧的 paxos storage，避免损坏的 commitlog 导致 ErrHelper
    let _ = std::fs::remove_dir_all(&base);
    let _ = std::fs::create_dir_all(&logs);

    let commitlog_opts = commitlog::LogOptions::new(&logs);
    let sled_opts = sled::Config::default().path(&base);
    let storage_cfg = PersistentStorageConfig::with(
        base.clone(), commitlog_opts, sled_opts,
    );
    let storage = PersistentStorage::open(storage_cfg);

    let mut cfg = OmniPaxosConfig::default();
    cfg.server_config.pid               = my_pid;
    cfg.cluster_config.nodes            = all_pids;
    cfg.cluster_config.configuration_id = 1;
    // 5ms tick × 5 = 25ms election timeout，partition 恢复后快速选主
    cfg.server_config.election_tick_timeout     = 5;
    cfg.server_config.resend_message_tick_timeout = 5;

    cfg.build(storage).expect("OmniPaxos build failed")
}

// ==========================================
// 5. 主程序
// ==========================================
fn main() {
    let (tx, rx) = mpsc::channel();

    // stdin 读取线程
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

    // Tick 线程：5ms 一次，给 OmniPaxos 驱动 election / heartbeat
    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(5));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    let mut my_node_id_str  = String::new();
    let mut my_pid: NodeId  = 0;
    let mut all_pids_cache: Vec<NodeId> = Vec::new();

    // KV 内存状态（由 kv_snap 持久化）
    let mut kv_store:    HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut reply_msg_id: u64 = 0;

    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;
    let mut snap_db:   Option<sled::Db> = None;

    for event in rx {
        match event {

            // ── 消息处理
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {

                    // ── init：节点启动 / 重启
                    "init" => {
                        if let (Some(nid), Some(raw_nodes)) =
                            (msg.body.node_id.clone(), msg.body.node_ids.clone())
                        {
                            my_node_id_str = nid;
                            my_pid = parse_node_id(&my_node_id_str);
                            all_pids_cache = raw_nodes.iter()
                                .map(|s| parse_node_id(s))
                                .collect();

                            // marker 文件存在 = crash-restart，需要从 kv_snap 恢复
                            let marker = format!("kv_snap_{}/.initialized", my_pid);
                            let is_restart = std::path::Path::new(&marker).exists();

                            eprintln!(
                                "{} Node {} — {}",
                                if is_restart { "🔄" } else { "🆕" },
                                my_node_id_str,
                                if is_restart {
                                    "crash-restart, loading kv_snap..."
                                } else {
                                    "fresh start"
                                }
                            );

                            // 步骤 1: 打开 kv_snap（始终保留，不受 paxos 影响）
                            let db = open_snap(my_pid);

                            if is_restart {
                                // 从 kv_snap 恢复 KV 状态
                                let (snap_kv, snap_idx) = load_snap(&db);
                                kv_store    = snap_kv;
                                applied_idx = snap_idx;
                                eprintln!(
                                    "📂 kv_snap loaded: {} keys, applied_idx={}",
                                    kv_store.len(), applied_idx
                                );
                                // OmniPaxos 会通过 AcceptSync 把 applied_idx 之后的
                                // entries 发过来，我们在 Tick 里 apply 它们
                            } else {
                                // 全新节点，清空 kv_snap
                                let _ = db.clear();
                                let _ = db.flush();
                                kv_store.clear();
                                applied_idx = 0;
                            }
                            snap_db = Some(db);

                            // 步骤 2: 构建 OmniPaxos（总是用空白 storage）
                            // 空白 paxos state 意味着节点以 follower 身份加入，
                            // leader 会发 AcceptSync 补齐缺少的 log entries
                            let op = build_omnipaxos(my_pid, all_pids_cache.clone());
                            omnipaxos = Some(op);

                            eprintln!(
                                "✅ OmniPaxos started (fresh paxos state). \
                                 Will sync from leader if needed."
                            );

                            // 写 marker，下次识别为 crash-restart
                            let _ = std::fs::create_dir_all(snap_path(my_pid));
                            let _ = std::fs::write(&marker, b"1");

                            reply_msg_id += 1;
                            send_reply(
                                &my_node_id_str, &msg.src,
                                "init_ok", msg.body.msg_id, reply_msg_id, None,
                            );
                        }
                    }

                    // ── 客户端请求：通过 Paxos 共识处理
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
                                                key: k, from: f, to: t,
                                                msg_id: m_id, client,
                                            })
                                        } else { None }
                                    }
                                    _ => None,
                                };
                                if let Some(c) = cmd {
                                    let _ = op.append(c);
                                }
                            }
                        }
                    }

                    // ── Paxos 内部消息转发
                    "paxos_net" => {
                        if let Some(data) = msg.body.paxos_data.clone() {
                            if let Ok(p_msg) = bincode::deserialize::<
                                omnipaxos::messages::Message<KVCommand>
                            >(&data) {
                                if let Some(op) = &mut omnipaxos {
                                    // handle_incoming 可能因旧数据 panic，安全包装
                                    if panic::catch_unwind(panic::AssertUnwindSafe(|| {
                                        op.handle_incoming(p_msg);
                                    })).is_err() {
                                        eprintln!("⚠️  handle_incoming panicked, ignoring message");
                                    }
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }

            // ── Tick：驱动 OmniPaxos（election、heartbeat、outgoing messages、apply）
            Event::Tick => {
                if let Some(op) = &mut omnipaxos {
                    // 驱动 OmniPaxos 内部时钟
                    if panic::catch_unwind(panic::AssertUnwindSafe(|| op.tick())).is_err() {
                        eprintln!("⚠️  tick() panicked");
                    }

                    // 发送 OmniPaxos 产生的出站 Paxos 消息
                    let out_msgs = panic::catch_unwind(
                        panic::AssertUnwindSafe(|| {
                            op.outgoing_messages().into_iter().collect::<Vec<_>>()
                        })
                    ).unwrap_or_default();

                    for out_msg in out_msgs {
                        let dest = format!("n{}", out_msg.get_receiver() - 1);
                        if let Ok(data) = bincode::serialize(&out_msg) {
                            let net_msg = Message {
                                src: my_node_id_str.clone(),
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

                    // Apply 新 decided 条目
                    // 
                    // 关键不变式：
                    //   applied_idx = kv_snap 里持久化的最后一条 entry 的序号
                    //   d_idx       = OmniPaxos 已决出的最新 entry 序号
                    //
                    // 正常运行时：d_idx == applied_idx（每条 decide 立即 apply）
                    // 重启后初始：d_idx == 0（空白 paxos），applied_idx 来自 kv_snap
                    //   → d_idx <= applied_idx，不 apply（避免重复）
                    //   → leader sync 后 d_idx 增长超过 applied_idx 才 apply 新条目
                    //
                    // 注意：read_decided_suffix(applied_idx) 只返回 applied_idx 之后的条目，
                    //        所以即使 d_idx 暂时小于 applied_idx 也不会出问题（不会读到已 apply 的条目）
                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        match panic::catch_unwind(
                            panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))
                        ) {
                            Ok(Some(entries)) => {
                                apply_entries(
                                    entries,
                                    &mut kv_store,
                                    &mut applied_idx,
                                    &mut reply_msg_id,
                                    &my_node_id_str,
                                    &snap_db,
                                );
                            }
                            Ok(None) => {
                                // OmniPaxos 还没有从 leader 收到足够数据，下次 tick 再试
                            }
                            Err(_) => {
                                // commitlog 读取失败（理论上不应发生，因为我们总是用空白 storage）
                                // 如果发生，记录日志，跳过这次读取
                                eprintln!(
                                    "⚠️  read_decided_suffix panicked unexpectedly \
                                     (applied={}, decided={}). Skipping.",
                                    applied_idx, d_idx
                                );
                            }
                        }
                    }
                }
            }

            Event::Shutdown => {
                // 关闭前强制 flush，减少最后一批数据丢失
                if let Some(db) = &snap_db {
                    let _ = db.flush();
                }
                eprintln!("🛑 shutdown");
                break;
            }
        }
    }
}

// ==========================================
// 6. 批量 apply decided entries
// ==========================================
fn apply_entries(
    entries: Vec<LogEntry<KVCommand>>,
    kv_store: &mut HashMap<u64, u64>,
    applied_idx: &mut u64,
    reply_msg_id: &mut u64,
    my_node_id_str: &str,
    snap_db: &Option<sled::Db>,
) {
    for entry in entries {
        if let LogEntry::Decided(cmd) = entry {
            let next_idx = *applied_idx + 1;

            // 先持久化到 sled（原子操作），再改内存，最后发 reply
            // snap_apply 返回 CAS 是否成功（Write/Read 总返回 true）
            let cmd_ok = if let Some(db) = snap_db {
                snap_apply(db, &cmd, next_idx, kv_store)
            } else {
                // fallback（不应发生）：仅改内存
                match &cmd {
                    KVCommand::Write { key, value, .. } => {
                        kv_store.insert(*key, *value);
                        true
                    }
                    KVCommand::Cas { key, from, to, .. } => {
                        if kv_store.get(key).copied() == Some(*from) {
                            kv_store.insert(*key, *to);
                            true
                        } else {
                            false
                        }
                    }
                    KVCommand::Read { .. } => true,
                }
            };
            *applied_idx = next_idx;

            // 发送客户端 reply
            *reply_msg_id += 1;
            match cmd {
                KVCommand::Write { msg_id, client, .. } => {
                    send_reply(
                        my_node_id_str, &client,
                        "write_ok", Some(msg_id), *reply_msg_id, None,
                    );
                }
                KVCommand::Read { key, msg_id, client } => {
                    if let Some(&val) = kv_store.get(&key) {
                        send_reply(
                            my_node_id_str, &client,
                            "read_ok", Some(msg_id), *reply_msg_id, Some(val),
                        );
                    } else {
                        send_error(
                            my_node_id_str, &client,
                            msg_id, *reply_msg_id, 20, "key does not exist",
                        );
                    }
                }
                KVCommand::Cas { key, from, to, msg_id, client } => {
                    // cmd_ok = snap_apply 的返回值，直接反映 from 是否匹配
                    // 避免用 kv_store[key] == to 判断（当 from!=to 且恰好 current==to 时会误判）
                    let _ = (key, from, to);
                    if cmd_ok {
                        send_reply(
                            my_node_id_str, &client,
                            "cas_ok", Some(msg_id), *reply_msg_id, None,
                        );
                    } else {
                        send_error(
                            my_node_id_str, &client,
                            msg_id, *reply_msg_id, 22, "CAS mismatch",
                        );
                    }
                }
            }
        }
    }
}

// ==========================================
// 7. 辅助发送函数
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
            key:         None,
            value,
            from:        None,
            to:          None,
            node_id:     None,
            node_ids:    None,
            paxos_data:  None,
            extra:       serde_json::Map::new(),
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
            key:         None,
            value:       None,
            from:        None,
            to:          None,
            node_id:     None,
            node_ids:    None,
            paxos_data:  None,
            extra,
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}