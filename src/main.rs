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
// 3. KV Snapshot 持久化（独立于 OmniPaxos commitlog）
//
// 核心设计：用一个与 OmniPaxos 完全独立的 sled::Tree（路径 kv_snap/）来持久化：
//   - 每个 KV key  -> value（u64 big-endian）
//   - 特殊 key "__applied_idx__" -> applied_idx（u64 big-endian）
//
// 好处：即使 OmniPaxos commitlog 在进程被 SIGKILL 时损坏，
// KV 状态本身仍然完整可读，崩溃重启时直接加载，不需要依赖 read_decided_suffix。
// ==========================================

const APPLIED_IDX_KEY: &[u8] = b"__applied_idx__";

fn open_kv_db(my_pid: NodeId) -> sled::Db {
    let path = format!("storage_node_{}/kv_snap", my_pid);
    sled::Config::default()
        .path(&path)
        .flush_every_ms(Some(50))   // 每 50ms 后台 flush，缩小崩溃丢失窗口
        .open()
        .expect("failed to open kv snapshot db")
}

/// 从 sled 加载 KV 快照和 applied_idx
fn load_kv_snapshot(db: &sled::Db) -> (HashMap<u64, u64>, u64) {
    let mut kv = HashMap::new();
    let mut applied_idx: u64 = 0;

    for item in db.iter() {
        if let Ok((k, v)) = item {
            if k.as_ref() == APPLIED_IDX_KEY {
                let bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0u8; 8]);
                applied_idx = u64::from_be_bytes(bytes);
            } else if k.len() == 8 {
                let key_bytes: [u8; 8] = k.as_ref().try_into().unwrap();
                let val_bytes: [u8; 8] = v.as_ref().try_into().unwrap_or([0u8; 8]);
                kv.insert(u64::from_be_bytes(key_bytes), u64::from_be_bytes(val_bytes));
            }
        }
    }
    (kv, applied_idx)
}

/// 将一条已 decide 的命令原子写入 sled，同时更新内存 kv_store
fn persist_apply(
    db: &sled::Db,
    cmd: &KVCommand,
    new_applied_idx: u64,
    kv: &mut HashMap<u64, u64>,
) {
    let mut batch = sled::Batch::default();

    match cmd {
        KVCommand::Write { key, value, .. } => {
            kv.insert(*key, *value);
            batch.insert(key.to_be_bytes().to_vec(), value.to_be_bytes().to_vec());
        }
        KVCommand::Cas { key, from, to, .. } => {
            if kv.get(key).copied() == Some(*from) {
                kv.insert(*key, *to);
                batch.insert(key.to_be_bytes().to_vec(), to.to_be_bytes().to_vec());
            }
            // from 不匹配时不修改，也不写 sled
        }
        KVCommand::Read { .. } => {
            // Read 是 no-op，只推进 applied_idx
        }
    }

    // 总是更新 applied_idx（即使是 Read 或 CAS 未命中）
    batch.insert(
        APPLIED_IDX_KEY.to_vec(),
        new_applied_idx.to_be_bytes().to_vec(),
    );
    let _ = db.apply_batch(batch);
    // 注意：不在热路径调用 db.flush()，依赖 flush_every_ms 后台刷盘。
    // 如需更强持久性（允许更高延迟），可改为：let _ = db.flush();
}

// ==========================================
// 4. 构建 OmniPaxos 实例
// ==========================================
fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
    keep_paxos_storage: bool,
) -> OmniPaxos<KVCommand, PersistentStorage<KVCommand>> {
    let base_path = format!("storage_node_{}", my_pid);
    let log_path  = format!("{}/logs", base_path);

    if !keep_paxos_storage {
        // 只清 OmniPaxos 的 commitlog + sled 元数据，保留 kv_snap/
        let _ = std::fs::remove_dir_all(&log_path);
        for entry in std::fs::read_dir(&base_path).into_iter().flatten().flatten() {
            let p = entry.path();
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name != "kv_snap" && name != ".initialized" {
                if p.is_dir() {
                    let _ = std::fs::remove_dir_all(&p);
                } else {
                    let _ = std::fs::remove_file(&p);
                }
            }
        }
    }
    let _ = std::fs::create_dir_all(&log_path);

    let make_storage = || -> PersistentStorage<KVCommand> {
        let commitlog_opts = commitlog::LogOptions::new(&log_path);
        let sled_opts = sled::Config::default().path(&base_path);
        let cfg = PersistentStorageConfig::with(
            base_path.clone(), commitlog_opts, sled_opts,
        );
        PersistentStorage::open(cfg)
    };

    let mut node_config = OmniPaxosConfig::default();
    node_config.server_config.pid               = my_pid;
    node_config.cluster_config.nodes            = all_pids.clone();
    node_config.cluster_config.configuration_id = 1;

    // 先尝试带存储打开（恢复 np/na/va，帮助更快重选 leader）
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        node_config.clone().build(make_storage()).expect("build failed")
    }));

    match result {
        Ok(op) => {
            eprintln!(
                "✅ OmniPaxos opened (decided_idx={}, keep_paxos={})",
                op.get_decided_idx(), keep_paxos_storage
            );
            op
        }
        Err(_) => {
            // commitlog 损坏：清空 paxos 状态，用空存储重建
            // kv_snap 不受影响，KV 状态由上层从 snapshot 恢复
            eprintln!("⚠️  OmniPaxos storage corrupt; rebuilding paxos state from scratch \
                       (kv_snap kept)...");
            let _ = std::fs::remove_dir_all(&log_path);
            let _ = std::fs::create_dir_all(&log_path);
            // 同样清掉 sled 里 paxos 的 metadata
            for entry in std::fs::read_dir(&base_path).into_iter().flatten().flatten() {
                let p = entry.path();
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "kv_snap" && name != ".initialized" {
                    if p.is_dir() {
                        let _ = std::fs::remove_dir_all(&p);
                    } else {
                        let _ = std::fs::remove_file(&p);
                    }
                }
            }
            let _ = std::fs::create_dir_all(&log_path);
            node_config.build(make_storage()).expect("rebuild failed")
        }
    }
}

// ==========================================
// 5. 主程序
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

    let mut kv_store:    HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;
    let mut reply_msg_id: u64 = 0;

    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;
    let mut kv_db:     Option<sled::Db> = None;

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

                            let _ = std::fs::create_dir_all(
                                format!("storage_node_{}", my_pid)
                            );
                            let marker_path = format!(
                                "storage_node_{}/.initialized", my_pid
                            );
                            let is_crash_restart =
                                std::path::Path::new(&marker_path).exists();

                            eprintln!(
                                "{} Node {} {}",
                                if is_crash_restart { "🔄" } else { "🆕" },
                                my_node_id_str,
                                if is_crash_restart {
                                    "restarting — loading KV snapshot."
                                } else {
                                    "started fresh."
                                }
                            );

                            // ── 1. 打开 KV snapshot DB（始终保留）
                            let db = open_kv_db(my_pid);

                            if is_crash_restart {
                                // ── 直接从 sled kv_snap 加载 KV 状态
                                let (snap_kv, snap_idx) = load_kv_snapshot(&db);
                                kv_store    = snap_kv;
                                applied_idx = snap_idx;
                                eprintln!(
                                    "📂 KV snapshot loaded: {} keys, applied_idx={}",
                                    kv_store.len(), applied_idx
                                );
                            } else {
                                // ── 全新节点：清空 snapshot
                                let _ = db.clear();
                                let _ = db.flush();
                                kv_store.clear();
                                applied_idx = 0;
                            }
                            kv_db = Some(db);

                            // ── 2. 构建 OmniPaxos
                            //    keep_paxos_storage=true 时尝试恢复 np/na/va，
                            //    即使 commitlog 损坏也只重建 paxos 状态，不动 kv_snap
                            let op = build_omnipaxos(
                                my_pid, all_pids_cache.clone(), is_crash_restart,
                            );

                            handle_incoming_panic_count = 0;
                            omnipaxos = Some(op);

                            // ── 3. 写 marker
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
                                                key: k, from: f, to: t,
                                                msg_id: m_id, client,
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
                                    eprintln!(
                                        "⚠️ handle_incoming panic #{}/{}",
                                        handle_incoming_panic_count, PANIC_REBUILD_THRESHOLD
                                    );

                                    if handle_incoming_panic_count >= PANIC_REBUILD_THRESHOLD {
                                        eprintln!(
                                            "🔄 Commitlog corrupted; rebuilding paxos \
                                             state (kv_snap kept)..."
                                        );
                                        drop(omnipaxos.take());
                                        let new_op = build_omnipaxos(
                                            my_pid, all_pids_cache.clone(), false,
                                        );
                                        // 从 kv_snap 重新加载 KV（不丢数据）
                                        if let Some(db) = &kv_db {
                                            let (snap_kv, snap_idx) = load_kv_snapshot(db);
                                            kv_store    = snap_kv;
                                            applied_idx = snap_idx;
                                        }
                                        handle_incoming_panic_count = 0;
                                        omnipaxos = Some(new_op);
                                        eprintln!(
                                            "✅ Paxos rebuilt; kv_snap restored \
                                             (applied_idx={}).", applied_idx
                                        );
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
                        eprintln!("⚠️ panic in tick(), continuing...");
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

                    // ── Apply 新 decided 条目 + 持久化到 kv_snap
                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        match panic::catch_unwind(
                            panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))
                        ) {
                            Ok(Some(entries)) => {
                                for entry in entries {
                                    if let LogEntry::Decided(cmd) = entry {
                                        let next_idx = applied_idx + 1;

                                        // persist_apply 先写 sled，再改内存
                                        if let Some(db) = &kv_db {
                                            persist_apply(db, &cmd, next_idx, &mut kv_store);
                                        } else {
                                            // 无 DB fallback（不应发生）
                                            match &cmd {
                                                KVCommand::Write { key, value, .. } => {
                                                    kv_store.insert(*key, *value);
                                                }
                                                KVCommand::Cas { key, from, to, .. } => {
                                                    if kv_store.get(key).copied() == Some(*from) {
                                                        kv_store.insert(*key, *to);
                                                    }
                                                }
                                                KVCommand::Read { .. } => {}
                                            }
                                        }
                                        applied_idx = next_idx;

                                        // 发送回复
                                        reply_msg_id += 1;
                                        match cmd {
                                            KVCommand::Write { msg_id, client, .. } => {
                                                send_reply(
                                                    &my_node_id_str, &client,
                                                    "write_ok", Some(msg_id),
                                                    reply_msg_id, None,
                                                );
                                            }
                                            KVCommand::Read { key, msg_id, client } => {
                                                if let Some(&val) = kv_store.get(&key) {
                                                    send_reply(
                                                        &my_node_id_str, &client,
                                                        "read_ok", Some(msg_id),
                                                        reply_msg_id, Some(val),
                                                    );
                                                } else {
                                                    send_error(
                                                        &my_node_id_str, &client,
                                                        msg_id, reply_msg_id,
                                                        20, "key does not exist",
                                                    );
                                                }
                                            }
                                            KVCommand::Cas { key, from, to, msg_id, client } => {
                                                // persist_apply 已更新 kv_store；
                                                // 判断是否成功：若 kv[key] == to 则成功
                                                if kv_store.get(&key).copied() == Some(to) {
                                                    send_reply(
                                                        &my_node_id_str, &client,
                                                        "cas_ok", Some(msg_id),
                                                        reply_msg_id, None,
                                                    );
                                                } else {
                                                    // CAS 未命中（from 不匹配）
                                                    let _ = (from, to); // 已 move，suppress lint
                                                    send_error(
                                                        &my_node_id_str, &client,
                                                        msg_id, reply_msg_id,
                                                        22, "CAS mismatch",
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(None) => {}
                            Err(_) => {
                                eprintln!(
                                    "⚠️ panic in read_decided_suffix, retrying next tick..."
                                );
                            }
                        }
                    }
                }
            }

            Event::Shutdown => {
                if let Some(db) = &kv_db {
                    let _ = db.flush();
                }
                eprintln!("🛑 stdin closed, shutting down (exit 0)...");
                break;
            }
        }
    }
}

// ==========================================
// 6. 辅助发送函数
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