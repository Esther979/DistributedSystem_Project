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
// 3. KV Snapshot（独立 sled，路径 kv_snap_N/，完全不受 OmniPaxos 影响）
//
// 每条 decide 时原子写入：
//   key.to_be_bytes() -> value.to_be_bytes()
//   b"__idx__"        -> applied_idx.to_be_bytes()
// ==========================================
const IDX_KEY: &[u8] = b"__idx__";

fn snap_path(pid: NodeId) -> String {
    // 独立目录，完全不在 storage_node_N/ 下面，避免被 paxos 清理逻辑误删
    format!("kv_snap_{}", pid)
}

fn open_snap(pid: NodeId) -> sled::Db {
    sled::Config::default()
        .path(snap_path(pid))
        .flush_every_ms(Some(200))
        .open()
        .expect("failed to open kv_snap db")
}

fn load_snap(db: &sled::Db) -> (HashMap<u64, u64>, u64) {
    let mut kv  = HashMap::new();
    let mut idx = 0u64;
    for item in db.iter() {
        if let Ok((k, v)) = item {
            if k.as_ref() == IDX_KEY {
                idx = u64::from_be_bytes(v.as_ref().try_into().unwrap_or([0u8; 8]));
            } else if k.len() == 8 {
                let key = u64::from_be_bytes(k.as_ref().try_into().unwrap());
                let val = u64::from_be_bytes(v.as_ref().try_into().unwrap_or([0u8; 8]));
                kv.insert(key, val);
            }
        }
    }
    (kv, idx)
}

/// 把一条已 decide 的命令原子写入 sled，同时更新内存 kv
fn snap_apply(db: &sled::Db, cmd: &KVCommand, new_idx: u64, kv: &mut HashMap<u64, u64>) {
    let mut batch = sled::Batch::default();
    match cmd {
        KVCommand::Write { key, value, .. } => {
            kv.insert(*key, *value);
            batch.insert(key.to_be_bytes().as_ref(), value.to_be_bytes().as_ref());
        }
        KVCommand::Cas { key, from, to, .. } => {
            if kv.get(key).copied() == Some(*from) {
                kv.insert(*key, *to);
                batch.insert(key.to_be_bytes().as_ref(), to.to_be_bytes().as_ref());
            }
        }
        KVCommand::Read { .. } => {}
    }
    batch.insert(IDX_KEY, new_idx.to_be_bytes().as_ref());
    let _ = db.apply_batch(batch);
}

// ==========================================
// 4. 构建 OmniPaxos 实例
//    keep=true  → 尝试用旧 paxos storage 恢复 np/na/va
//    keep=false → 清空 paxos storage，用空白重建（kv_snap 不动）
// ==========================================
fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
    keep: bool,
) -> OmniPaxos<KVCommand, PersistentStorage<KVCommand>> {
    let base = format!("storage_node_{}", my_pid);
    let logs = format!("{}/logs", base);

    let make = |base: &str, logs: &str| {
        let _ = std::fs::create_dir_all(logs);
        let commitlog_opts = commitlog::LogOptions::new(logs);
        let sled_opts = sled::Config::default().path(base);
        let cfg = PersistentStorageConfig::with(base.to_string(), commitlog_opts, sled_opts);
        PersistentStorage::open(cfg)
    };

    let mut cfg = OmniPaxosConfig::default();
    // election_tick_timeout: ticks before starting an election after losing leader heartbeat.
    // Default is 5. With 5ms ticks → 25ms election trigger. Keep it at 5 for fast recovery.
    // resend_message_tick_timeout: how often to resend un-acked messages.
    // Lowering to 5 ticks (25ms) helps re-sync faster after a partition heals.
    cfg.server_config.election_tick_timeout = 5;
    cfg.server_config.resend_message_tick_timeout = 5;
    cfg.server_config.pid               = my_pid;
    cfg.cluster_config.nodes            = all_pids.clone();
    cfg.cluster_config.configuration_id = 1;

    if !keep {
        let _ = std::fs::remove_dir_all(&base);
        let op = cfg.build(make(&base, &logs)).expect("build fresh failed");
        eprintln!("🆕 OmniPaxos: fresh paxos state");
        return op;
    }

    // keep=true: 先尝试保留旧 storage
    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
        cfg.clone().build(make(&base, &logs)).expect("build kept failed")
    }));

    match result {
        Ok(op) => {
            eprintln!("✅ OmniPaxos: restored paxos state (decided_idx={})", op.get_decided_idx());
            op
        }
        Err(_) => {
            eprintln!("⚠️  OmniPaxos storage corrupt on open, clearing paxos state (kv_snap kept)...");
            let _ = std::fs::remove_dir_all(&base);
            let op = cfg.build(make(&base, &logs)).expect("rebuild after corrupt failed");
            eprintln!("🆕 OmniPaxos: rebuilt fresh paxos state");
            op
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
        thread::sleep(Duration::from_millis(5));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId  = 0;
    let mut all_pids_cache: Vec<NodeId> = Vec::new();

    let mut kv_store:    HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;
    // commitlog_broken: true のとき Tick の read_decided_suffix を呼ばない
    let mut commitlog_broken: bool = false;

    let mut reply_msg_id: u64 = 0;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;
    let mut snap_db:    Option<sled::Db> = None;

    let mut handle_incoming_panic_count: u32 = 0;
    const PANIC_REBUILD_THRESHOLD: u32 = 3;

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

                            let marker = format!("kv_snap_{}/.initialized", my_pid);
                            let is_restart = std::path::Path::new(&marker).exists();

                            eprintln!(
                                "{} Node {} {}",
                                if is_restart { "🔄" } else { "🆕" },
                                my_node_id_str,
                                if is_restart { "crash-restart" } else { "fresh start" }
                            );

                            // ── 1. KV snapshot DB（独立目录，永远保留）
                            let db = open_snap(my_pid);
                            if is_restart {
                                let (snap_kv, snap_idx) = load_snap(&db);
                                kv_store    = snap_kv;
                                applied_idx = snap_idx;
                                eprintln!(
                                    "📂 KV snapshot: {} keys, applied_idx={}",
                                    kv_store.len(), applied_idx
                                );
                            } else {
                                let _ = db.clear();
                                let _ = db.flush();
                                kv_store.clear();
                                applied_idx = 0;
                            }
                            snap_db = Some(db);

                            // ── 2. OmniPaxos（paxos storage 可能损坏，但 kv_snap 不受影响）
                            let op = build_omnipaxos(
                                my_pid, all_pids_cache.clone(), is_restart,
                            );

                            // ── 3. 关键：判断 commitlog 是否可用
                            //    如果 applied_idx（从 kv_snap 恢复）>= op.get_decided_idx()，
                            //    说明 kv 状态已经是最新的，commitlog 不需要再读（或已损坏）。
                            //    如果 applied_idx < op.get_decided_idx()，说明 kv_snap 落后，
                            //    尝试用 read_decided_suffix 补齐。但如果 commitlog 损坏，
                            //    我们直接标记 commitlog_broken=true，让 OmniPaxos 从其他节点
                            //    同步新的 entries（通过正常 Paxos 协议）。
                            let paxos_decided = op.get_decided_idx();
                            if is_restart && applied_idx < paxos_decided {
                                eprintln!(
                                    "⚠️  kv_snap(applied={}) < paxos(decided={}): \
                                     trying to replay gap...", applied_idx, paxos_decided
                                );
                                // 尝试一次性补齐，如果 commitlog 损坏就放弃（不循环重试）
                                let replay = panic::catch_unwind(panic::AssertUnwindSafe(|| {
                                    op.read_decided_suffix(applied_idx)
                                }));
                                match replay {
                                    Ok(Some(entries)) => {
                                        if let Some(db) = &snap_db {
                                            for entry in entries {
                                                if let LogEntry::Decided(cmd) = entry {
                                                    let next = applied_idx + 1;
                                                    snap_apply(db, &cmd, next, &mut kv_store);
                                                    applied_idx = next;
                                                }
                                            }
                                        }
                                        let _ = snap_db.as_ref().map(|db| db.flush());
                                        eprintln!(
                                            "✅ Gap replayed: applied_idx now {}",
                                            applied_idx
                                        );
                                        commitlog_broken = false;
                                    }
                                    Ok(None) => {
                                        eprintln!(
                                            "📭 read_decided_suffix returned None: \
                                             will re-sync from cluster"
                                        );
                                        // applied_idx 已经是 kv_snap 里的，不需要动
                                        // 设 commitlog_broken=true 避免 Tick 里反复 panic
                                        commitlog_broken = true;
                                    }
                                    Err(_) => {
                                        eprintln!(
                                            "⚠️  commitlog corrupt (ErrHelper): \
                                             kv_snap is the best we have. \
                                             Will re-sync new entries from cluster."
                                        );
                                        commitlog_broken = true;
                                    }
                                }
                            } else {
                                // kv_snap 已是最新，或全新节点
                                commitlog_broken = false;
                            }

                            handle_incoming_panic_count = 0;
                            omnipaxos = Some(op);

                            // ── 4. 写 marker（放在 kv_snap 目录下，和 kv_snap 同生命周期）
                            let _ = std::fs::create_dir_all(snap_path(my_pid));
                            let _ = std::fs::write(&marker, b"1");

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
                                        "⚠️  handle_incoming panic #{}/{}",
                                        handle_incoming_panic_count, PANIC_REBUILD_THRESHOLD
                                    );
                                    if handle_incoming_panic_count >= PANIC_REBUILD_THRESHOLD {
                                        eprintln!("🔄 Rebuilding paxos state fresh \
                                                   (kv_snap kept)...");
                                        drop(omnipaxos.take());
                                        let new_op = build_omnipaxos(
                                            my_pid, all_pids_cache.clone(), false,
                                        );
                                        // kv_snap 不动，applied_idx 保持
                                        handle_incoming_panic_count = 0;
                                        commitlog_broken = true; // 新建的 paxos 没有旧日志
                                        omnipaxos = Some(new_op);
                                        eprintln!(
                                            "✅ Paxos rebuilt fresh, kv_snap intact \
                                             (applied_idx={}).", applied_idx
                                        );
                                    }
                                } else if panicked == false {
                                    // 成功收到消息说明 commitlog 现在是新的（leader sync 了过来）
                                    // 如果之前标记为 broken，在有新 decided entry 之后会自然清除
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }

            Event::Tick => {
                // Step 1: if commitlog just broke, rebuild paxos storage NOW.
                // Done OUTSIDE if-let so we can replace omnipaxos cleanly.
                // After rebuild, applied_idx stays (from kv_snap), decided_idx
                // resets to 0, and the leader will re-sync new entries via Paxos.
                if commitlog_broken {
                    eprintln!(
                        "🔄 commitlog_broken — rebuilding paxos storage fresh \
                         (applied_idx={} preserved)...", applied_idx
                    );
                    drop(omnipaxos.take());
                    let new_op = build_omnipaxos(my_pid, all_pids_cache.clone(), false);
                    omnipaxos = Some(new_op);
                    commitlog_broken = false;
                    eprintln!("✅ Paxos rebuilt. Waiting for leader sync.");
                }

                if let Some(op) = &mut omnipaxos {
                    if panic::catch_unwind(
                        panic::AssertUnwindSafe(|| op.tick())
                    ).is_err() {
                        eprintln!("⚠️  panic in tick()");
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
                                src: my_node_id_str.clone(),
                                dest,
                                body: Body {
                                    msg_type: "paxos_net".to_string(),
                                    msg_id: None, in_reply_to: None,
                                    key: None, value: None,
                                    from: None, to: None,
                                    node_id: None, node_ids: None,
                                    paxos_data: Some(data),
                                    extra: serde_json::Map::new(),
                                },
                            };
                            println!("{}", serde_json::to_string(&net_msg).unwrap());
                        }
                    }

                    // Step 2: apply newly decided entries.
                    // After a paxos rebuild, decided_idx starts at 0 and grows
                    // as the leader syncs. We only read when d_idx > applied_idx
                    // so we never double-apply entries already in kv_snap.
                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        match panic::catch_unwind(
                            panic::AssertUnwindSafe(|| op.read_decided_suffix(applied_idx))
                        ) {
                            Ok(Some(entries)) => {
                                apply_entries(
                                    entries, &mut kv_store, &mut applied_idx,
                                    &mut reply_msg_id, &my_node_id_str, &snap_db,
                                );
                            }
                            Ok(None) => {}
                            Err(_) => {
                                eprintln!(
                                    "⚠️  read_decided_suffix panicked \
                                     (applied={}, decided={}). \
                                     Will rebuild paxos on next tick.",
                                    applied_idx, d_idx
                                );
                                // Set flag; rebuild happens at TOP of next Tick
                                // outside if-let so omnipaxos can be replaced.
                                commitlog_broken = true;
                            }
                        }
                    }
                }
            }

            Event::Shutdown => {
                if let Some(db) = &snap_db { let _ = db.flush(); }
                eprintln!("🛑 shutdown");
                break;
            }
        }
    }
}

// ==========================================
// 6. 批量 apply decided entries（共用逻辑）
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

            // 先写 sled（持久化），再改内存，最后发 reply
            if let Some(db) = snap_db {
                snap_apply(db, &cmd, next_idx, kv_store);
            } else {
                // fallback：仅改内存
                match &cmd {
                    KVCommand::Write { key, value, .. } => { kv_store.insert(*key, *value); }
                    KVCommand::Cas { key, from, to, .. } => {
                        if kv_store.get(key).copied() == Some(*from) {
                            kv_store.insert(*key, *to);
                        }
                    }
                    KVCommand::Read { .. } => {}
                }
            }
            *applied_idx = next_idx;

            *reply_msg_id += 1;
            match cmd {
                KVCommand::Write { msg_id, client, .. } => {
                    send_reply(my_node_id_str, &client, "write_ok",
                               Some(msg_id), *reply_msg_id, None);
                }
                KVCommand::Read { key, msg_id, client } => {
                    if let Some(&val) = kv_store.get(&key) {
                        send_reply(my_node_id_str, &client, "read_ok",
                                   Some(msg_id), *reply_msg_id, Some(val));
                    } else {
                        send_error(my_node_id_str, &client,
                                   msg_id, *reply_msg_id, 20, "key does not exist");
                    }
                }
                KVCommand::Cas { key, from, to, msg_id, client } => {
                    // snap_apply 已经改了 kv_store；判断是否成功看 kv[key] == to
                    if kv_store.get(&key).copied() == Some(to) {
                        send_reply(my_node_id_str, &client, "cas_ok",
                                   Some(msg_id), *reply_msg_id, None);
                    } else {
                        let _ = (from, to);
                        send_error(my_node_id_str, &client,
                                   msg_id, *reply_msg_id, 22, "CAS mismatch");
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
            msg_type: msg_type.to_string(),
            msg_id: Some(msg_id),
            in_reply_to,
            key: None, value,
            from: None, to: None,
            node_id: None, node_ids: None, paxos_data: None,
            extra: serde_json::Map::new(),
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_error(src: &str, dest: &str, in_reply_to: u64, msg_id: u64, code: u32, text: &str) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    let msg = Message {
        src:  src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type: "error".to_string(),
            msg_id: Some(msg_id),
            in_reply_to: Some(in_reply_to),
            key: None, value: None,
            from: None, to: None,
            node_id: None, node_ids: None, paxos_data: None,
            extra,
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}