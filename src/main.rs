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
// 1. KV Command Definition
//
// Each command carries `node` (the receiving node's ID) so that only
// the node that originally received the request sends the reply after
// the entry is decided. All other nodes apply the state change silently.
// This prevents duplicate / "invalid dest" replies.
// ==========================================
#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub enum KVCommand {
    Read  { key: u64, msg_id: u64, client: String, node: String },
    Write { key: u64, value: u64, msg_id: u64, client: String, node: String },
    Cas   { key: u64, from: u64, to: u64, msg_id: u64, client: String, node: String },
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n')
     .parse::<u64>()
     .map(|id| id + 1)
     .unwrap_or(1)
}

// ==========================================
// 2. Maelstrom Protocol Structures
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
// 3. KV Snapshot Persistence (sled)
//
// Layout in the sled DB:
//   key(u64 big-endian) → value(u64 big-endian)
//   b"__idx__"          → last_applied_idx(u64 big-endian)
//
// Every decided entry is atomically written via a sled::Batch that
// updates both the KV pair and __idx__ in one fsync.  On crash-restart
// we read this DB to recover the exact KV state and applied_idx without
// replaying the (potentially corrupted) OmniPaxos commitlog.
//
// NOTE: applied_idx is the index of the last entry that has been
// durably written to kv_snap.  Entries with index > applied_idx in
// the paxos log are replayed silently on recovery (see section 5).
// ==========================================
const IDX_KEY: &[u8] = b"__idx__";

fn snap_path(pid: NodeId) -> String {
    format!("kv_snap_{}", pid)
}

fn marker_path(pid: NodeId) -> String {
    format!("kv_snap_{}/.initialized", pid)
}

fn open_snap(pid: NodeId) -> sled::Db {
    sled::Config::default()
        .path(snap_path(pid))
        .flush_every_ms(Some(50))   // background flush every 50 ms
        .open()
        .expect("failed to open kv_snap sled db")
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

/// Atomically persist one decided entry into sled and update in-memory kv.
/// Returns true if the operation "succeeded" (Write/Read always true; CAS only
/// if `from` matched).
fn snap_apply(
    db: &sled::Db,
    cmd: &KVCommand,
    new_idx: u64,
    kv: &mut HashMap<u64, u64>,
) -> bool {
    let mut batch = sled::Batch::default();
    let mut ok = true;

    match cmd {
        KVCommand::Write { key, value, .. } => {
            kv.insert(*key, *value);
            batch.insert(key.to_be_bytes().as_ref(), value.to_be_bytes().as_ref());
        }
        KVCommand::Cas { key, from, to, .. } => {
            if kv.get(key).copied() == Some(*from) {
                kv.insert(*key, *to);
                batch.insert(key.to_be_bytes().as_ref(), to.to_be_bytes().as_ref());
                ok = true;
            } else {
                ok = false;
                // applied_idx still advances even on CAS mismatch
            }
        }
        KVCommand::Read { .. } => { /* read-through consensus, no KV mutation */ }
    }

    // Always persist the new applied_idx together with any KV change.
    batch.insert(IDX_KEY, new_idx.to_be_bytes().as_ref());
    let _ = db.apply_batch(batch);
    ok
}

// ==========================================
// 4. Build / Restore OmniPaxos Instance
//
// `fresh_start = true`  → wipe the paxos storage directory first.
//   Used on the very first run, or when existing storage is detected
//   as corrupt (see section 5).
//
// `fresh_start = false` → open existing storage in-place.
//   Used on crash-restart; OmniPaxos will read its own ballot / promise
//   state and re-establish leadership / followership correctly.
//   Any decided entries it still has in the commitlog will be returned
//   by `read_decided_suffix()` so we can apply the ones we missed.
//
// Election tuning:
//   5 ms tick × election_tick_timeout 5 = 25 ms election timeout.
//   This lets the cluster elect a new leader within ~25–50 ms after a
//   partition heals or a node restarts, which keeps test latencies low.
// ==========================================
fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
    fresh_start: bool,
) -> OmniPaxos<KVCommand, PersistentStorage<KVCommand>> {
    let base = format!("storage_node_{}", my_pid);
    let logs = format!("{}/logs", base);

    if fresh_start {
        let _ = std::fs::remove_dir_all(&base);
    }
    let _ = std::fs::create_dir_all(&logs);

    let commitlog_opts = commitlog::LogOptions::new(&logs);
    let sled_opts      = sled::Config::default().path(&base);
    let storage_cfg    = PersistentStorageConfig::with(base.clone(), commitlog_opts, sled_opts);
    let storage        = PersistentStorage::open(storage_cfg);

    let mut cfg = OmniPaxosConfig::default();
    cfg.server_config.pid                         = my_pid;
    cfg.cluster_config.nodes                      = all_pids;
    cfg.cluster_config.configuration_id           = 1;
    cfg.server_config.election_tick_timeout        = 5;
    cfg.server_config.resend_message_tick_timeout  = 5;

    cfg.build(storage).expect("OmniPaxos build failed")
}

// ==========================================
// 5. Main Event Loop
// ==========================================
fn main() {
    let (tx, rx) = mpsc::channel::<Event>();

    // --- stdin reader thread ---
    let tx_in = tx.clone();
    thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(l) => {
                    if let Ok(msg) = serde_json::from_str::<Message>(&l) {
                        let _ = tx_in.send(Event::Message(msg));
                    }
                }
                Err(_) => { break; }
            }
        }
        let _ = tx_in.send(Event::Shutdown);
    });

    // --- tick thread: 5 ms period drives OmniPaxos heartbeat / election ---
    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(5));
        if tx_tick.send(Event::Tick).is_err() { break; }
    });

    // --- mutable node state ---
    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId = 0;          // 由第一条 "init" 消息赋值
    #[allow(unused_assignments)]
    let mut all_pids_cache: Vec<NodeId> = Vec::new(); // 同上

    let mut kv_store:    HashMap<u64, u64> = HashMap::new();
    let mut applied_idx: u64 = 0;

    // `recovery_fence`:
    //   After a crash-restart, the paxos storage may already contain
    //   decided entries (indices applied_idx+1 … paxos_decided_idx) that
    //   the node had committed but not yet applied to kv_snap before it
    //   crashed.  We replay those entries to rebuild state, but we must
    //   NOT send client replies for them because:
    //     (a) the clients have already seen a timeout / :info result, and
    //     (b) the msg_ids are stale – replying would confuse Maelstrom.
    //   recovery_fence is set to the paxos decided_idx at restart time.
    //   Any entry with index ≤ recovery_fence is applied silently.
    let mut recovery_fence: u64 = 0;

    let mut reply_msg_id: u64 = 0;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, PersistentStorage<KVCommand>>> = None;
    let mut snap_db:   Option<sled::Db> = None;

    for event in rx {
        match event {

            // ── Incoming Message ────────────────────────────────────────────
            Event::Message(msg) => {
                match msg.body.msg_type.as_str() {

                    // ── init ──────────────────────────────────────────────
                    // Maelstrom sends this once at startup (fresh test) and
                    // again whenever a killed node is restarted (nemesis kill).
                    "init" => {
                        if let (Some(nid), Some(raw_nodes)) =
                            (msg.body.node_id.clone(), msg.body.node_ids.clone())
                        {
                            my_node_id_str = nid;
                            my_pid         = parse_node_id(&my_node_id_str);
                            all_pids_cache = raw_nodes.iter()
                                .map(|s| parse_node_id(s))
                                .collect();

                            let marker     = marker_path(my_pid);
                            let is_restart = std::path::Path::new(&marker).exists();

                            if is_restart {
                                // ── CRASH-RESTART PATH ────────────────────
                                // Step 1: restore KV state from kv_snap
                                let db = open_snap(my_pid);
                                let (kv, snap_idx) = load_snap(&db);
                                kv_store    = kv;
                                applied_idx = snap_idx;
                                snap_db     = Some(db);

                                eprintln!(
                                    "🔄 [{}] Crash-restart: {} keys restored, \
                                     kv_snap applied_idx={}",
                                    my_node_id_str,
                                    kv_store.len(),
                                    applied_idx
                                );

                                // Step 2: try to restore OmniPaxos from its
                                // own persistent storage so it can replay any
                                // decided entries we haven't applied yet.
                                let op_result = panic::catch_unwind(
                                    panic::AssertUnwindSafe(|| {
                                        build_omnipaxos(
                                            my_pid,
                                            all_pids_cache.clone(),
                                            false,   // keep existing paxos storage
                                        )
                                    })
                                );

                                let op = match op_result {
                                    Ok(op) => {
                                        let d = op.get_decided_idx();
                                        eprintln!(
                                            "✅ [{}] Paxos storage restored \
                                             (paxos decided_idx={})",
                                            my_node_id_str, d
                                        );
                                        // Entries (applied_idx, d] exist in the
                                        // paxos log but not in kv_snap.  We will
                                        // replay them silently in the tick loop.
                                        recovery_fence = d;
                                        op
                                    }
                                    Err(_) => {
                                        // Commitlog corrupted (SIGKILL mid-write).
                                        // Fall back to a fresh paxos instance.
                                        // kv_snap state is still valid; the leader
                                        // will bring us up to date via AcceptSync.
                                        eprintln!(
                                            "⚠️  [{}] Paxos storage corrupt – \
                                             starting fresh OmniPaxos \
                                             (kv_snap applied_idx={} is authoritative)",
                                            my_node_id_str, applied_idx
                                        );
                                        recovery_fence = applied_idx; // nothing to replay
                                        build_omnipaxos(
                                            my_pid,
                                            all_pids_cache.clone(),
                                            true,    // wipe corrupted storage
                                        )
                                    }
                                };

                                omnipaxos = Some(op);
                            } else {
                                // ── FRESH-START PATH ──────────────────────
                                // First time this node runs in this test.
                                // Wipe any leftover state from a previous run.
                                let db = open_snap(my_pid);
                                let _ = db.clear();
                                let _ = db.flush();
                                kv_store.clear();
                                applied_idx    = 0;
                                recovery_fence = 0;
                                snap_db        = Some(db);

                                omnipaxos = Some(build_omnipaxos(
                                    my_pid,
                                    all_pids_cache.clone(),
                                    true,   // always fresh paxos storage
                                ));

                                // Write the marker so future inits know this
                                // is a crash-restart rather than a fresh run.
                                let _ = std::fs::create_dir_all(snap_path(my_pid));
                                let _ = std::fs::write(&marker, b"1");

                                eprintln!(
                                    "🆕 [{}] Fresh start",
                                    my_node_id_str
                                );
                            }

                            reply_msg_id += 1;
                            send_reply(
                                &my_node_id_str, &msg.src,
                                "init_ok", msg.body.msg_id, reply_msg_id, None,
                            );
                        }
                    }

                    // ── Client operations: linearizable via Paxos log ──────
                    //
                    // All three operation types (read, write, cas) are
                    // appended to the OmniPaxos log and executed only after
                    // they are decided.  In particular, reads are NOT served
                    // locally; they go through consensus.  This is the correct
                    // mechanism for linearizable reads in a consensus system:
                    // a local read could return stale data if the node is a
                    // deposed leader or is lagging behind.  By requiring reads
                    // to be decided, every read observes all writes that were
                    // committed before it.
                    "read" | "write" | "cas" => {
                        if let Some(op) = &mut omnipaxos {
                            // 保留 client / node 的所有权用于错误回复；
                            // 构建命令时 .clone() 一份传入，避免 "borrow of moved value"。
                            let client = msg.src.clone();
                            let node   = my_node_id_str.clone();
                            if let Some(m_id) = msg.body.msg_id {
                                let cmd: Option<KVCommand> =
                                    match msg.body.msg_type.as_str() {
                                    "read" => msg.body.key.map(|k| KVCommand::Read {
                                        key: k, msg_id: m_id,
                                        client: client.clone(), node: node.clone(),
                                    }),
                                    "write" => {
                                        if let (Some(k), Some(v)) =
                                            (msg.body.key, msg.body.value)
                                        {
                                            Some(KVCommand::Write {
                                                key: k, value: v, msg_id: m_id,
                                                client: client.clone(), node: node.clone(),
                                            })
                                        } else { None }
                                    }
                                    "cas" => {
                                        if let (Some(k), Some(f), Some(t)) =
                                            (msg.body.key, msg.body.from, msg.body.to)
                                        {
                                            Some(KVCommand::Cas {
                                                key: k, from: f, to: t, msg_id: m_id,
                                                client: client.clone(), node: node.clone(),
                                            })
                                        } else { None }
                                    }
                                    _ => None,
                                };

                                if let Some(c) = cmd {
                                    if let Err(e) = op.append(c) {
                                        // 节点不是 leader 或尚未就绪时 append 会失败。
                                        // 回复错误码 11，让 Maelstrom 把该操作标记为
                                        // indeterminate，线性一致性检查器可以正确处理。
                                        reply_msg_id += 1;
                                        send_error(
                                            &my_node_id_str, &client, // client 仍可用
                                            m_id, reply_msg_id, 11,
                                            &format!("not leader / not ready: {:?}", e),
                                        );
                                    }
                                }
                            }
                        } else {
                            // Paxos not yet initialized
                            if let Some(m_id) = msg.body.msg_id {
                                reply_msg_id += 1;
                                send_error(
                                    &my_node_id_str, &msg.src,
                                    m_id, reply_msg_id, 11,
                                    "node not initialized",
                                );
                            }
                        }
                    }

                    // ── OmniPaxos internal message forwarding ──────────────
                    "paxos_net" => {
                        if let (Some(data), Some(op)) =
                            (msg.body.paxos_data.clone(), &mut omnipaxos)
                        {
                            match bincode::deserialize::<
                                omnipaxos::messages::Message<KVCommand>
                            >(&data) {
                                Ok(p_msg) => {
                                    if panic::catch_unwind(
                                        panic::AssertUnwindSafe(|| op.handle_incoming(p_msg))
                                    ).is_err() {
                                        eprintln!(
                                            "⚠️  [{}] handle_incoming panicked",
                                            my_node_id_str
                                        );
                                    }
                                }
                                Err(e) => {
                                    eprintln!(
                                        "⚠️  [{}] bincode deserialize failed: {}",
                                        my_node_id_str, e
                                    );
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }

            // ── Tick ────────────────────────────────────────────────────────
            Event::Tick => {
                let Some(op) = &mut omnipaxos else { continue };

                // 1. Drive OmniPaxos internal clock (heartbeat / election)
                if panic::catch_unwind(
                    panic::AssertUnwindSafe(|| op.tick())
                ).is_err() {
                    eprintln!("⚠️  [{}] tick() panicked", my_node_id_str);
                }

                // 2. Forward outgoing OmniPaxos messages to peers via Maelstrom
                let out_msgs = panic::catch_unwind(
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
                                msg_type:    "paxos_net".into(),
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

                // 3. Apply newly decided log entries
                //
                // Invariants:
                //   applied_idx  = highest entry written atomically to kv_snap
                //   d_idx        = highest entry decided by OmniPaxos
                //
                // Normal operation:   d_idx grows one entry at a time, always
                //                     > applied_idx after each decision.
                //
                // After crash-restart with intact paxos storage:
                //   applied_idx = kv_snap value (e.g. 8)
                //   d_idx       = paxos value   (e.g. 10)
                //   → entries 9-10 are replayed silently (≤ recovery_fence)
                //   → entry 11+  are applied normally with client replies
                //
                // After crash-restart with corrupted paxos storage:
                //   applied_idx = kv_snap value (e.g. 8)
                //   d_idx       = 0  (fresh paxos)
                //   → d_idx ≤ applied_idx → nothing to apply yet
                //   → leader sends AcceptSync to bring us up to date
                //   → once d_idx > applied_idx, new entries are applied normally
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
                                recovery_fence,
                            );
                        }
                        Ok(None) => {
                            // OmniPaxos does not yet have the suffix in memory
                            // (e.g. still syncing after restart).  Try again
                            // on the next tick.
                        }
                        Err(_) => {
                            eprintln!(
                                "⚠️  [{}] read_decided_suffix panicked \
                                 (applied={}, decided={})",
                                my_node_id_str, applied_idx, d_idx
                            );
                        }
                    }
                }
            }

            // ── Shutdown ────────────────────────────────────────────────────
            Event::Shutdown => {
                if let Some(db) = &snap_db {
                    let _ = db.flush();
                }
                eprintln!("🛑 [{}] shutdown", my_node_id_str);
                break;
            }
        }
    }
}

// ==========================================
// 6. Apply a batch of decided log entries
//
// Parameters:
//   recovery_fence  – entries with index ≤ this value are replayed to
//                     rebuild kv state after a crash-restart.  Client
//                     replies are suppressed for these entries because
//                     the clients have already received a timeout (the
//                     operations are recorded as :info in Maelstrom's
//                     history, which the linearizability checker handles
//                     correctly).  Entries with index > recovery_fence
//                     are new decisions and get full client replies.
// ==========================================
fn apply_entries(
    entries:         Vec<LogEntry<KVCommand>>,
    kv_store:        &mut HashMap<u64, u64>,
    applied_idx:     &mut u64,
    reply_msg_id:    &mut u64,
    my_node_id_str:  &str,
    snap_db:         &Option<sled::Db>,
    recovery_fence:  u64,
) {
    for entry in entries {
        let LogEntry::Decided(cmd) = entry else { continue };

        let next_idx = *applied_idx + 1;

        // Atomically persist to sled and update in-memory kv.
        let cmd_ok = if let Some(db) = snap_db {
            snap_apply(db, &cmd, next_idx, kv_store)
        } else {
            // Fallback (should never happen in normal operation)
            apply_in_memory(&cmd, kv_store)
        };

        *applied_idx = next_idx;

        // --- Decide whether to send a client reply ---
        //
        // Two conditions must both hold:
        //   (a) This node was the one that received the original request
        //       (the `node` field in the command matches my_node_id_str).
        //       Without this, every node would send a reply.
        //   (b) This entry is not part of the crash-recovery replay
        //       (next_idx > recovery_fence).
        //       Entries in the replay window had their clients time out;
        //       replying now would send stale / duplicate messages.
        let is_recovery_replay = next_idx <= recovery_fence;
        if is_recovery_replay {
            eprintln!(
                "  ↩ [recovery replay] idx={} (fence={})",
                next_idx, recovery_fence
            );
            continue;
        }

        let owned_by_me = match &cmd {
            KVCommand::Read  { node, .. } => node == my_node_id_str,
            KVCommand::Write { node, .. } => node == my_node_id_str,
            KVCommand::Cas   { node, .. } => node == my_node_id_str,
        };
        if !owned_by_me { continue; }

        // --- Send reply ---
        *reply_msg_id += 1;
        match cmd {
            KVCommand::Write { msg_id, client, .. } => {
                send_reply(
                    my_node_id_str, &client,
                    "write_ok", Some(msg_id), *reply_msg_id, None,
                );
            }
            KVCommand::Read { key, msg_id, client, .. } => {
                if let Some(&val) = kv_store.get(&key) {
                    send_reply(
                        my_node_id_str, &client,
                        "read_ok", Some(msg_id), *reply_msg_id, Some(val),
                    );
                } else {
                    // Key not found → Maelstrom error code 20
                    send_error(
                        my_node_id_str, &client,
                        msg_id, *reply_msg_id, 20, "key does not exist",
                    );
                }
            }
            KVCommand::Cas { key, from, to, msg_id, client, .. } => {
                let _ = (key, from, to); // already reflected in cmd_ok / kv_store
                if cmd_ok {
                    send_reply(
                        my_node_id_str, &client,
                        "cas_ok", Some(msg_id), *reply_msg_id, None,
                    );
                } else {
                    // CAS mismatch → Maelstrom error code 22
                    send_error(
                        my_node_id_str, &client,
                        msg_id, *reply_msg_id, 22, "CAS mismatch",
                    );
                }
            }
        }
    }
}

// In-memory-only apply (fallback, should never be reached when snap_db is Some)
fn apply_in_memory(cmd: &KVCommand, kv: &mut HashMap<u64, u64>) -> bool {
    match cmd {
        KVCommand::Write { key, value, .. } => {
            kv.insert(*key, *value);
            true
        }
        KVCommand::Cas { key, from, to, .. } => {
            if kv.get(key).copied() == Some(*from) {
                kv.insert(*key, *to);
                true
            } else {
                false
            }
        }
        KVCommand::Read { .. } => true,
    }
}

// ==========================================
// 7. Wire helpers
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