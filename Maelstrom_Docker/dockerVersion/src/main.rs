use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{self, BufRead, Read, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::macros::Entry;
use omnipaxos::storage::{StopSign, Storage, StorageResult};
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{OmniPaxos, OmniPaxosConfig};

#[derive(Clone, Debug, Serialize, Deserialize, Entry)]
pub enum KVCommand {
    Read { key: u64, msg_id: u64, client: String, node: String },
    Write { key: u64, value: u64, msg_id: u64, client: String, node: String },
    Cas { key: u64, from: u64, to: u64, msg_id: u64, client: String, node: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum CachedReply {
    WriteOk,
    ReadOk { value: Option<u64> },
    CasOk,
    Error { code: u32, text: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct AppState {
    kv: HashMap<u64, u64>,
    executed: HashMap<String, CachedReply>,
    applied_idx: u64,
}

fn cmd_req_id(cmd: &KVCommand) -> String {
    match cmd {
        KVCommand::Read { client, msg_id, .. }
        | KVCommand::Write { client, msg_id, .. }
        | KVCommand::Cas { client, msg_id, .. } => format!("{}:{}", client, msg_id),
    }
}

fn parse_node_id(s: &str) -> NodeId {
    s.trim_start_matches('n').parse::<u64>().unwrap_or(0) + 1
}
fn node_name_from_pid(pid: NodeId) -> String {
    format!("n{}", pid - 1)
}
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
    Shutdown,
}

fn storage_root() -> String {
    std::env::var("OMNIPAXOS_STORAGE_PATH").unwrap_or_else(|_| {
        let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
        cwd.join("store_local").to_string_lossy().into_owned()
    })
}

fn server_base_dir(pid: NodeId) -> String {
    format!("{}/server-{}", storage_root(), pid)
}

fn paxos_base_dir(pid: NodeId) -> String {
    format!("{}/paxos", server_base_dir(pid))
}

fn app_state_file(pid: NodeId) -> PathBuf {
    PathBuf::from(server_base_dir(pid)).join("app_state.bin")
}

fn ensure_server_dirs(pid: NodeId) {
    let _ = std::fs::create_dir_all(paxos_base_dir(pid));
}

fn fsync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn persist_app_state(pid: NodeId, state: &AppState) -> io::Result<()> {
    let dir = PathBuf::from(server_base_dir(pid));
    fs::create_dir_all(&dir)?;
    let path = app_state_file(pid);
    let tmp = dir.join("app_state.bin.tmp");
    let bytes =
        bincode::serialize(state).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    fsync_dir(&dir)?;
    Ok(())
}

fn load_app_state(pid: NodeId) -> io::Result<Option<AppState>> {
    let path = app_state_file(pid);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let s =
        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    Ok(Some(s))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CrashSafeState<T>
where
    T: omnipaxos::storage::Entry,
{
    log: Vec<T>,
    n_prom: Ballot,
    acc_round: Ballot,
    ld: u64,
    trimmed_idx: u64,
    snapshot: Option<T::Snapshot>,
    stopsign: Option<StopSign>,
}

impl<T> Default for CrashSafeState<T>
where
    T: omnipaxos::storage::Entry,
{
    fn default() -> Self {
        Self {
            log: vec![],
            n_prom: Ballot::default(),
            acc_round: Ballot::default(),
            ld: 0,
            trimmed_idx: 0,
            snapshot: None,
            stopsign: None,
        }
    }
}

#[derive(Clone, Debug)]
struct CrashSafeFileStorage<T>
where
    T: omnipaxos::storage::Entry,
{
    dir: PathBuf,
    state_file: PathBuf,
    state: CrashSafeState<T>,
}

impl<T> CrashSafeFileStorage<T>
where
    T: omnipaxos::storage::Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    fn open(dir: impl AsRef<Path>, fresh_start: bool) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let state_file = dir.join("state.bin");
        if fresh_start {
            let _ = fs::remove_dir_all(&dir);
        }
        fs::create_dir_all(&dir).expect("failed to create crash-safe storage dir");
        let state = if state_file.exists() {
            let mut f = File::open(&state_file).expect("failed to open state.bin");
            let mut buf = Vec::new();
            f.read_to_end(&mut buf).expect("failed to read state.bin");
            bincode::deserialize(&buf).expect("failed to deserialize state.bin")
        } else {
            CrashSafeState::default()
        };
        Self { dir, state_file, state }
    }

    fn persist(&self) -> StorageResult<()> {
        let tmp = self.dir.join("state.bin.tmp");
        let bytes = bincode::serialize(&self.state)?;
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.state_file)?;
        fsync_dir(&self.dir)?;
        Ok(())
    }

    fn local_index(&self, global_idx: u64) -> usize {
        global_idx.saturating_sub(self.state.trimmed_idx) as usize
    }
}

impl<T> Storage<T> for CrashSafeFileStorage<T>
where
    T: omnipaxos::storage::Entry + Serialize + for<'a> Deserialize<'a>,
    T::Snapshot: Serialize + for<'a> Deserialize<'a>,
{
    fn append_entry(&mut self, entry: T) -> StorageResult<u64> {
        self.state.log.push(entry);
        self.persist()?;
        self.get_log_len()
    }

    fn append_entries(&mut self, mut entries: Vec<T>) -> StorageResult<u64> {
        self.state.log.append(&mut entries);
        self.persist()?;
        self.get_log_len()
    }

    fn append_on_prefix(&mut self, from_idx: u64, entries: Vec<T>) -> StorageResult<u64> {
        let local_from = self.local_index(from_idx).min(self.state.log.len());
        self.state.log.truncate(local_from);
        self.state.log.extend(entries);
        self.persist()?;
        self.get_log_len()
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.state.n_prom = n_prom;
        self.persist()
    }

    fn set_decided_idx(&mut self, ld: u64) -> StorageResult<()> {
        self.state.ld = ld;
        self.persist()
    }

    fn get_decided_idx(&self) -> StorageResult<u64> {
        Ok(self.state.ld)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.state.acc_round = na;
        self.persist()
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(Some(self.state.acc_round))
    }

    fn get_entries(&self, from: u64, to: u64) -> StorageResult<Vec<T>> {
        if to <= from {
            return Ok(vec![]);
        }
        let start = self.local_index(from);
        let end = self.local_index(to);
        Ok(self.state.log.get(start..end).unwrap_or(&[]).to_vec())
    }

    fn get_log_len(&self) -> StorageResult<u64> {
        Ok(self.state.trimmed_idx + self.state.log.len() as u64)
    }

    fn get_suffix(&self, from: u64) -> StorageResult<Vec<T>> {
        let start = self.local_index(from);
        Ok(self.state.log.get(start..).unwrap_or(&[]).to_vec())
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(Some(self.state.n_prom))
    }

    fn set_stopsign(&mut self, s: Option<StopSign>) -> StorageResult<()> {
        self.state.stopsign = s;
        self.persist()
    }

    fn get_stopsign(&self) -> StorageResult<Option<StopSign>> {
        Ok(self.state.stopsign.clone())
    }

    fn trim(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        if trimmed_idx <= self.state.trimmed_idx {
            return Ok(());
        }
        let drop_n = (trimmed_idx - self.state.trimmed_idx) as usize;
        let drop_n = drop_n.min(self.state.log.len());
        self.state.log.drain(0..drop_n);
        self.state.trimmed_idx += drop_n as u64;
        if self.state.trimmed_idx < trimmed_idx {
            self.state.trimmed_idx = trimmed_idx;
        }
        self.persist()
    }

    fn set_compacted_idx(&mut self, trimmed_idx: u64) -> StorageResult<()> {
        if trimmed_idx > self.state.trimmed_idx {
            self.state.trimmed_idx = trimmed_idx;
        }
        self.persist()
    }

    fn get_compacted_idx(&self) -> StorageResult<u64> {
        Ok(self.state.trimmed_idx)
    }

    fn set_snapshot(&mut self, snapshot: Option<T::Snapshot>) -> StorageResult<()> {
        self.state.snapshot = snapshot;
        self.persist()
    }

    fn get_snapshot(&self) -> StorageResult<Option<T::Snapshot>> {
        Ok(self.state.snapshot.clone())
    }
}

fn build_omnipaxos(
    my_pid: NodeId,
    all_pids: Vec<NodeId>,
    fresh_start: bool,
) -> OmniPaxos<KVCommand, CrashSafeFileStorage<KVCommand>> {
    let base = paxos_base_dir(my_pid);
    let storage = CrashSafeFileStorage::open(&base, fresh_start);
    let mut cfg = OmniPaxosConfig::default();
    cfg.server_config.pid = my_pid;
    cfg.cluster_config.nodes = all_pids;
    cfg.cluster_config.configuration_id = 1;
    cfg.server_config.election_tick_timeout = 20;
    cfg.server_config.resend_message_tick_timeout = 3;
    cfg.build(storage).expect("OmniPaxos build failed")
}

fn send_reply(
    src: &str,
    dest: &str,
    msg_type: &str,
    in_reply_to: Option<u64>,
    msg_id: u64,
    value: Option<u64>,
) {
    let msg = Message {
        src: src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type: msg_type.to_string(),
            msg_id: Some(msg_id),
            in_reply_to,
            key: None,
            value,
            from: None,
            to: None,
            node_id: None,
            node_ids: None,
            paxos_data: None,
            extra: serde_json::Map::new(),
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_read_reply(
    src: &str,
    dest: &str,
    in_reply_to: u64,
    msg_id: u64,
    value: Option<u64>,
) {
    let msg = serde_json::json!({
        "src": src,
        "dest": dest,
        "body": {
            "type": "read_ok",
            "msg_id": msg_id,
            "in_reply_to": in_reply_to,
            "value": value
        }
    });
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_error(src: &str, dest: &str, in_reply_to: u64, msg_id: u64, code: u32, text: &str) {
    let mut extra = serde_json::Map::new();
    extra.insert("code".to_string(), serde_json::json!(code));
    extra.insert("text".to_string(), serde_json::json!(text));
    let msg = Message {
        src: src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type: "error".to_string(),
            msg_id: Some(msg_id),
            in_reply_to: Some(in_reply_to),
            key: None,
            value: None,
            from: None,
            to: None,
            node_id: None,
            node_ids: None,
            paxos_data: None,
            extra,
        },
    };
    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn send_cached_reply(src: &str, dest: &str, in_reply_to: u64, msg_id: u64, reply: &CachedReply) {
    match reply {
        CachedReply::WriteOk => send_reply(src, dest, "write_ok", Some(in_reply_to), msg_id, None),
        CachedReply::ReadOk { value } => {
            send_read_reply(src, dest, in_reply_to, msg_id, *value)
        }
        CachedReply::CasOk => send_reply(src, dest, "cas_ok", Some(in_reply_to), msg_id, None),
        CachedReply::Error { code, text } => {
            send_error(src, dest, in_reply_to, msg_id, *code, text)
        }
    }
}

fn send_forward_req(src: &str, dest: &str, cmd: &KVCommand) {
    let mut extra = serde_json::Map::new();
    extra.insert(
        "cmd".to_string(),
        serde_json::to_value(cmd).expect("serialize forwarded cmd"),
    );

    let msg = Message {
        src: src.to_string(),
        dest: dest.to_string(),
        body: Body {
            msg_type: "forward_req".to_string(),
            msg_id: None,
            in_reply_to: None,
            key: None,
            value: None,
            from: None,
            to: None,
            node_id: None,
            node_ids: None,
            paxos_data: None,
            extra,
        },
    };

    println!("{}", serde_json::to_string(&msg).unwrap());
}

fn forwarded_cmd_from_msg(msg: &Message) -> Option<KVCommand> {
    let v = msg.body.extra.get("cmd")?.clone();
    serde_json::from_value::<KVCommand>(v).ok()
}

fn node_log(node: &str, event: &str, fields: serde_json::Value) {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    eprintln!(
        "{}",
        serde_json::json!({
            "ts_ms": ts_ms,
            "node": node,
            "event": event,
            "fields": fields,
        })
    );
}

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

fn cached_reply_for(cmd: &KVCommand, cmd_ok: bool, kv: &HashMap<u64, u64>) -> CachedReply {
    match cmd {
        KVCommand::Write { .. } => CachedReply::WriteOk,
        KVCommand::Read { key, .. } => CachedReply::ReadOk {
            value: kv.get(key).copied(),
        },
        KVCommand::Cas { .. } => {
            if cmd_ok {
                CachedReply::CasOk
            } else {
                CachedReply::Error {
                    code: 22,
                    text: "CAS mismatch".to_string(),
                }
            }
        }
    }
}

struct ApplyCtx<'a> {
    kv_store: &'a mut HashMap<u64, u64>,
    executed: &'a mut HashMap<String, CachedReply>,
    pending: &'a mut HashSet<String>,
    applied_idx: &'a mut u64,
    reply_msg_id: &'a mut u64,
    my_node_id_str: &'a str,
    my_pid: NodeId,
    recovery_fence: u64,
}

fn apply_entries(entries: Vec<LogEntry<KVCommand>>, ctx: &mut ApplyCtx<'_>) {
    for entry in entries {
        let LogEntry::Decided(cmd) = entry else { continue };

        let next_idx = *ctx.applied_idx + 1;
        let rid = cmd_req_id(&cmd);

        let owned_by_me = match &cmd {
            KVCommand::Read { node, .. }
            | KVCommand::Write { node, .. }
            | KVCommand::Cas { node, .. } => node == ctx.my_node_id_str,
        };

        node_log(
            ctx.my_node_id_str,
            "command_decided_seen",
            serde_json::json!({
                "req_id": rid,
                "decided_log_idx": next_idx,
                "owned_by_me": owned_by_me,
                "recovery_fence": ctx.recovery_fence,
                "in_recovery_replay": next_idx <= ctx.recovery_fence,
                "cmd": format!("{:?}", cmd),
            }),
        );

        let cached = if let Some(cached) = ctx.executed.get(&rid).cloned() {
            node_log(
                ctx.my_node_id_str,
                "command_already_executed",
                serde_json::json!({
                    "req_id": rid,
                    "decided_log_idx": next_idx,
                    "reply": format!("{:?}", cached),
                }),
            );
            cached
        } else {
            let cmd_ok = apply_in_memory(&cmd, ctx.kv_store);

            node_log(
                ctx.my_node_id_str,
                "command_applied_in_memory",
                serde_json::json!({
                    "req_id": rid,
                    "decided_log_idx": next_idx,
                    "cmd_ok": cmd_ok,
                    "kv_size": ctx.kv_store.len(),
                }),
            );

            let cached = cached_reply_for(&cmd, cmd_ok, ctx.kv_store);
            ctx.executed.insert(rid.clone(), cached.clone());

            let app_state = AppState {
                kv: ctx.kv_store.clone(),
                executed: ctx.executed.clone(),
                applied_idx: next_idx,
            };

            if let Err(e) = persist_app_state(ctx.my_pid, &app_state) {
                node_log(
                    ctx.my_node_id_str,
                    "persist_app_state_failed",
                    serde_json::json!({
                        "req_id": rid,
                        "decided_log_idx": next_idx,
                        "error": e.to_string(),
                    }),
                );
                eprintln!(
                    "❌ [{}] persist app state failed at idx {}: {}",
                    ctx.my_node_id_str, next_idx, e
                );
                break;
            }

            node_log(
                ctx.my_node_id_str,
                "command_applied_persisted",
                serde_json::json!({
                    "req_id": rid,
                    "decided_log_idx": next_idx,
                    "reply": format!("{:?}", cached),
                }),
            );

            cached
        };

        *ctx.applied_idx = next_idx;
        ctx.pending.remove(&rid);

        node_log(
            ctx.my_node_id_str,
            "command_apply_complete",
            serde_json::json!({
                "req_id": rid,
                "applied_idx": *ctx.applied_idx,
                "pending_removed": true,
                "owned_by_me": owned_by_me,
                "in_recovery_replay": next_idx <= ctx.recovery_fence,
            }),
        );

        if next_idx <= ctx.recovery_fence || !owned_by_me {
            node_log(
                ctx.my_node_id_str,
                "reply_suppressed",
                serde_json::json!({
                    "req_id": rid,
                    "applied_idx": *ctx.applied_idx,
                    "reason": if next_idx <= ctx.recovery_fence {
                        "recovery_replay"
                    } else {
                        "not_owned_by_this_node"
                    },
                }),
            );
            continue;
        }

        *ctx.reply_msg_id += 1;
        match &cmd {
            KVCommand::Read { client, msg_id, .. }
            | KVCommand::Write { client, msg_id, .. }
            | KVCommand::Cas { client, msg_id, .. } => {
                node_log(
                    ctx.my_node_id_str,
                    "client_reply_sent",
                    serde_json::json!({
                        "req_id": rid,
                        "client": client,
                        "in_reply_to": msg_id,
                        "reply_msg_id": *ctx.reply_msg_id,
                        "reply": format!("{:?}", cached),
                        "applied_idx": *ctx.applied_idx,
                    }),
                );
                send_cached_reply(
                    ctx.my_node_id_str,
                    client,
                    *msg_id,
                    *ctx.reply_msg_id,
                    &cached,
                );
            }
        }
    }
}

fn main() {
    let (tx, rx) = mpsc::channel::<Event>();

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
                Err(_) => break,
            }
        }
        let _ = tx_in.send(Event::Shutdown);
    });

    let tx_tick = tx.clone();
    thread::spawn(move || loop {
        thread::sleep(Duration::from_millis(5));
        if tx_tick.send(Event::Tick).is_err() {
            break;
        }
    });

    let mut my_node_id_str = String::new();
    let mut my_pid: NodeId = 0;
    let mut all_nodes: Vec<String> = Vec::new();

    let mut kv_store: HashMap<u64, u64> = HashMap::new();
    let mut executed: HashMap<String, CachedReply> = HashMap::new();
    let mut pending: HashSet<String> = HashSet::new();
    let mut applied_idx: u64 = 0;
    let mut recovery_fence: u64 = 0;
    let mut reply_msg_id: u64 = 0;
    let mut recovering = false;
    let mut omnipaxos: Option<OmniPaxos<KVCommand, CrashSafeFileStorage<KVCommand>>> = None;

    for event in rx {
        match event {
            Event::Message(msg) => match msg.body.msg_type.as_str() {
                "init" => {
                    if let (Some(nid), Some(raw_nodes)) =
                        (msg.body.node_id.clone(), msg.body.node_ids.clone())
                    {
                        my_node_id_str = nid;
                        my_pid = parse_node_id(&my_node_id_str);
                        ensure_server_dirs(my_pid);
                        all_nodes = raw_nodes.clone();

                        let all_pids: Vec<NodeId> =
                            raw_nodes.iter().map(|s| parse_node_id(s)).collect();

                        let restart_mode = std::env::var("OMNIPAXOS_RECOVER")
                            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                            .unwrap_or(false);

                        kv_store.clear();
                        executed.clear();
                        pending.clear();
                        applied_idx = 0;
                        recovery_fence = 0;
                        recovering = false;

                        let op = if restart_mode {
                            let op = build_omnipaxos(my_pid, all_pids.clone(), false);
                            let local_decided = op.get_decided_idx();

                            let loaded = if let Ok(Some(app)) = load_app_state(my_pid) {
                                if app.applied_idx <= local_decided {
                                    kv_store = app.kv;
                                    executed = app.executed;
                                    applied_idx = app.applied_idx;
                                    true
                                } else {
                                    false
                                }
                            } else {
                                false
                            };

                            recovery_fence = local_decided;
                            recovering = applied_idx < recovery_fence;

                            node_log(
                                &my_node_id_str,
                                "recovery_mode_entered",
                                serde_json::json!({
                                    "restart_mode": true,
                                    "local_decided_idx": local_decided,
                                    "loaded_app_state": loaded,
                                }),
                            );

                            eprintln!(
                                "🔄 [{}] restart: restored paxos storage, local decided_idx={}, replay_from={}",
                                my_node_id_str,
                                local_decided,
                                applied_idx + 1
                            );

                            node_log(
                                &my_node_id_str,
                                "recovery_state_ready",
                                serde_json::json!({
                                    "applied_idx": applied_idx,
                                    "recovery_fence": recovery_fence,
                                    "replay_from": applied_idx + 1,
                                    "recovering": recovering,
                                }),
                            );
                            if !recovering {
                                node_log(
                                    &my_node_id_str,
                                    "recovery_complete",
                                    serde_json::json!({
                                        "applied_idx": applied_idx,
                                        "recovery_fence": recovery_fence,
                                        "mode": "init_already_caught_up",
                                    }),
                                );
                            }
                            op
                        } else {
                            let _ = fs::remove_file(app_state_file(my_pid));
                            eprintln!("🆕 [{}] fresh start", my_node_id_str);
                            node_log(
                                &my_node_id_str,
                                "fresh_start",
                                serde_json::json!({
                                    "restart_mode": false
                                }),
                            );
                            build_omnipaxos(my_pid, all_pids.clone(), true)
                        };

                        omnipaxos = Some(op);
                        reply_msg_id += 1;
                        send_reply(
                            &my_node_id_str,
                            &msg.src,
                            "init_ok",
                            msg.body.msg_id,
                            reply_msg_id,
                            None,
                        );
                    }
                }

                "read" | "write" | "cas" => {
                    if let Some(op) = &mut omnipaxos {
                        let client = msg.src.clone();
                        let node = my_node_id_str.clone();

                        if let Some(m_id) = msg.body.msg_id {
                            let cmd = match msg.body.msg_type.as_str() {
                                "read" => msg.body.key.map(|k| KVCommand::Read {
                                    key: k,
                                    msg_id: m_id,
                                    client: client.clone(),
                                    node: node.clone(),
                                }),
                                "write" => {
                                    if let (Some(k), Some(v)) = (msg.body.key, msg.body.value) {
                                        Some(KVCommand::Write {
                                            key: k,
                                            value: v,
                                            msg_id: m_id,
                                            client: client.clone(),
                                            node: node.clone(),
                                        })
                                    } else {
                                        None
                                    }
                                }
                                "cas" => {
                                    if let (Some(k), Some(f), Some(t)) =
                                        (msg.body.key, msg.body.from, msg.body.to)
                                    {
                                        Some(KVCommand::Cas {
                                            key: k,
                                            from: f,
                                            to: t,
                                            msg_id: m_id,
                                            client: client.clone(),
                                            node: node.clone(),
                                        })
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };

                            if let Some(c) = cmd {
                                let rid = cmd_req_id(&c);

                                node_log(
                                    &my_node_id_str,
                                    "client_request_received",
                                    serde_json::json!({
                                        "req_id": rid,
                                        "type": msg.body.msg_type,
                                        "client": client,
                                        "msg_id": m_id,
                                        "key": msg.body.key,
                                        "value": msg.body.value,
                                        "from": msg.body.from,
                                        "to": msg.body.to,
                                        "recovering": recovering,
                                        "pending_len": pending.len(),
                                    }),
                                );

                                if let Some(cached) = executed.get(&rid).cloned() {
                                    node_log(
                                        &my_node_id_str,
                                        "client_request_dedup_hit",
                                        serde_json::json!({
                                            "req_id": rid,
                                            "reply": format!("{:?}", cached),
                                        }),
                                    );
                                    reply_msg_id += 1;
                                    send_cached_reply(
                                        &my_node_id_str,
                                        &client,
                                        m_id,
                                        reply_msg_id,
                                        &cached,
                                    );
                                    continue;
                                }

                                if pending.contains(&rid) {
                                    node_log(
                                        &my_node_id_str,
                                        "client_request_pending_duplicate",
                                        serde_json::json!({
                                            "req_id": rid,
                                        }),
                                    );
                                    reply_msg_id += 1;
                                    send_error(
                                        &my_node_id_str,
                                        &client,
                                        m_id,
                                        reply_msg_id,
                                        11,
                                        "request already pending",
                                    );
                                    continue;
                                }

                                if recovering {
                                    node_log(
                                        &my_node_id_str,
                                        "client_request_rejected_recovering",
                                        serde_json::json!({
                                            "req_id": rid,
                                        }),
                                    );
                                    reply_msg_id += 1;
                                    send_error(
                                        &my_node_id_str,
                                        &client,
                                        m_id,
                                        reply_msg_id,
                                        11,
                                        "node recovering",
                                    );
                                    continue;
                                }

                                match op.append(c.clone()) {
                                    Err(e) => {
                                        node_log(
                                            &my_node_id_str,
                                            "client_request_append_rejected",
                                            serde_json::json!({
                                                "req_id": rid,
                                                "error": format!("{:?}", e),
                                            }),
                                        );

                                        // 尝试把请求转发给其他节点；真 leader 会 append 成功
                                        let mut forwarded_to = Vec::new();
                                        for peer in all_nodes.iter() {
                                            if peer != &my_node_id_str {
                                                send_forward_req(&my_node_id_str, peer, &c);
                                                forwarded_to.push(peer.clone());
                                            }
                                        }

                                        if !forwarded_to.is_empty() {
                                            pending.insert(rid.clone());
                                            node_log(
                                                &my_node_id_str,
                                                "client_request_forwarded",
                                                serde_json::json!({
                                                    "req_id": rid,
                                                    "forwarded_to": forwarded_to,
                                                }),
                                            );
                                            // 这里不要立刻回复 client；等 decided 后由 owned_by_me 节点回包
                                        } else {
                                            reply_msg_id += 1;
                                            send_error(
                                                &my_node_id_str,
                                                &client,
                                                m_id,
                                                reply_msg_id,
                                                11,
                                                &format!("not leader / not ready: {:?}", e),
                                            );
                                        }
                                    }
                                    Ok(_) => {
                                        node_log(
                                            &my_node_id_str,
                                            "client_request_appended",
                                            serde_json::json!({
                                                "req_id": rid,
                                                "pending_len_after": pending.len() + 1,
                                            }),
                                        );
                                        pending.insert(rid);
                                    }
                                }
                            }
                        }
                    } else if let Some(m_id) = msg.body.msg_id {
                        reply_msg_id += 1;
                        send_error(
                            &my_node_id_str,
                            &msg.src,
                            m_id,
                            reply_msg_id,
                            11,
                            "node not initialized",
                        );
                    }
                }

                "paxos_net" => {
                    if let (Some(data), Some(op)) = (msg.body.paxos_data.clone(), &mut omnipaxos) {
                        match bincode::deserialize::<omnipaxos::messages::Message<KVCommand>>(&data)
                        {
                            Ok(p_msg) => {
                                if panic::catch_unwind(panic::AssertUnwindSafe(|| {
                                    op.handle_incoming(p_msg)
                                }))
                                .is_err()
                                {
                                    eprintln!("⚠️  [{}] handle_incoming panicked", my_node_id_str);
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "⚠️  [{}] bincode deserialize failed: {}",
                                    my_node_id_str, e
                                )
                            }
                        }
                    }
                }

                "forward_req" => {
                    if let Some(op) = &mut omnipaxos {
                        if recovering {
                            node_log(
                                &my_node_id_str,
                                "forward_req_rejected_recovering",
                                serde_json::json!({}),
                            );
                            continue;
                        }

                        if let Some(cmd) = forwarded_cmd_from_msg(&msg) {
                            let rid = cmd_req_id(&cmd);

                            if executed.contains_key(&rid) {
                                node_log(
                                    &my_node_id_str,
                                    "forward_req_dedup_executed",
                                    serde_json::json!({
                                        "req_id": rid,
                                    }),
                                );
                                continue;
                            }

                            if pending.contains(&rid) {
                                node_log(
                                    &my_node_id_str,
                                    "forward_req_dedup_pending",
                                    serde_json::json!({
                                        "req_id": rid,
                                    }),
                                );
                                continue;
                            }

                            match op.append(cmd.clone()) {
                                Ok(_) => {
                                    pending.insert(rid.clone());
                                    node_log(
                                        &my_node_id_str,
                                        "forward_req_appended",
                                        serde_json::json!({
                                            "req_id": rid,
                                            "cmd": format!("{:?}", cmd),
                                        }),
                                    );
                                }
                                Err(e) => {
                                    node_log(
                                        &my_node_id_str,
                                        "forward_req_append_rejected",
                                        serde_json::json!({
                                            "req_id": rid,
                                            "error": format!("{:?}", e),
                                        }),
                                    );
                                }
                            }
                        } else {
                            node_log(
                                &my_node_id_str,
                                "forward_req_decode_failed",
                                serde_json::json!({}),
                            );
                        }
                    }
                }               
                _ => {}
            },

            Event::Tick => {
                if let Some(op) = &mut omnipaxos {
                    op.tick();

                    let outbound = op.outgoing_messages();
                    for m in outbound {
                        let bytes = bincode::serialize(&m).unwrap();
                        let body = Body {
                            msg_type: "paxos_net".to_string(),
                            msg_id: None,
                            in_reply_to: None,
                            key: None,
                            value: None,
                            from: None,
                            to: None,
                            node_id: None,
                            node_ids: None,
                            paxos_data: Some(bytes),
                            extra: serde_json::Map::new(),
                        };
                        let net = Message {
                            src: my_node_id_str.clone(),
                            dest: node_name_from_pid(m.get_receiver()),
                            body,
                        };
                        println!("{}", serde_json::to_string(&net).unwrap());
                    }

                    let d_idx = op.get_decided_idx();
                    if d_idx > applied_idx {
                        match panic::catch_unwind(panic::AssertUnwindSafe(|| {
                            op.read_decided_suffix(applied_idx)
                        })) {
                            Ok(Some(entries)) => {
                                let mut ctx = ApplyCtx {
                                    kv_store: &mut kv_store,
                                    executed: &mut executed,
                                    pending: &mut pending,
                                    applied_idx: &mut applied_idx,
                                    reply_msg_id: &mut reply_msg_id,
                                    my_node_id_str: &my_node_id_str,
                                    my_pid,
                                    recovery_fence,
                                };

                                apply_entries(entries, &mut ctx);

                                if recovering && applied_idx >= recovery_fence {
                                    recovering = false;
                                    node_log(
                                        &my_node_id_str,
                                        "recovery_complete",
                                        serde_json::json!({
                                            "applied_idx": applied_idx,
                                            "recovery_fence": recovery_fence,
                                        }),
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(_) => {
                                eprintln!(
                                    "⚠️  [{}] read_decided_suffix panicked (applied={}, decided={})",
                                    my_node_id_str, applied_idx, d_idx
                                )
                            }
                        }
                    }
                }
            }

            Event::Shutdown => break,
        }
    }
}