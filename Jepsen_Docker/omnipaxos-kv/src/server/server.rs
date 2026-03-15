use crate::{configs::OmniPaxosKVConfig, database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_kv::common::{kv::*, messages::*, utils::Timestamp};
// ↓ 改动1：换成 PersistentStorage
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

// ↓ 改动2：类型别名换成 PersistentStorage
type OmniPaxosInstance = OmniPaxos<Command, PersistentStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

const HTTP_CLIENT_ID: ClientId = 0;
static HTTP_CMD_COUNTER: AtomicUsize = AtomicUsize::new(1_000_000);

pub struct HttpRequest {
    pub kv_cmd: KVCommand,
    pub resp_tx: oneshot::Sender<Result<Option<String>, String>>,
}

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosKVConfig,
    peers: Vec<NodeId>,
    http_rx: mpsc::Receiver<HttpRequest>,
    http_pending: HashMap<CommandId, oneshot::Sender<Result<Option<String>, String>>>,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosKVConfig) -> (Self, mpsc::Sender<HttpRequest>) {
        // ↓ 改动3：创建 PersistentStorage，路径按 server_id 区分
        // 注意：不要预先创建目录，PersistentStorage 会自己创建
        let storage_path = format!("./logs/omnipaxos_storage_{}", config.local.server_id);

        let mut storage_config = PersistentStorageConfig::default();
        storage_config.set_path(storage_path);

        let storage = PersistentStorage::new(storage_config);

        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;
        let (http_tx, http_rx) = mpsc::channel(256);

        let server = OmniPaxosServer {
            id: config.local.server_id,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
            http_rx,
            http_pending: HashMap::new(),
        };
        (server, http_tx)
    }

    pub async fn run(&mut self) {
        self.save_output().expect("Failed to write to file");

        // ★ 崩溃恢复：重启后从持久化存储恢复已决定的条目
        self.recover_from_storage();

        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;

        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                Some(http_req) = self.http_rx.recv() => {
                    self.handle_http_request(http_req);
                    self.send_outgoing_msgs();
                },
            }
        }
    }

    // ★ 新增：从持久化存储恢复数据库状态
    fn recover_from_storage(&mut self) {
        let decided_idx = self.omnipaxos.get_decided_idx();
        if decided_idx > 0 {
            info!("{}: Recovering {} entries from persistent storage", self.id, decided_idx);
            if let Some(entries) = self.omnipaxos.read_decided_suffix(0) {
                let commands: Vec<Command> = entries
                    .into_iter()
                    .filter_map(|e| match e {
                        LogEntry::Decided(cmd) => Some(cmd),
                        _ => None,
                    })
                    .collect();
                // 重放所有已提交的命令到内存数据库
                for cmd in commands {
                    self.database.handle_command(cmd.kv_cmd);
                }
                self.current_decided_idx = decided_idx;
                info!("{}: Recovery complete, restored to index {}", self.id, decided_idx);
            }
        }
    }

    fn handle_http_request(&mut self, req: HttpRequest) {
        let command_id: CommandId = HTTP_CMD_COUNTER.fetch_add(1, Ordering::Relaxed);
        self.http_pending.insert(command_id, req.resp_tx);

        let command = Command {
            client_id: HTTP_CLIENT_ID,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: req.kv_cmd,
        };

        if let Err(e) = self.omnipaxos.append(command) {
            if let Some(tx) = self.http_pending.remove(&command_id) {
                let _ = tx.send(Err(format!("not leader or append failed: {e:?}")));
            }
        }
    }

    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader() {
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            self.send_client_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start { break; }
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
            debug!("Decided {new_decided_idx}");
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands);
        }
    }

    fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        for command in commands {
            let result = self.database.handle_command(command.kv_cmd);

            if command.client_id == HTTP_CLIENT_ID {
                if command.coordinator_id == self.id {
                    if let Some(tx) = self.http_pending.remove(&command.id) {
                        let resp = match result.flatten() {
                            Some(v) if v == "__cas_failed__" =>
                                Err("precondition-failed".to_string()),
                            other => Ok(other),
                        };
                        let _ = tx.send(resp);
                    }
                }
            } else {
                if command.coordinator_id == self.id {
                    let response = match result {
                        Some(read_result) => ServerMessage::Read(command.id, read_result),
                        None => ServerMessage::Write(command.id),
                    };
                    self.network.send_to_client(command.client_id, response);
                }
            }
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Append(command_id, kv_command) => {
                    self.append_to_log(from, command_id, kv_command)
                }
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries();
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time);
                }
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, kv_command: KVCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}
