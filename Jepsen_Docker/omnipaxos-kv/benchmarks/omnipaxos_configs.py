from __future__ import annotations

from dataclasses import asdict, dataclass, replace

import toml

from gcp_cluster import InstanceConfig


@dataclass(frozen=True)
class ClusterConfig:
    omnipaxos_cluster_config: OmniPaxosKVClusterConfig
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    client_image: str
    server_image: str

    @dataclass(frozen=True)
    class OmniPaxosKVClusterConfig:
        nodes: list[int]
        node_addrs: list[str]
        initial_leader: int
        initial_flexible_quorum: FlexibleQuorum | None

    def __post_init__(self):
        self.validate()

    def validate(self):
        op_config = self.omnipaxos_cluster_config
        if op_config.initial_flexible_quorum:
            read_quorum = op_config.initial_flexible_quorum.read_quorum_size
            write_quorum = op_config.initial_flexible_quorum.write_quorum_size
            if read_quorum < 2:
                raise ValueError(f"Read quorum must be greater than 2")
            if write_quorum < 2:
                raise ValueError(f"Write quorum must be greater than 2")
            if read_quorum + write_quorum <= len(op_config.nodes):
                raise ValueError(
                    f"Flexible quorum {(read_quorum, write_quorum)} must guarantee overlap"
                )

        for client_id in self.client_configs.keys():
            if client_id not in self.server_configs.keys():
                raise ValueError(f"Client {client_id} has no server to connect to")

        for server_id, server_config in self.server_configs.items():
            client_configs = self.client_configs.values()
            server_id_matches = sum(
                1
                for _ in filter(
                    lambda c: c.omnipaxos_client_config.server_id == server_id,
                    client_configs,
                )
            )
            total_matches = server_id_matches
            server_num_clients = server_config.omnipaxos_server_config.num_clients
            if server_num_clients != server_id_matches:
                raise ValueError(
                    f"Server {server_id} has {server_num_clients} clients but found {total_matches} references among client configs"
                )

        if op_config.initial_leader not in self.server_configs.keys():
            raise ValueError(
                f"Initial leader {op_config.initial_leader} must be one of the server nodes"
            )

        server_ids = sorted(self.server_configs.keys())
        if op_config.nodes != server_ids:
            raise ValueError(
                f"Cluster nodes {op_config.nodes} must match defined server ids {server_ids}"
            )

    def update_omnipaxos_config(self, **kwargs) -> ClusterConfig:
        new_op_config = replace(self.omnipaxos_cluster_config, **kwargs)
        new_config = replace(self, omnipaxos_cluster_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_cluster_toml(self) -> str:
        cluster_toml_str = toml.dumps(asdict(self.omnipaxos_cluster_config))
        return cluster_toml_str


@dataclass(frozen=True)
class ServerConfig:
    instance_config: InstanceConfig
    omnipaxos_server_config: OmniPaxosKVServerConfig
    rust_log: str
    server_address: str

    @dataclass(frozen=True)
    class OmniPaxosKVServerConfig:
        location: str
        server_id: int
        listen_address: str
        listen_port: int
        num_clients: int
        output_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        op_config = self.omnipaxos_server_config
        if op_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {op_config.server_id}. It must be greater than 0."
            )

        if op_config.num_clients < 0:
            raise ValueError(
                f"Invalid num_clients: {op_config.num_clients}. It must be a positive number."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_omnipaxos_config(self, **kwargs) -> ServerConfig:
        new_op_config = replace(self.omnipaxos_server_config, **kwargs)
        new_config = replace(self, omnipaxos_server_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_server_toml(self) -> str:
        server_toml_str = toml.dumps(asdict(self.omnipaxos_server_config))
        return server_toml_str


@dataclass(frozen=True)
class ClientConfig:
    instance_config: InstanceConfig
    omnipaxos_client_config: OmniPaxosKVClientConfig
    rust_log: str = "info"

    @dataclass(frozen=True)
    class OmniPaxosKVClientConfig:
        location: str
        server_id: int
        server_address: str
        requests: list[RequestInterval]
        summary_filepath: str
        output_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        op_config = self.omnipaxos_client_config
        if op_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {op_config.server_id}. It must be greater than 0."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_omnipaxos_config(self, **kwargs) -> ClientConfig:
        new_op_config = replace(self.omnipaxos_client_config, **kwargs)
        new_config = replace(self, omnipaxos_client_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_client_toml(self) -> str:
        client_toml_str = toml.dumps(asdict(self.omnipaxos_client_config))
        return client_toml_str


@dataclass(frozen=True)
class FlexibleQuorum:
    read_quorum_size: int
    write_quorum_size: int


@dataclass(frozen=True)
class RequestInterval:
    duration_sec: int
    requests_per_sec: int
    read_ratio: float
