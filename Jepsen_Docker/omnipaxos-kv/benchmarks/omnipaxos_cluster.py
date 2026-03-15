import subprocess
from pathlib import Path

from gcp_cluster import GcpCluster, InstanceConfig
from gcp_ssh_client import GcpClusterSSHClient
from omnipaxos_configs import *


class OmnipaxosCluster:
    """
    Orchestration class for managing a Omnipaxos cluster on GCP.

    This class automates the setup, deployment, and management of Omnipaxos servers and clients
    on Google Cloud Platform (GCP) instances. It abstracts the steps required to push Docker images,
    start instances, configure and launch Omnipaxos containers, and manage logs and shutdown operations.

    Deployment Steps:
    1.   Configure project settings (See `./scripts/project_env.sh`). Configure gcloud authentication (see `./scripts/auth.sh`).
    2.   Push Omnipaxos server and client Docker images to GCR (Google Cloud Registry).
         See `./scripts/push-server-image.sh` and `./scripts/push-client-image.sh` for details.
    3-4. `__init__` Initializes the cluster by creating GCP instances (using the GcpCluster class) for Omnipaxos servers and clients.
         The instances will run startup scripts (passed via ClusterConfig) to configure Docker for the gcloud OS login user and assign
         DNS names to the servers.
    5-6. Use `run()` to SSH into client and server instances, pass configuration files,
         and run Docker containers from the artifact registry. This also waits for client processes to finish
         and then kills the remote processes and pulls logs from the server and client GCP instances
    7.   Use `shutdown()` to shut down the GCP instances (or leave them running for reuse).
    """

    _cluster_config: ClusterConfig
    _gcp_cluster: GcpCluster
    _gcp_ssh_client: GcpClusterSSHClient

    def __init__(self, project_id: str, cluster_config: ClusterConfig):
        self._cluster_config = cluster_config
        instance_configs = [
            c.instance_config for c in cluster_config.server_configs.values()
        ]
        instance_configs.extend(
            [c.instance_config for c in cluster_config.client_configs.values()]
        )
        self._gcp_cluster = GcpCluster(project_id, instance_configs)
        kill_command = (
            "docker kill client > /dev/null 2>&1; docker kill server > /dev/null 2>&1"
        )
        self._gcp_ssh_client = GcpClusterSSHClient(self._gcp_cluster, kill_command)


    def run(self, logs_directory: Path, pull_images: bool = False):
        """
        Starts servers and clients but only waits for client processes to exit before
        killing remote processes and pulling logs.
        """
        server_process_ids = self._start_servers(pull_images=pull_images)
        client_process_ids = self._start_clients(pull_images=pull_images)
        clients_finished = self._gcp_ssh_client.await_processes_concurrent(client_process_ids)
        if clients_finished:
            self._gcp_ssh_client.clear_processes(client_process_ids)
            self._gcp_ssh_client.stop_processes(server_process_ids)
        else:
            self._gcp_ssh_client.stop_processes(server_process_ids + client_process_ids)
        self._get_logs(logs_directory)


    def change_cluster_config(self, **kwargs):
        self._cluster_config = self._cluster_config.update_omnipaxos_config(**kwargs)


    def change_server_config(self, server_id: int, **kwargs):
        server_config = self._get_server_config(server_id)
        self._cluster_config.server_configs[server_id] = server_config.update_omnipaxos_config(
            **kwargs
        )


    def change_client_config(self, client_id: int, **kwargs):
        client_config = self._get_client_config(client_id)
        self._cluster_config.client_configs[client_id] = client_config.update_omnipaxos_config(
            **kwargs
        )


    def shutdown(self):
        instance_names = [
            c.instance_config.name for c in self._cluster_config.server_configs.values()
        ]
        client_names = [
            c.instance_config.name for c in self._cluster_config.client_configs.values()
        ]
        instance_names.extend(client_names)
        self._gcp_cluster.shutdown_instances(instance_names)


    def _start_servers(self, pull_images: bool = False) -> list[str]:
        process_ids = []
        for id, config in self._cluster_config.server_configs.items():
            process_id = f"server-{id}"
            instance_name = config.instance_config.name
            ssh_command = self._start_server_command(id, pull_image=pull_images)
            self._gcp_ssh_client.start_process(process_id, instance_name, ssh_command)
            process_ids.append(process_id)
        return process_ids


    def _start_clients(self, pull_images: bool = False):
        process_ids = []
        for id, config in self._cluster_config.client_configs.items():
            process_id = f"client-{id}"
            instance_name = config.instance_config.name
            ssh_command = self._start_client_command(id, pull_image=pull_images)
            self._gcp_ssh_client.start_process(process_id, instance_name, ssh_command)
            process_ids.append(process_id)
        return process_ids


    def _get_logs(self, dest_directory: Path):
        # Make sure destination directory exists
        subprocess.run(["mkdir", "-p", dest_directory])
        instance_results_dir = "./results"
        processes = []
        for config in self._cluster_config.server_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, instance_results_dir, dest_directory
            )
            processes.append(scp_process)
        for config in self._cluster_config.client_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(
                name, instance_results_dir, dest_directory
            )
            processes.append(scp_process)
        successes = 0
        for process in processes:
            process.wait()
            if process.returncode == 0:
                successes += 1
        print(f"Collected logs from {successes} instances")


    def _get_server_config(self, server_id: int) -> ServerConfig:
        server_config = self._cluster_config.server_configs.get(server_id)
        if server_config is None:
            raise ValueError(f"Server {server_id} doesn't exist")
        return server_config


    def _get_client_config(self, client_id: int) -> ClientConfig:
        client_config = self._cluster_config.client_configs.get(client_id)
        if client_config is None:
            raise ValueError(f"Client {client_id} doesn't exist")
        return client_config


    def _start_server_command(self, server_id: int, pull_image: bool = False) -> str:
        config = self._get_server_config(server_id)
        op_config = config.omnipaxos_server_config
        server_config_toml = config.generate_server_toml()
        cluster_config_toml = self._cluster_config.generate_cluster_toml()
        start_server_command = (
            f"PULL_IMAGE={'true' if pull_image else 'false'}",
            f"SERVER_IMAGE={self._cluster_config.server_image}",
            f"SERVER_CONFIG_TOML=$(cat <<EOF\n{server_config_toml}\nEOF\n)",
            f"CLUSTER_CONFIG_TOML=$(cat <<EOF\n{cluster_config_toml}\nEOF\n)",
            f"LISTEN_PORT={op_config.listen_port}",
            f"RUST_LOG={config.rust_log}",
            f"SERVER_ID={op_config.server_id}",
            "bash ./run_container.sh",
        )
        return " ".join(start_server_command)


    def _start_client_command(self, client_id: int, pull_image: bool = False) -> str:
        config = self._get_client_config(client_id)
        client_config_toml = config.generate_client_toml()
        start_client_command = (
            f"PULL_IMAGE={'true' if pull_image else 'false'}",
            f"CLIENT_CONFIG_TOML=$(cat <<EOF\n{client_config_toml}\nEOF\n)",
            f"RUST_LOG={config.rust_log}",
            f"CLIENT_IMAGE={self._cluster_config.client_image}",
            f"bash ./run_container.sh",
        )
        return " ".join(start_client_command)


class OmnipaxosClusterBuilder:
    """
    Builder class for defining and validating configurations to start a OmnipaxosCluster.
    Relies on environment variables from `./scripts/project_env.sh` to configure settings.
    """

    def __init__(self, cluster_id: int) -> None:
        env_vals = self._get_project_env_variables()
        self.cluster_id = f"{cluster_id}"
        self._project_id = env_vals["PROJECT_ID"]
        self._service_account = env_vals["SERVICE_ACCOUNT"]
        self._gcloud_ssh_user = env_vals["OSLOGIN_USERNAME"]
        self._gcloud_oslogin_uid = env_vals['OSLOGIN_UID']
        self._server_docker_image_name = env_vals["SERVER_DOCKER_IMAGE_NAME"]
        self._client_docker_image_name = env_vals["CLIENT_DOCKER_IMAGE_NAME"]
        self._instance_startup_script = self._get_instance_startup_script()
        self._server_configs: dict[int, ServerConfig] = {}
        self._client_configs: dict[int, ClientConfig] = {}
        # Cluster-wide settings
        self._initial_leader: int | None = None
        self._initial_quorum: FlexibleQuorum | None = None
        self._server_port: int = 8000

    def server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
        rust_log: str = "info",
    ):
        if server_id in self._server_configs.keys():
            raise ValueError(f"Server {server_id} already exists")
        instance_name = f"user-{self._gcloud_oslogin_uid}-cluster-{self.cluster_id}-server-{server_id}"
        instance_config = InstanceConfig(
            name=instance_name,
            zone=zone,
            machine_type=machine_type,
            startup_script=self._instance_startup_script,
            custom_metadata={
                "oslogin_user": self._gcloud_ssh_user,
                "docker_image": self._server_docker_image_name,
                "run_container_script": self._get_run_server_script(),
            },
            dns_name=instance_name,
            service_account=self._service_account,
        )
        server_address =f"{instance_config.dns_name}.internal.zone.:{self._server_port}"
        server_config = ServerConfig(
            instance_config=instance_config,
            server_address=server_address,
            omnipaxos_server_config=ServerConfig.OmniPaxosKVServerConfig(
                location=zone,
                server_id=server_id,
                listen_address="0.0.0.0",
                listen_port=self._server_port,
                num_clients=0,
                output_filepath=f"server-{server_id}.json",
            ),
            rust_log=rust_log,
        )
        self._server_configs[server_id] = server_config
        return self

    def client(
        self,
        server_id: int,
        zone: str,
        requests: list[RequestInterval] = [],
        machine_type: str = "e2-standard-2",
        rust_log: str = "info",
    ):
        if server_id in self._client_configs.keys():
            raise ValueError(f"Client {server_id} already exists")
        instance_config = InstanceConfig(
            name=f"user-{self._gcloud_oslogin_uid}-cluster-{self.cluster_id}-client-{server_id}",
            zone=zone,
            machine_type=machine_type,
            startup_script=self._instance_startup_script,
            service_account=self._service_account,
            custom_metadata={
                "oslogin_user": self._gcloud_ssh_user,
                "docker_image": self._client_docker_image_name,
                "run_container_script": self._get_run_client_script(),
            },
        )
        client_config = ClientConfig(
            instance_config=instance_config,
            omnipaxos_client_config=ClientConfig.OmniPaxosKVClientConfig(
                location=zone,
                server_id=server_id,
                server_address="",
                requests=requests,
                summary_filepath=f"client-{server_id}.json",
                output_filepath=f"client-{server_id}.csv",
            ),
            rust_log=rust_log,
        )
        self._client_configs[server_id] = client_config
        return self

    def initial_leader(self, initial_leader: int):
        self._initial_leader = initial_leader
        return self

    def initial_quorum(self, flex_quorum: FlexibleQuorum):
        self._initial_quorum = flex_quorum
        return self

    def build(self) -> OmnipaxosCluster:
        # Add num_clients to server configs
        for server_id, server_config in self._server_configs.items():
            client_configs = self._client_configs.values()
            server_id_matches = sum(
                1 for _ in filter(lambda c: c.omnipaxos_client_config.server_id == server_id, client_configs)
            )
            total_matches = server_id_matches
            self._server_configs[server_id] = server_config.update_omnipaxos_config(num_clients=total_matches)

        # Add server_address to client configs
        for client_id, client_config in self._client_configs.items():
            server_config = self._server_configs[client_config.omnipaxos_client_config.server_id]
            self._client_configs[client_id] = client_config.update_omnipaxos_config(server_address=server_config.server_address)

        if self._initial_leader is None:
            raise ValueError("Need to set cluster's initial leader")

        nodes = sorted(self._server_configs.keys())
        node_addrs = list(map(lambda id: self._server_configs[id].server_address, nodes))

        cluster_config = ClusterConfig(
            omnipaxos_cluster_config=ClusterConfig.OmniPaxosKVClusterConfig(
                nodes=nodes,
                node_addrs=node_addrs,
                initial_leader=self._initial_leader,
                initial_flexible_quorum=self._initial_quorum,
            ),
            server_configs=self._server_configs,
            client_configs=self._client_configs,
            client_image=self._client_docker_image_name,
            server_image=self._server_docker_image_name,
        )
        return OmnipaxosCluster(self._project_id, cluster_config)


    @staticmethod
    def _get_project_env_variables() -> dict[str,str]:
        env_keys = [
            "PROJECT_ID",
            "SERVICE_ACCOUNT",
            "OSLOGIN_USERNAME",
            "OSLOGIN_UID",
            "CLIENT_DOCKER_IMAGE_NAME",
            "SERVER_DOCKER_IMAGE_NAME",
        ]
        env_vals = {}
        process = subprocess.run(['bash', '-c', 'source ./scripts/project_env.sh && env'], check=True, stdout= subprocess.PIPE, text=True)
        for line in process.stdout.split("\n"):
            (key, _, value) = line.partition("=")
            if key in env_keys:
                env_keys.remove(key)
                env_vals[key] = value
        for key in env_keys:
            raise ValueError(f"{key} env var must be set. Sourcing from ./scripts/project_env.sh")
        return env_vals


    @staticmethod
    def _get_instance_startup_script() -> str:
        with open("./startup_scripts/instance_startup_script.sh", "r") as f:
            return f.read()


    @staticmethod
    def _get_run_server_script() -> str:
        with open("./startup_scripts/run_server.sh", "r") as f:
            return f.read()


    @staticmethod
    def _get_run_client_script() -> str:
        with open("./startup_scripts/run_client.sh", "r") as f:
            return f.read()
