import json
import os
import queue
import random
import shlex
import socket
import subprocess
import threading
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import sys

SEED = 42
RNG = random.Random(SEED)

# ALL_NODES = ["n1", "n2", "n3"]
# NODE_PORTS = {"n1": 7001, "n2": 7002, "n3": 7003}

ALL_NODES = ["n1", "n2", "n3"]
NODE_PORTS = {
    "n1": 7001,
    "n2": 7002,
    "n3": 7003,
}

CONTAINER_NAMES = {n: f"omnipaxos-{n}" for n in ALL_NODES}
COMPOSE = ["docker-compose"]

WORKLOAD_SECONDS = 60.0
CLIENTS = 4
KEYSPACE = 7
NEMESIS_INTERVAL_S = 3.0
PROBE_KEY_BASE = 10_000_000

NODE_LOCKS = {n: threading.Lock() for n in ALL_NODES}

MISSING = object()
sys.setrecursionlimit(20000)

VERBOSE_STDOUT = False
PRINT_WITNESS_TO_STDOUT = False
PRINT_DOCKER_LOGS_TO_STDOUT = False
PRINT_EVENT_KINDS = {"nemesis_action"}


def console(msg: str):
    if VERBOSE_STDOUT:
        print(msg)


def now_ts() -> float:
    return time.time()


@dataclass
class ArtifactWriter:
    root: Path = field(default_factory=lambda: Path("store/linear-nemesis") / time.strftime("%Y%m%dT%H%M%S"))
    events: List[dict] = field(default_factory=list)
    history: List[dict] = field(default_factory=list)
    nemesis: List[dict] = field(default_factory=list)
    analysis: Dict[str, Any] = field(default_factory=dict)
    recovery_report: Dict[str, Any] = field(default_factory=dict)
    node_actions: Dict[str, List[dict]] = field(default_factory=lambda: {n: [] for n in ALL_NODES})
    docker_log_files: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        self.root.mkdir(parents=True, exist_ok=True)

    def event(self, kind: str, **kwargs):
        item = {"ts": now_ts(), "kind": kind, **kwargs}
        self.events.append(item)

        if kind in PRINT_EVENT_KINDS:
            print(f"[event] {kind}: {kwargs}")

        related_nodes = []
        for key in ("node", "src", "dest", "victim", "leader", "leader_at_time"):
            val = kwargs.get(key)
            if isinstance(val, str) and val in ALL_NODES:
                related_nodes.append(val)

        related_nodes = sorted(set(related_nodes))
        for node in related_nodes:
            self.node_actions[node].append(item)

    def append_history(self, item: dict):
        if "start" in item and "end" in item and item["end"] < item["start"]:
            self.event("bad_history_timestamp", item=item)
        self.history.append(item)

    def append_nemesis(self, item: dict):
        self.nemesis.append(item)

    def set_analysis(self, item: dict):
        self.analysis = item

    def set_recovery_report(self, item: dict):
        self.recovery_report = item

    def set_docker_log_file(self, node: str, path: str):
        self.docker_log_files[node] = path

    def flush(self):
        (self.root / "events.json").write_text(json.dumps(self.events, indent=2), encoding="utf-8")
        (self.root / "history.json").write_text(json.dumps(self.history, indent=2), encoding="utf-8")
        (self.root / "nemesis.json").write_text(json.dumps(self.nemesis, indent=2), encoding="utf-8")
        (self.root / "analysis.json").write_text(json.dumps(self.analysis, indent=2, ensure_ascii=False), encoding="utf-8")
        (self.root / "recovery_report.json").write_text(json.dumps(self.recovery_report, indent=2, ensure_ascii=False), encoding="utf-8")
        (self.root / "node_actions.json").write_text(json.dumps(self.node_actions, indent=2, ensure_ascii=False), encoding="utf-8")
        (self.root / "docker_log_files.json").write_text(json.dumps(self.docker_log_files, indent=2, ensure_ascii=False), encoding="utf-8")


ART = ArtifactWriter()


def sh(cmd, check=True, env=None, timeout=None):
    if VERBOSE_STDOUT:
        print(f"[cmd] {' '.join(shlex.quote(str(x)) for x in cmd)}")
    return subprocess.run(cmd, check=check, env=env, timeout=timeout)


class Partition:
    def __init__(self):
        self.lock = threading.Lock()
        self.left = set()
        self.right = set()
        self.isolated = set()

    def clear(self):
        with self.lock:
            self.left.clear()
            self.right.clear()
            self.isolated.clear()

    def isolate(self, node: str):
        with self.lock:
            self.left.clear()
            self.right.clear()
            self.isolated = {node}

    def split(self, left: List[str], right: List[str]):
        with self.lock:
            self.left = set(left)
            self.right = set(right)
            self.isolated.clear()

    def allowed(self, src: str, dst: str) -> bool:
        with self.lock:
            if src in self.isolated or dst in self.isolated:
                return False
            if self.left and self.right:
                if (src in self.left and dst in self.right) or (src in self.right and dst in self.left):
                    return False
            return True


PARTITION = Partition()


class NodeConn:
    def __init__(self, name: str, port: int, router: "Router"):
        self.name = name
        self.port = port
        self.router = router
        self.lock = threading.Lock()
        self.sock = None
        self.rfile = None
        self.wfile = None
        self.alive = False
        self.reader_thread = None

    def connect(self, retries=120, sleep_s=0.5):
        self.close()
        last_err = None
        for _ in range(retries):
            try:
                self.sock = socket.create_connection(("127.0.0.1", self.port), timeout=2.0)
                self.sock.settimeout(None)
                self.rfile = self.sock.makefile("r", encoding="utf-8", newline="\n")
                self.wfile = self.sock.makefile("w", encoding="utf-8", newline="\n")
                self.alive = True
                self.reader_thread = threading.Thread(target=self._reader, daemon=True)
                self.reader_thread.start()
                ART.event("node_connected", node=self.name, port=self.port)
                console(f"[node:{self.name}] connected on port {self.port}")
                return
            except Exception as e:
                last_err = e
                time.sleep(sleep_s)
        raise RuntimeError(f"failed to connect to {self.name} on port {self.port}: {last_err!r}")

    def _reader(self):
        try:
            while self.alive:
                line = self.rfile.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    self.router.handle_from_node(self.name, json.loads(line))
                except Exception as e:
                    ART.event("bad_json_from_node", node=self.name, error=repr(e), raw=line)
        finally:
            self.alive = False
            ART.event("node_reader_stopped", node=self.name)

    def send(self, msg: dict):
        with self.lock:
            if not self.alive or self.wfile is None:
                raise RuntimeError(f"{self.name} is not connected")
            self.wfile.write(json.dumps(msg) + "\n")
            self.wfile.flush()

    def close(self):
        self.alive = False
        for obj in (self.wfile, self.rfile, self.sock):
            try:
                if obj:
                    obj.close()
            except Exception:
                pass
        self.wfile = None
        self.rfile = None
        self.sock = None
        ART.event("node_closed", node=self.name)


class Router:
    def __init__(self):
        self.nodes: Dict[str, NodeConn] = {}
        self.pending: Dict[Tuple[str, int], queue.Queue] = {}
        self.pending_lock = threading.Lock()
        self.msg_id = 1
        self.msg_id_lock = threading.Lock()

    def register_node(self, node_conn: NodeConn):
        self.nodes[node_conn.name] = node_conn

    def next_msg_id(self) -> int:
        with self.msg_id_lock:
            mid = self.msg_id
            self.msg_id += 1
            return mid

    def handle_from_node(self, src_node: str, msg: dict):
        dest = msg.get("dest")
        body = msg.get("body", {})

        if dest in self.nodes:
            if PARTITION.allowed(src_node, dest):
                target = self.nodes[dest]
                if target.alive:
                    try:
                        target.send(msg)
                    except Exception as e:
                        ART.event("forward_failed", src=src_node, dest=dest, error=repr(e))
            else:
                ART.event("partition_drop", src=src_node, dest=dest, body_type=body.get("type"))
            return

        if dest and dest.startswith("c"):
            key = (dest, body.get("in_reply_to"))
            with self.pending_lock:
                q = self.pending.get(key)
            if q is not None:
                q.put(msg)
            return

    def rpc(self, node: str, client: str, msg_type: str, timeout_s=5.0, **fields):
        msg_id = self.next_msg_id()
        q = queue.Queue()
        key = (client, msg_id)

        msg = {
            "src": client,
            "dest": node,
            "body": {
                "type": msg_type,
                "msg_id": msg_id,
                **fields,
            },
        }

        with self.pending_lock:
            self.pending[key] = q

        try:
            nc = self.nodes[node]
            nc.send(msg)
            return q.get(timeout=timeout_s)
        finally:
            with self.pending_lock:
                self.pending.pop(key, None)

    def init_node(self, node: str):
        reply = self.rpc(
            node=node,
            client=f"c-init-{node}",
            msg_type="init",
            timeout_s=8.0,
            node_id=node,
            node_ids=ALL_NODES,
        )
        if body_type(reply) != "init_ok":
            raise RuntimeError(f"init failed for {node}: {reply!r}")
        console(f"[init] {node} ok")

    def write(self, node: str, key: int, value: int, client: str):
        return self.rpc(node=node, client=client, msg_type="write", timeout_s=5.0, key=key, value=value)

    def read(self, node: str, key: int, client: str):
        return self.rpc(node=node, client=client, msg_type="read", timeout_s=5.0, key=key)

    def cas(self, node: str, key: int, from_v: int, to_v: int, client: str):
        return self.rpc(node=node, client=client, msg_type="cas", timeout_s=5.0, key=key, **{"from": from_v, "to": to_v})


ROUTER = Router()


def body_type(msg: dict) -> Optional[str]:
    return msg.get("body", {}).get("type")


def body_code(msg: dict) -> Optional[int]:
    return msg.get("body", {}).get("code")


def maybe_find_leader(router: Router) -> str:
    probe_key = PROBE_KEY_BASE + (int(time.time() * 1000) % 10000)
    for node in ALL_NODES:
        nc = router.nodes.get(node)
        if not nc or not nc.alive:
            continue
        try:
            r = router.write(node, probe_key, 1, client=f"c-probe-{node}")
            if body_type(r) == "write_ok":
                ART.event("leader_detected", node=node)
                return node
        except Exception as e:
            ART.event("leader_probe_failed", node=node, error=repr(e))
    raise RuntimeError("could not find leader")


def find_leader_with_retry(router: Router, attempts=20, sleep_s=0.7) -> str:
    last_err = None
    for _ in range(attempts):
        try:
            return maybe_find_leader(router)
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    raise RuntimeError(f"could not find leader after retries: {last_err!r}")


def wait_for_port(port: int, timeout_s: float = 30.0):
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1.0)
            s.close()
            return
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    raise RuntimeError(f"port {port} not ready after {timeout_s}s: {last_err!r}")


def init_with_retry(router: Router, node: str, attempts: int = 20, sleep_s: float = 1.0):
    last_err = None
    for _ in range(attempts):
        try:
            nc = router.nodes[node]
            if not nc.alive:
                nc.connect(retries=10, sleep_s=0.5)
            router.init_node(node)
            time.sleep(0.5)
            return
        except Exception as e:
            last_err = e
            ART.event("init_retry", node=node, error=repr(e))
            time.sleep(sleep_s)
    raise RuntimeError(f"init_with_retry failed for {node}: {last_err!r}")


def docker_kill(node: str):
    cname = f"omnipaxos-{node}"
    ART.event("docker_kill", node=node, container=cname)
    sh(["docker", "kill", cname], check=True)
    time.sleep(1.5)


def docker_resume(node: str):
    print(f"[debug] docker_resume start: {node}")
    ART.event("docker_resume", node=node)
    sh(["docker", "rm", "-f", CONTAINER_NAMES[node]], check=False)

    env = os.environ.copy()
    env["OMNIPAXOS_RECOVER"] = "1"
    sh(COMPOSE + ["up", "-d", node], check=True, env=env)

    print(f"[debug] docker_resume up-done: {node}, waiting port {NODE_PORTS[node]}")
    wait_for_port(NODE_PORTS[node], timeout_s=30.0)
    print(f"[debug] docker_resume port-ready: {node}")
    time.sleep(0.5)

def connect_and_init_all(router: Router):
    for n, p in NODE_PORTS.items():
        nc = NodeConn(n, p, router)
        router.register_node(nc)
        nc.connect()
    time.sleep(1.0)
    for n in ALL_NODES:
        init_with_retry(router, n, attempts=10, sleep_s=1.0)
    time.sleep(1.0)

def reconnect_and_init(router: Router, node: str):
    print(f"[debug] reconnect_and_init start: {node}")
    old = router.nodes[node]
    old.close()

    wait_for_port(NODE_PORTS[node], timeout_s=30.0)
    print(f"[debug] reconnect_and_init port confirmed: {node}")

    nc = NodeConn(node, NODE_PORTS[node], router)
    router.register_node(nc)

    last_err = None
    for attempt in range(20):
        try:
            print(f"[debug] reconnect_and_init attempt {attempt+1}: {node}")
            if not nc.alive:
                nc.connect(retries=10, sleep_s=0.5)
            time.sleep(0.5)
            router.init_node(node)
            time.sleep(1.0)
            ART.event("reconnect_ok", node=node)
            print(f"[debug] reconnect_and_init ok: {node}")
            return
        except Exception as e:
            last_err = e
            ART.event("reconnect_retry", node=node, error=repr(e))
            print(f"[debug] reconnect_and_init retry: {node} error={e!r}")
            time.sleep(1.0)

    raise RuntimeError(f"reconnect_and_init failed for {node}: {last_err!r}")


def reset_and_start():
    sh(COMPOSE + ["down", "-v"], check=False)
    sh(COMPOSE + ["build"], check=True)

    env = os.environ.copy()
    env["OMNIPAXOS_RECOVER"] = "0"
    sh(COMPOSE + ["up", "-d"], check=True, env=env)

    for n in ALL_NODES:
        wait_for_port(NODE_PORTS[n], timeout_s=30.0)
    time.sleep(1.0)


class Nemesis(threading.Thread):
    def __init__(self, router: Router, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.router = router
        self.stop_event = stop_event
        self.killed = set()
        self.restart_count = 0
        self.covered_leader_crash = False
        self.covered_follower_crash = False
        # self.covered_n3_crash = False
        self.covered_crash_nodes = set()

    def run(self):
        while not self.stop_event.wait(NEMESIS_INTERVAL_S):
            try:
                self.step()
            except Exception as e:
                ART.event("nemesis_error", error=repr(e))
                if VERBOSE_STDOUT:
                    print(f"[nemesis] ERROR: {e!r}")
                    traceback.print_exc()

    def crash_and_restart(self, victim: str, down_time: float = 4.0):
        with NODE_LOCKS[victim]:
            docker_kill(victim)
            self.router.nodes[victim].close()
            self.killed.add(victim)
            time.sleep(down_time)
            docker_resume(victim)
            reconnect_and_init(self.router, victim)
            self.killed.discard(victim)
            self.restart_count += 1

    def choose_action(self) -> str:
        if not self.covered_leader_crash:
            return "kill_leader"
        if not self.covered_follower_crash:
            return "kill_follower"
        if len(self.covered_crash_nodes) < len(ALL_NODES):
            return "kill_uncovered"
        return RNG.choice(["isolate_leader", "split", "kill_follower", "kill_leader"])

    def step(self):
        if self.covered_leader_crash and self.covered_follower_crash and len(self.covered_crash_nodes) == len(ALL_NODES) and RNG.random() < 0.30:
            PARTITION.clear()
            payload = {"ts": time.time(), "action": "heal_partition"}
            ART.append_nemesis(payload)
            ART.event("nemesis_action", **payload)
            return

        action = self.choose_action()
        print(f"[debug] nemesis chose action={action}")
        payload = {"ts": time.time(), "action": action}

        if action == "isolate_leader":
            leader = find_leader_with_retry(self.router, attempts=8, sleep_s=0.5)
            PARTITION.clear()
            PARTITION.isolate(leader)
            payload["leader"] = leader
            payload["role"] = "leader"

        elif action == "split":
            PARTITION.clear()
            chosen = list(ALL_NODES)
            RNG.shuffle(chosen)

            # left = chosen[:1]
            # right = chosen[1:]

            mid = len(chosen) // 2
            left = chosen[:mid]
            right = chosen[mid:]

            PARTITION.split(left, right)
            payload["left"] = left
            payload["right"] = right

        elif action == "kill_follower":
            leader = find_leader_with_retry(self.router, attempts=8, sleep_s=0.5)
            candidates = [n for n in ALL_NODES if n != leader and n not in self.killed]
            if not candidates:
                return
            victim = RNG.choice(candidates)
            self.crash_and_restart(victim)
            payload["victim"] = victim
            payload["restarted"] = True
            payload["leader_at_time"] = leader
            payload["role"] = "follower"
            self.covered_follower_crash = True
            self.covered_crash_nodes.add(victim)

        elif action == "kill_leader":
            leader = find_leader_with_retry(self.router, attempts=8, sleep_s=0.5)
            if leader in self.killed:
                return
            self.crash_and_restart(leader)
            payload["victim"] = leader
            victim = leader
            payload["restarted"] = True
            payload["role"] = "leader"
            payload["leader_at_time"] = leader
            self.covered_leader_crash = True
            self.covered_crash_nodes.add(victim)

        elif action == "kill_n3":
            leader = find_leader_with_retry(self.router, attempts=8, sleep_s=0.5)
            victim = "n3"
            if victim in self.killed:
                return
            self.crash_and_restart(victim)
            payload["victim"] = victim
            payload["restarted"] = True
            payload["leader_at_time"] = leader
            payload["role"] = "forced_n3"
            # self.covered_n3_crash = True
            self.covered_crash_nodes.add(victim)
            
        elif action == "kill_uncovered":
            leader = find_leader_with_retry(self.router, attempts=8, sleep_s=0.5)
            candidates = [
                n for n in ALL_NODES
                if n not in self.killed and n not in self.covered_crash_nodes
            ]
            if not candidates:
                return
            # victim = RNG.choice(candidates)
            victim = sorted(candidates)[0]
            self.crash_and_restart(victim)
            payload["victim"] = victim
            payload["restarted"] = True
            payload["role"] = "uncovered"
            payload["leader_at_time"] = leader
            self.covered_crash_nodes.add(victim)
            
        ART.append_nemesis(payload)
        ART.event("nemesis_action", **payload)

    def cleanup(self):
        PARTITION.clear()
        for node in sorted(list(self.killed)):
            with NODE_LOCKS[node]:
                try:
                    docker_resume(node)
                    reconnect_and_init(self.router, node)
                    self.restart_count += 1
                except Exception as e:
                    ART.event("nemesis_cleanup_failed", node=node, error=repr(e))
        self.killed.clear()


class Worker(threading.Thread):
    def __init__(self, worker_id: int, router: Router, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.router = router
        self.stop_event = stop_event
        self.process = f"c{worker_id}"
        self.rand = random.Random(SEED * 1000 + worker_id)
        self.last_leader: Optional[str] = None
        self.last_leader_ts: float = 0.0

    def choose_node(self) -> str:
        now = time.time()
        if self.last_leader and (now - self.last_leader_ts) < 1.0:
            nc = self.router.nodes.get(self.last_leader)
            if nc and nc.alive:
                return self.last_leader
        try:
            leader = maybe_find_leader(self.router)
            self.last_leader = leader
            self.last_leader_ts = now
            return leader
        except Exception:
            alive = [n for n in ALL_NODES if self.router.nodes.get(n) and self.router.nodes[n].alive]
            if not alive:
                raise RuntimeError("no alive node connection")
            return self.rand.choice(alive)

    def choose_key(self) -> int:
        return self.rand.randint(1, KEYSPACE)

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.one_op()
            except Exception as e:
                ART.event("worker_loop_error", worker=self.worker_id, error=repr(e))
                time.sleep(0.1)

    def one_op(self):
        node = self.choose_node()
        key = self.choose_key()
        op_kind = self.rand.choice(["read", "write", "cas"])

        start_wall = time.time()
        start_mono = time.monotonic()

        record = {
            "process": self.process,
            "node": node,
            "key": key,
            "type": op_kind,
            "start": start_mono,
            "start_wall": start_wall,
        }

        try:
            if op_kind == "read":
                resp = self.router.read(node, key, self.process)
            elif op_kind == "write":
                value = self.rand.randint(1, 100000)
                record["value"] = value
                resp = self.router.write(node, key, value, self.process)
            else:
                from_v = self.rand.randint(1, 10)
                to_v = self.rand.randint(11, 20)
                record["from"] = from_v
                record["to"] = to_v
                resp = self.router.cas(node, key, from_v, to_v, self.process)

            end_wall = time.time()
            end_mono = time.monotonic()
            record["end"] = end_mono
            record["end_wall"] = end_wall
            record["response"] = resp
            ART.append_history(record)

        except queue.Empty:
            end_wall = time.time()
            end_mono = time.monotonic()
            record["end"] = end_mono
            record["end_wall"] = end_wall
            record["response"] = {"body": {"type": "timeout"}}
            ART.append_history(record)

        except Exception as e:
            end_wall = time.time()
            end_mono = time.monotonic()
            record["end"] = end_mono
            record["end_wall"] = end_wall
            record["response"] = {"body": {"type": "exception", "text": repr(e)}}
            ART.append_history(record)


def op_ok_type(rec: dict) -> Optional[str]:
    return rec.get("response", {}).get("body", {}).get("type")


def validate_history_timestamps(history: List[dict]):
    bad = []
    for idx, rec in enumerate(history):
        s = rec.get("start")
        e = rec.get("end")
        if s is None or e is None:
            bad.append({"idx": idx, "reason": "missing start/end", "process": rec.get("process"), "node": rec.get("node"), "key": rec.get("key"), "type": rec.get("type")})
            continue
        if e < s:
            bad.append({"idx": idx, "reason": "end_before_start", "start": s, "end": e, "process": rec.get("process"), "node": rec.get("node"), "key": rec.get("key"), "type": rec.get("type")})
    if bad:
        sample = json.dumps(bad[:10], ensure_ascii=False, indent=2)
        raise RuntimeError(f"invalid history timestamps detected, sample:\n{sample}")


@dataclass(frozen=True)
class HistOp:
    idx: int
    process: str
    node: str
    key: int
    kind: str
    start: float
    end: float
    status: str
    input_data: Tuple[Tuple[str, Any], ...]
    output_data: Tuple[Tuple[str, Any], ...]
    raw: Any

    def input_dict(self) -> dict:
        return dict(self.input_data)

    def output_dict(self) -> dict:
        return dict(self.output_data)


def record_input_fields(rec: dict) -> dict:
    if rec["type"] == "write":
        return {"value": rec["value"]}
    if rec["type"] == "read":
        return {}
    if rec["type"] == "cas":
        return {"from": rec["from"], "to": rec["to"]}
    return {}


def normalize_record(idx: int, rec: dict) -> HistOp:
    body = rec.get("response", {}).get("body", {})
    t = body.get("type")
    if t in {"write_ok", "read_ok", "cas_ok"}:
        status = "ok"
    elif t == "error":
        status = "fail"
    elif t in {"timeout", "exception"}:
        status = "unknown"
    else:
        status = "info"

    start_ts = rec.get("start_mono", rec.get("start", 0.0))
    end_ts = rec.get("end_mono", rec.get("end", start_ts))

    return HistOp(
        idx=idx,
        process=rec.get("process", "?"),
        node=rec.get("node", "?"),
        key=rec.get("key"),
        kind=rec.get("type"),
        start=float(start_ts),
        end=float(end_ts),
        status=status,
        input_data=tuple(sorted(record_input_fields(rec).items())),
        output_data=tuple(sorted(body.items())),
        raw=rec,
    )


def state_key(state):
    if state is MISSING:
        return ("MISSING", None)
    return ("VAL", state)


def unstate_key(sk):
    return MISSING if sk[0] == "MISSING" else sk[1]


def op_short(op: HistOp) -> str:
    return f"#{op.idx}:{op.process}:{op.kind}@k{op.key} [{op.status}] in={dict(op.input_data)} out={dict(op.output_data)} [{op.start:.6f},{op.end:.6f}]"


def build_precedence(ops: List[HistOp]) -> List[set]:
    preds = [set() for _ in range(len(ops))]
    for i in range(len(ops)):
        for j in range(len(ops)):
            if i != j and ops[i].end < ops[j].start:
                preds[j].add(i)
    return preds


def assert_acyclic(preds, ops):
    n = len(preds)
    color = [0] * n
    def dfs(u):
        color[u] = 1
        for v in preds[u]:
            if color[v] == 1:
                raise RuntimeError(f"precedence cycle detected: {op_short(ops[v])} -> ... -> {op_short(ops[u])}")
            if color[v] == 0:
                dfs(v)
        color[u] = 2
    for i in range(n):
        if color[i] == 0:
            dfs(i)


def known_step_variants(state, op: HistOp) -> List[Tuple[str, bool, Any]]:
    inp = op.input_dict()
    out = op.output_dict()
    out_type = out.get("type")
    code = out.get("code")

    if op.kind == "write":
        if op.status == "ok" and out_type == "write_ok":
            return [("commit_write_ok", True, inp["value"])]
        return [("invalid_completed_write", False, state)]

    if op.kind == "read":
        if op.status == "ok" and out_type == "read_ok":
            # lin-kv 的 read_ok 必须带 value；value=None 表示 key 当前不存在
            if "value" not in out:
                return [("read_ok_missing_value_field", False, state)]

            got = out.get("value")

            if state is MISSING:
                # 空状态下，只有读到 null/None 才合法
                return [("read_ok_null_on_missing", got is None, state)]

            # 非空状态下，必须读到当前值
            return [("read_ok_match", got == state, state)]

        if op.status == "fail" and out_type == "error":
            if code == 20:
                return [("read_not_found", state is MISSING, state)]
            return [(f"read_error_code_{code}", False, state)]

        return [("invalid_completed_read", False, state)]

    if op.kind == "cas":
        from_v = inp["from"]
        to_v = inp["to"]
        if op.status == "ok" and out_type == "cas_ok":
            if state == from_v:
                return [("cas_ok", True, to_v)]
            return [("cas_ok_but_state_mismatch", False, state)]
        if op.status == "fail" and out_type == "error":
            if code == 22:
                return [("cas_compare_fail", state != from_v, state)]
            if code == 20:
                return [("cas_not_found", state is MISSING, state)]
            return [(f"cas_error_code_{code}", False, state)]
        return [("invalid_completed_cas", False, state)]

    return [("unknown_op_kind", False, state)]


def unknown_step_variants(state, op: HistOp) -> List[Tuple[str, bool, Any, bool]]:
    inp = op.input_dict()
    variants = [("skip_unknown", True, state, False)]
    if op.kind == "write":
        variants.append(("unknown_write_took_effect", True, inp["value"], True))
    elif op.kind == "read":
        variants.append(("unknown_read_linearized", True, state, True))
    elif op.kind == "cas":
        variants.append(("unknown_cas_failed", True, state, True))
        if state == inp["from"]:
            variants.append(("unknown_cas_succeeded", True, inp["to"], True))
    return variants


def deterministic_candidate_order(ops: List[HistOp], candidates: List[int]) -> List[int]:
    return sorted(candidates, key=lambda i: (ops[i].end, ops[i].start, ops[i].idx))


def extract_ops_for_key(history: List[dict], key: int) -> List[HistOp]:
    return [normalize_record(idx, rec) for idx, rec in enumerate(history) if rec.get("key") == key]


def check_linearizable_full(history: List[dict], key: int, max_witness_steps: int = 200) -> dict:
    ops = extract_ops_for_key(history, key)
    preds = build_precedence(ops)
    assert_acyclic(preds, ops)
    n = len(ops)
    all_mask = (1 << n) - 1
    memo: Dict[Tuple[int, Tuple[Any, Any]], Tuple[bool, Optional[List[dict]], str]] = {}

    def preds_satisfied(mask: int, i: int) -> bool:
        return all(mask & (1 << p) for p in preds[i])

    def candidate_indices(mask: int) -> List[int]:
        return deterministic_candidate_order(ops, [i for i in range(n) if not (mask & (1 << i)) and preds_satisfied(mask, i)])

    def mk_step(op: HistOp, decision: str, linearized: bool, before_state, after_state) -> dict:
        return {
            "idx": op.idx,
            "process": op.process,
            "node": op.node,
            "key": op.key,
            "type": op.kind,
            "status": op.status,
            "decision": decision,
            "linearized": linearized,
            "start": op.start,
            "end": op.end,
            "start_wall": op.raw.get("start_wall"),
            "end_wall": op.raw.get("end_wall"),
            "input": dict(op.input_data),
            "output": dict(op.output_data),
            "state_before": None if before_state is MISSING else before_state,
            "state_after": None if after_state is MISSING else after_state,
        }

    def dfs(mask: int, skey: Tuple[Any, Any]) -> Tuple[bool, Optional[List[dict]], str]:
        cache_key = (mask, skey)
        if cache_key in memo:
            return memo[cache_key]

        state = unstate_key(skey)
        if mask == all_mask:
            memo[cache_key] = (True, [], "ok")
            return memo[cache_key]

        cands = candidate_indices(mask)
        if not cands:
            blocked = []
            for i in range(n):
                if mask & (1 << i):
                    continue
                missing_preds = [p for p in preds[i] if not (mask & (1 << p))]
                blocked.append({"op": op_short(ops[i]), "missing_preds": [op_short(ops[p]) for p in missing_preds]})
            reason = {"error": "no eligible candidate but history unresolved", "done_mask": mask, "remaining": blocked[:20]}
            memo[cache_key] = (False, None, json.dumps(reason, ensure_ascii=False))
            return memo[cache_key]

        first_reason = None
        for i in cands:
            op = ops[i]
            if op.status in {"ok", "fail"}:
                variants = [(d, a, ns, True) for d, a, ns in known_step_variants(state, op)]
            elif op.status == "unknown":
                variants = unknown_step_variants(state, op)
            else:
                variants = [("skip_info_op", True, state, False)]

            any_allowed = False
            for decision, allowed, new_state, linearized in variants:
                if not allowed:
                    if first_reason is None:
                        first_reason = f"{op_short(op)} rejected by {decision} on state={None if state is MISSING else state}"
                    continue
                any_allowed = True
                step = mk_step(op, decision, linearized, state, new_state)
                ok, suffix, reason = dfs(mask | (1 << i), state_key(new_state))
                if ok:
                    path = [step] + (suffix or [])
                    memo[cache_key] = (True, path[:max_witness_steps], "ok")
                    return memo[cache_key]
                if first_reason is None and reason:
                    first_reason = reason

            if not any_allowed and first_reason is None:
                first_reason = f"{op_short(op)} had no allowed variant"

        memo[cache_key] = (False, None, first_reason or "search exhausted")
        return memo[cache_key]

    ok, witness, reason = dfs(0, state_key(MISSING))
    return {
        "linearizable": ok,
        "ops_total": n,
        "ops_completed": sum(1 for op in ops if op.status in {"ok", "fail"}),
        "ops_unknown": sum(1 for op in ops if op.status == "unknown"),
        "witness_len": len(witness or []),
        "linearized_steps": sum(1 for s in (witness or []) if s["linearized"]),
        "skipped_unknown_steps": sum(1 for s in (witness or []) if s["decision"] == "skip_unknown"),
        "witness": witness or [],
        "reason": reason,
    }


def build_recovery_summary() -> dict:
    summary = {
        "kills": {n: 0 for n in ALL_NODES},
        "resumes": {n: 0 for n in ALL_NODES},
        "reconnect_ok": {n: 0 for n in ALL_NODES},
        "partition_drops": {n: 0 for n in ALL_NODES},
        "leaders_detected": {n: 0 for n in ALL_NODES},
    }
    for ev in ART.events:
        kind = ev.get("kind")
        if kind == "docker_kill" and ev.get("node") in ALL_NODES:
            summary["kills"][ev["node"]] += 1
        elif kind == "docker_resume" and ev.get("node") in ALL_NODES:
            summary["resumes"][ev["node"]] += 1
        elif kind == "reconnect_ok" and ev.get("node") in ALL_NODES:
            summary["reconnect_ok"][ev["node"]] += 1
        elif kind == "partition_drop":
            for fld in ("src", "dest"):
                n = ev.get(fld)
                if n in ALL_NODES:
                    summary["partition_drops"][n] += 1
        elif kind == "leader_detected" and ev.get("node") in ALL_NODES:
            summary["leaders_detected"][ev["node"]] += 1
    summary["coverage"] = {f"{n}_crash_tested": summary["kills"][n] > 0 for n in ALL_NODES}
    summary["all_nodes_crash_tested"] = all(summary["coverage"].values())
    return summary


def _fmt_ts(x):
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        return None


def detect_log_recovery_mode(log_text: str) -> dict:
    low = log_text.lower()
    return {"fresh_start": "fresh start" in low, "restored_from_storage": "restored paxos storage" in low}


def collect_node_log_modes() -> dict:
    out = {}
    for node in ALL_NODES:
        path = ART.root / f"docker-{node}.log"
        if path.exists():
            out[node] = detect_log_recovery_mode(path.read_text(encoding="utf-8", errors="replace"))
        else:
            out[node] = {"fresh_start": False, "restored_from_storage": False}
    return out


def build_recovery_report(history: List[dict], analysis: dict) -> dict:
    per_node = {
        n: {
            "kills": [], "resumes": [], "reconnect_ok": [], "node_connected": [], "node_closed": [],
            "leader_detected": [], "partition_drop_count": 0,
            "operations_before_first_kill": 0, "operations_after_last_reconnect": 0, "operations_total": 0,
            "first_op_after_last_reconnect": None, "last_op_before_first_kill": None,
            "recovered_after_crash": False, "served_requests_after_recovery": False,
            "caught_up_after_recovery": None,
        }
        for n in ALL_NODES
    }

    for ev in ART.events:
        kind = ev.get("kind")
        node = ev.get("node")
        if kind == "docker_kill" and node in per_node:
            per_node[node]["kills"].append(ev["ts"])
        elif kind == "docker_resume" and node in per_node:
            per_node[node]["resumes"].append(ev["ts"])
        elif kind == "reconnect_ok" and node in per_node:
            per_node[node]["reconnect_ok"].append(ev["ts"])
        elif kind == "node_connected" and node in per_node:
            per_node[node]["node_connected"].append(ev["ts"])
        elif kind == "node_closed" and node in per_node:
            per_node[node]["node_closed"].append(ev["ts"])
        elif kind == "leader_detected" and node in per_node:
            per_node[node]["leader_detected"].append(ev["ts"])
        elif kind == "partition_drop":
            for fld in ("src", "dest"):
                n = ev.get(fld)
                if n in per_node:
                    per_node[n]["partition_drop_count"] += 1

    for rec in history:
        node = rec.get("node")
        if node not in per_node:
            continue
        op_ts = _fmt_ts(rec.get("end_wall")) or _fmt_ts(rec.get("start_wall"))
        per_node[node]["operations_total"] += 1
        kills = per_node[node]["kills"]
        reconnects = per_node[node]["reconnect_ok"]
        if kills:
            first_kill = min(kills)
            if op_ts is not None and op_ts < first_kill:
                per_node[node]["operations_before_first_kill"] += 1
                last_prev = per_node[node]["last_op_before_first_kill"]
                if last_prev is None or op_ts > last_prev["ts"]:
                    per_node[node]["last_op_before_first_kill"] = {
                        "ts": op_ts, "type": rec.get("type"), "key": rec.get("key"),
                        "process": rec.get("process"), "response_type": rec.get("response", {}).get("body", {}).get("type"),
                    }
        if reconnects:
            last_reconnect = max(reconnects)
            if op_ts is not None and op_ts > last_reconnect:
                per_node[node]["operations_after_last_reconnect"] += 1
                if per_node[node]["first_op_after_last_reconnect"] is None:
                    per_node[node]["first_op_after_last_reconnect"] = {
                        "ts": op_ts, "type": rec.get("type"), "key": rec.get("key"),
                        "process": rec.get("process"), "response_type": rec.get("response", {}).get("body", {}).get("type"),
                    }

    global_linearizable = bool(analysis.get("full linearizable with unknown handling"))
    log_modes = collect_node_log_modes()

    for node, info in per_node.items():
        if info["kills"]:
            info["recovered_after_crash"] = len(info["reconnect_ok"]) > 0
            info["served_requests_after_recovery"] = info["operations_after_last_reconnect"] > 0
            info["caught_up_after_recovery"] = bool(
                info["recovered_after_crash"]
                and info["served_requests_after_recovery"]
                and global_linearizable
                and log_modes.get(node, {}).get("restored_from_storage", False)
            )
        else:
            info["recovered_after_crash"] = None
            info["served_requests_after_recovery"] = None
            info["caught_up_after_recovery"] = None

    crashed_nodes = [n for n in ALL_NODES if per_node[n]["kills"]]
    recovered_nodes = [n for n in crashed_nodes if per_node[n]["recovered_after_crash"]]
    served_after_recovery_nodes = [n for n in crashed_nodes if per_node[n]["served_requests_after_recovery"]]
    caught_up_after_recovery_nodes = [n for n in crashed_nodes if per_node[n]["caught_up_after_recovery"]]

    return {
        "goal": {
            "persistent_storage_configured": True,
            "crash_restart_injected_under_load": len(crashed_nodes) > 0,
            "cluster_linearizable_during_run": analysis.get("full linearizable with unknown handling"),
        },
        "summary": {
            "crashed_nodes": crashed_nodes,
            "recovered_nodes": recovered_nodes,
            "served_after_recovery_nodes": served_after_recovery_nodes,
            "caught_up_after_recovery_nodes": caught_up_after_recovery_nodes,
            "all_crashed_nodes_recovered": set(recovered_nodes) == set(crashed_nodes),
            "all_crashed_nodes_served_requests_after_recovery": set(served_after_recovery_nodes) == set(crashed_nodes),
            "all_crashed_nodes_caught_up_after_recovery": set(caught_up_after_recovery_nodes) == set(crashed_nodes),
            "per_key_linearizable": {k: v.get("linearizable") for k, v in analysis.get("per-key", {}).items()},
        },
        "log_recovery_mode": log_modes,
        "nodes": per_node,
        "notes": [
            "No committed-data-loss proof can be derived from local container logs alone; this report uses externally observed operation history plus linearizability checking.",
            "If global linearizability is true and recovered nodes resume serving requests, that is strong evidence that committed client-visible data was not lost across restart.",
            "For stronger recovery evidence, inspect docker-<node>.log together with history.json and node_actions.json.",
        ],
    }


def summarize(history: List[dict]):
    validate_history_timestamps(history)
    completed = indeterminate = successful = definite_fail = 0
    by_type = {}
    for rec in history:
        t = op_ok_type(rec)
        by_type[t] = by_type.get(t, 0) + 1
        if t in {"write_ok", "read_ok", "cas_ok", "error"}:
            completed += 1
        if t in {"timeout", "exception"}:
            indeterminate += 1
        if t in {"write_ok", "read_ok", "cas_ok"}:
            successful += 1
        if t == "error" and rec["response"]["body"].get("code") in {20, 22}:
            definite_fail += 1

    per_key = {}
    overall = True
    total_checked = total_unknown = 0
    for k in range(1, KEYSPACE + 1):
        ops_for_key = [rec for rec in history if rec.get("key") == k]

        if len(ops_for_key) > 1500:
            r = {
                "linearizable": False,
                "ops_total": len(ops_for_key),
                "ops_completed": 0,
                "ops_unknown": 0,
                "witness_len": 0,
                "linearized_steps": 0,
                "skipped_unknown_steps": 0,
                "witness": [],
                "reason": f"too many ops for recursive checker: {len(ops_for_key)}",
            }
        else:
            r = check_linearizable_full(history, k)

        overall = overall and r["linearizable"]
        total_checked += r["ops_total"]
        total_unknown += r["ops_unknown"]
        per_key[str(k)] = r

    return {
        "full linearizable with unknown handling": overall,
        "checked ops in model": total_checked,
        "unknown ops in model": total_unknown,
        "checked completed ops": completed,
        "indeterminate ops": indeterminate,
        "successful ops": successful,
        "definite fail ops": definite_fail,
        "response_type_histogram": by_type,
        "recovery_summary": build_recovery_summary(),
        "per-key": per_key,
    }


def docker_logs(node: str) -> str:
    try:
        cp = subprocess.run(["docker", "logs", CONTAINER_NAMES[node]], check=False, capture_output=True, text=True)
        return cp.stdout + cp.stderr
    except Exception as e:
        return f"<failed to read logs for {node}: {e!r}>"


def main():
    print(f"[test] running workload for {WORKLOAD_SECONDS:.1f}s with {CLIENTS} clients over {KEYSPACE} keys")
    reset_and_start()
    connect_and_init_all(ROUTER)

    stop_event = threading.Event()
    workers = [Worker(i, ROUTER, stop_event) for i in range(CLIENTS)]
    nemesis = Nemesis(ROUTER, stop_event)

    for w in workers:
        w.start()
    nemesis.start()

    time.sleep(WORKLOAD_SECONDS)
    stop_event.set()

    for w in workers:
        w.join(timeout=3.0)

    nemesis.join(timeout=3.0)
    nemesis.cleanup()
    

    result = summarize(ART.history)
    ART.set_analysis(result)

    for node in ALL_NODES:
        log_text = docker_logs(node)
        log_path = ART.root / f"docker-{node}.log"
        log_path.write_text(log_text, encoding="utf-8")
        ART.set_docker_log_file(node, str(log_path))

    recovery_report = build_recovery_report(ART.history, result)
    ART.set_recovery_report(recovery_report)
    ART.flush()

    print("\n===== RESULT =====")
    print(f"full linearizable with unknown handling: {result['full linearizable with unknown handling']}")
    print(f"checked ops in model: {result['checked ops in model']}")
    print(f"unknown ops in model: {result['unknown ops in model']}")
    print(f"checked completed ops: {result['checked completed ops']}")
    print(f"indeterminate ops: {result['indeterminate ops']}")
    print(f"successful ops: {result['successful ops']}")
    print(f"definite fail ops: {result['definite fail ops']}")

    print("\nPer-key summary:")
    for key, detail in result["per-key"].items():
        print(f"  key={key} linearizable={detail['linearizable']} ops_total={detail['ops_total']} unknown={detail['ops_unknown']} reason={detail['reason']}")

    print("\nRecovery summary:")
    for k, v in result["recovery_summary"].items():
        print(f"  {k}: {v}")

    print("\nRecovery verification:")
    rr = recovery_report
    print(f"  persistent_storage_configured: {rr['goal']['persistent_storage_configured']}")
    print(f"  crash_restart_injected_under_load: {rr['goal']['crash_restart_injected_under_load']}")
    print(f"  cluster_linearizable_during_run: {rr['goal']['cluster_linearizable_during_run']}")
    print(f"  crashed_nodes: {rr['summary']['crashed_nodes']}")
    print(f"  recovered_nodes: {rr['summary']['recovered_nodes']}")
    print(f"  served_after_recovery_nodes: {rr['summary']['served_after_recovery_nodes']}")
    print(f"  caught_up_after_recovery_nodes: {rr['summary']['caught_up_after_recovery_nodes']}")
    print(f"  all_crashed_nodes_recovered: {rr['summary']['all_crashed_nodes_recovered']}")
    print(f"  all_crashed_nodes_served_requests_after_recovery: {rr['summary']['all_crashed_nodes_served_requests_after_recovery']}")
    print(f"  all_crashed_nodes_caught_up_after_recovery: {rr['summary']['all_crashed_nodes_caught_up_after_recovery']}")
    print(f"  all_nodes_crash_tested: {result['recovery_summary']['all_nodes_crash_tested']}")
    print(f"  coverage: {result['recovery_summary']['coverage']}")
    print("  log_recovery_mode:")
    for node, mode in rr['log_recovery_mode'].items():
        print(f"    {node}: {mode}")

    if PRINT_WITNESS_TO_STDOUT:
        for key, detail in result["per-key"].items():
            if detail["witness"]:
                print(f"\n----- witness key={key} -----")
                for step in detail["witness"]:
                    print(json.dumps(step, ensure_ascii=False))

    if PRINT_DOCKER_LOGS_TO_STDOUT:
        for node in ALL_NODES:
            print(f"\n========== logs {node} ==========")
            print(docker_logs(node))
    else:
        print("\nDocker logs saved:")
        for node in ALL_NODES:
            print(f"  {node}: {ART.root / f'docker-{node}.log'}")

    print(f"\nArtifacts written to: {ART.root}")


if __name__ == "__main__":
    main()
