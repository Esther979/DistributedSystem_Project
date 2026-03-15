#!/usr/bin/env python3
import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime

NODE_PORTS = {
    "n1": 7001,
    "n2": 7002,
    "n3": 7003,
}
ALL_NODES = ["n1", "n2", "n3"]


def compose_cmd():
    if subprocess.call(["docker", "compose", "version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0:
        return ["docker", "compose"]
    if subprocess.call(["docker-compose", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0:
        return ["docker-compose"]
    raise RuntimeError("neither docker compose nor docker-compose is available")


COMPOSE = compose_cmd()


class ArtifactStore:
    def __init__(self):
        ts = datetime.now().strftime("%Y%m%dT%H%M%S")
        self.base_dir = os.path.join("store", "bonus-direct", ts)
        self.logs_dir = os.path.join(self.base_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)

        self.history_path = os.path.join(self.base_dir, "history.jsonl")
        self.summary_path = os.path.join(self.base_dir, "summary.json")
        self.storage_path = os.path.join(self.base_dir, "storage.txt")

        self.history_lock = threading.Lock()
        self.summary = {
            "started_at": ts,
            "status": "running",
            "leader1": None,
            "follower": None,
            "leader_after_follower_recover": None,
            "old_leader": None,
            "new_leader": None,
            "final_leader": None,
            "error": None,
        }

    def event(self, event_type, **fields):
        rec = {
            "ts": time.time(),
            "event": event_type,
            **fields,
        }
        with self.history_lock:
            with open(self.history_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def set_summary(self, **fields):
        self.summary.update(fields)

    def finalize(self, ok: bool, error_text: str = None):
        self.summary["status"] = "passed" if ok else "failed"
        self.summary["finished_at"] = datetime.now().strftime("%Y%m%dT%H%M%S")
        self.summary["error"] = error_text
        with open(self.summary_path, "w", encoding="utf-8") as f:
            json.dump(self.summary, f, ensure_ascii=False, indent=2)

    def save_logs(self):
        for node in ALL_NODES:
            cname = f"omnipaxos-{node}"
            out = subprocess.run(
                ["docker", "logs", cname],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
            with open(os.path.join(self.logs_dir, f"{node}.log"), "w", encoding="utf-8") as f:
                f.write(out.stdout)

    def save_storage_listing(self):
        with open(self.storage_path, "w", encoding="utf-8") as f:
            for node in ALL_NODES:
                cname = f"omnipaxos-{node}"
                f.write(f"===== {node} =====\n")
                out = subprocess.run(
                    ["docker", "exec", cname, "sh", "-lc", "find /data -maxdepth 4 -type f | sort"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                )
                f.write(out.stdout)
                f.write("\n")


ART = ArtifactStore()


def sh(cmd, check=True):
    print("[cmd]", " ".join(cmd))
    ART.event("command", cmd=cmd)
    return subprocess.run(cmd, check=check)


def docker_kill(node):
    cname = f"omnipaxos-{node}"
    ART.event("docker_kill", node=node, container=cname)
    sh(["docker", "kill", cname], check=True)
    time.sleep(1.5)


def docker_resume(node):
    ART.event("docker_resume", node=node)
    sh(COMPOSE + ["up", "-d", node], check=True)
    time.sleep(2.0)


class NodeConn:
    def __init__(self, name, port, router):
        self.name = name
        self.port = port
        self.router = router
        self.sock = None
        self.rfile = None
        self.wfile = None
        self.reader_thread = None
        self.alive = False

    def connect(self, retries=50, sleep_s=0.4):
        for _ in range(retries):
            try:
                self.sock = socket.create_connection(("127.0.0.1", self.port), timeout=2.0)
                self.sock.settimeout(None)
                self.rfile = self.sock.makefile("r", encoding="utf-8", newline="\n")
                self.wfile = self.sock.makefile("w", encoding="utf-8", newline="\n")
                self.alive = True
                self.reader_thread = threading.Thread(target=self._reader, daemon=True)
                self.reader_thread.start()
                print(f"[node:{self.name}] connected on port {self.port}")
                ART.event("node_connected", node=self.name, port=self.port)
                return
            except Exception:
                time.sleep(sleep_s)
        raise RuntimeError(f"failed to connect to {self.name} on port {self.port}")

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
                    msg = json.loads(line)
                    self.router.handle_from_node(self.name, msg)
                except Exception as e:
                    ART.event("bad_json_from_node", node=self.name, raw=line, error=str(e))
        finally:
            self.alive = False

    def send(self, msg):
        if not self.alive or self.wfile is None:
            raise RuntimeError(f"{self.name} is not connected")
        self.wfile.write(json.dumps(msg) + "\n")
        self.wfile.flush()

    def close(self):
        self.alive = False
        try:
            if self.wfile:
                self.wfile.close()
        except Exception:
            pass
        try:
            if self.rfile:
                self.rfile.close()
        except Exception:
            pass
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
        self.wfile = None
        self.rfile = None
        self.sock = None
        ART.event("node_closed", node=self.name)


class Router:
    def __init__(self):
        self.nodes = {}
        self.msg_id = 1
        self.pending = {}
        self.pending_lock = threading.Lock()
        self.last_leader_printed = None

    def next_msg_id(self):
        mid = self.msg_id
        self.msg_id += 1
        return mid

    def register_node(self, node_conn):
        self.nodes[node_conn.name] = node_conn

    def handle_from_node(self, src_node, msg):
        dest = msg.get("dest")
        body = msg.get("body", {})

        if dest in self.nodes:
            target = self.nodes[dest]
            if target.alive:
                try:
                    target.send(msg)
                except Exception:
                    pass
            return

        if dest and dest.startswith("c"):
            in_reply_to = body.get("in_reply_to")
            key = (dest, in_reply_to)
            with self.pending_lock:
                q = self.pending.get(key)
            if q is not None:
                q.put(msg)
            return

        return

    def rpc(self, node, client, msg_type, **kwargs):
        mid = self.next_msg_id()
        q = queue.Queue(maxsize=1)
        key = (client, mid)
        with self.pending_lock:
            self.pending[key] = q
        try:
            msg = {
                "src": client,
                "dest": node,
                "body": {
                    "type": msg_type,
                    "msg_id": mid,
                    **kwargs,
                },
            }
            ART.event("rpc_send", node=node, client=client, msg_type=msg_type, body=msg["body"])
            self.nodes[node].send(msg)
            reply = q.get(timeout=8.0)
            ART.event("rpc_reply", node=node, client=client, in_reply_to=mid, reply=reply)
            return reply
        finally:
            with self.pending_lock:
                self.pending.pop(key, None)

    def init_node(self, node):
        client = f"c-init-{node}"
        reply = self.rpc(
            node=node,
            client=client,
            msg_type="init",
            node_id=node,
            node_ids=ALL_NODES,
        )
        body = reply["body"]
        if body.get("type") != "init_ok":
            raise RuntimeError(f"init failed for {node}: {reply}")
        print(f"[init] {node} ok")
        ART.event("init_ok", node=node)

    def write(self, node, key, value, client="c-write"):
        return self.rpc(
            node=node,
            client=client,
            msg_type="write",
            **{"key": key, "value": value},
        )

    def read(self, node, key, client="c-read"):
        return self.rpc(
            node=node,
            client=client,
            msg_type="read",
            **{"key": key},
        )

    def cas(self, node, key, from_v, to_v, client="c-cas"):
        return self.rpc(
            node=node,
            client=client,
            msg_type="cas",
            **{"key": key, "from": from_v, "to": to_v},
        )


def body_type(msg):
    return msg.get("body", {}).get("type")


def must_ok(msg, want_type):
    got = body_type(msg)
    if got != want_type:
        raise RuntimeError(f"expected {want_type}, got {json.dumps(msg, ensure_ascii=False)}")


def maybe_find_leader(router):
    probe_key = 900000 + int(time.time()) % 10000
    for node in ALL_NODES:
        if not router.nodes[node].alive:
            continue
        try:
            r = router.write(node, probe_key, 1, client=f"c-probe-{node}")
            if body_type(r) == "write_ok":
                if router.last_leader_printed != node:
                    print(f"[leader] detected leader = {node}")
                    router.last_leader_printed = node
                ART.event("leader_detected", node=node)
                return node
        except Exception as e:
            ART.event("leader_probe_failed", node=node, error=repr(e))
    raise RuntimeError("could not find leader")


def find_leader_with_retry(router, attempts=30, sleep_s=1.0):
    last_err = None
    for _ in range(attempts):
        try:
            return maybe_find_leader(router)
        except Exception as e:
            last_err = e
            time.sleep(sleep_s)
    raise RuntimeError(f"could not find leader after retries, last_err={last_err}")


def find_follower(leader):
    for n in ALL_NODES:
        if n != leader:
            return n
    raise RuntimeError("no follower candidate")


def read_expect_with_retry(router, key, expected, attempts=30, sleep_s=1.0):
    last_err = None
    for _ in range(attempts):
        try:
            leader = maybe_find_leader(router)
            r = router.read(leader, key, client=f"c-read-{key}")
            if body_type(r) != "read_ok":
                last_err = RuntimeError(f"read failed: {r}")
            else:
                got = r["body"].get("value")
                if got == expected:
                    ART.event("verify_read_ok", key=key, expected=expected, leader=leader)
                    return
                last_err = RuntimeError(f"bad value for key={key}: expected {expected}, got {got}")
        except Exception as e:
            last_err = e
        time.sleep(sleep_s)
    raise RuntimeError(f"read_expect_with_retry failed for key={key}, expected={expected}, last_err={last_err}")


def write_with_retry(router, key, value, attempts=30, sleep_s=1.0):
    last_err = None
    for _ in range(attempts):
        try:
            leader = maybe_find_leader(router)
            r = router.write(leader, key, value, client=f"c-write-{key}")
            if body_type(r) == "write_ok":
                ART.event("write_ok", key=key, value=value, leader=leader)
                return leader, r
            last_err = RuntimeError(f"write got non-ok reply: {r}")
        except Exception as e:
            last_err = e
        time.sleep(sleep_s)
    raise RuntimeError(f"write_with_retry failed for key={key}, value={value}, last_err={last_err}")


def connect_and_init_all(router):
    for n, p in NODE_PORTS.items():
        nc = NodeConn(n, p, router)
        router.register_node(nc)
        nc.connect()
    time.sleep(1.0)
    for n in ALL_NODES:
        router.init_node(n)
    time.sleep(2.0)


def reconnect_and_init(router, node):
    print(f"[reconnect] waiting for {node} to come back")
    old = router.nodes[node]
    old.close()

    nc = NodeConn(node, NODE_PORTS[node], router)
    router.register_node(nc)
    nc.connect(retries=80, sleep_s=0.5)

    time.sleep(1.0)
    router.init_node(node)
    time.sleep(2.0)

    print(f"[reconnect] {node} reconnected and init ok")
    ART.event("reconnect_ok", node=node)


def tail_logs_to_console():
    for node in ALL_NODES:
        cname = f"omnipaxos-{node}"
        print("\n========== logs", node, "==========")
        subprocess.run(["docker", "logs", "--tail", "120", cname], check=False)


def storage_to_console():
    for node in ALL_NODES:
        cname = f"omnipaxos-{node}"
        print("\n========== storage", node, "==========")
        subprocess.run(
            ["docker", "exec", cname, "sh", "-lc", "find /data -maxdepth 4 -type f | sort | head -100"],
            check=False,
        )


def reset_and_start():
    sh(COMPOSE + ["down", "-v"], check=False)
    sh(COMPOSE + ["build"], check=True)
    sh(COMPOSE + ["up", "-d"], check=True)
    time.sleep(3.0)


def main():
    reset_and_start()

    router = Router()
    connect_and_init_all(router)

    print("\n===== PHASE 1: follower crash / recover =====")
    leader1 = find_leader_with_retry(router)
    follower = find_follower(leader1)
    ART.set_summary(leader1=leader1, follower=follower)
    print(f"[test] leader1={leader1}, follower={follower}")

    print("[test] write initial keys 1..5")
    for i in range(1, 6):
        r = router.write(leader1, i, i * 10, client=f"c-w1-{i}")
        must_ok(r, "write_ok")

    print(f"[test] crash follower {follower}")
    docker_kill(follower)
    router.nodes[follower].close()
    time.sleep(4.0)

    print("[test] write more keys 6..10 while follower is down")
    for i in range(6, 11):
        leader_used, _ = write_with_retry(router, i, i * 10)
        print(f"[test] wrote key={i} via leader={leader_used}")

    print(f"[test] resume follower {follower}")
    docker_resume(follower)
    reconnect_and_init(router, follower)

    leader_after_follower_recover = find_leader_with_retry(router)
    ART.set_summary(leader_after_follower_recover=leader_after_follower_recover)
    print(f"[test] leader after follower recover = {leader_after_follower_recover}")

    print("[test] verify keys 1..10 after follower recovery")
    for i in range(1, 11):
        read_expect_with_retry(router, i, i * 10)

    print("\n===== PHASE 2: leader crash / re-election / recover =====")
    old_leader = leader_after_follower_recover
    ART.set_summary(old_leader=old_leader)
    print(f"[test] crash leader {old_leader}")
    docker_kill(old_leader)
    router.nodes[old_leader].close()

    print("[test] waiting for re-election")
    time.sleep(8.0)

    new_leader = find_leader_with_retry(router, attempts=40, sleep_s=1.0)
    ART.set_summary(new_leader=new_leader)
    if new_leader == old_leader:
        raise RuntimeError("leader did not change after crash")
    print(f"[test] new leader = {new_leader}")

    print("[test] write more keys 11..15 while old leader is down")
    time.sleep(3.0)
    for i in range(11, 16):
        leader_used, _ = write_with_retry(router, i, i * 10)
        print(f"[test] wrote key={i} via leader={leader_used}")

    print(f"[test] resume old leader {old_leader}")
    docker_resume(old_leader)
    reconnect_and_init(router, old_leader)

    final_leader = find_leader_with_retry(router)
    ART.set_summary(final_leader=final_leader)
    print(f"[test] final leader = {final_leader}")

    print("[test] verify all keys 1..15 after leader recovery")
    for i in range(1, 16):
        read_expect_with_retry(router, i, i * 10)

    print("\n===== RESULT =====")
    print("✅ BONUS TEST PASSED")
    print("Verified:")
    print("  - follower crash/restart")
    print("  - leader crash/restart")
    print("  - committed data survives")
    print("  - cluster continues after recovery")

    ART.finalize(ok=True)
    ART.save_logs()
    ART.save_storage_listing()
    tail_logs_to_console()
    storage_to_console()

    print(f"\nArtifacts written to: {ART.base_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ BONUS TEST FAILED: {repr(e)}")
        traceback.print_exc()
        ART.finalize(ok=False, error_text=repr(e))
        ART.save_logs()
        ART.save_storage_listing()
        tail_logs_to_console()
        storage_to_console()
        print(f"\nArtifacts written to: {ART.base_dir}")
        sys.exit(1)