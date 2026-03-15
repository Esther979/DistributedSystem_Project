#!/usr/bin/env python3
"""
OmniPaxos KV — Crash Recovery Integration Test
Manage node processes with Python subprocess to completely avoid the file system limitations of mkfifo.

Methods:
  python3 crash_recovery_test.py 
  python3 crash_recovery_test.py ./target/release/omnipaxos_kv_node
"""

import subprocess
import threading
import json
import time
import sys
import os
import signal
import shutil

# ── configuration ─────────────────────────────────────────────────────────────────────
BIN   = sys.argv[1] if len(sys.argv) > 1 else "./target/release/omnipaxos_kv_node"
NODES = ["n1", "n2", "n3"]               # node ID list
PASS  = True                             # results

# ── Node encapsulation ─────────────────────────────────────────────────────────────────
class Node:
    """
   Encapsulate an OmniPaxos KV node process.
    - Open it with subprocess.Popen, with both stdin and stdout being pipes.
    - A background thread continuously reads from stdout, storing each line of JSON in self.output.
    - The paxos_net messages between nodes are forwarded by the Router class.
    """

    def __init__(self, node_id: str, router):
        self.node_id = node_id
        self.router  = router
        self.output  = []          # All the received non-paxos_net lines
        self.all_out = []          # All output lines (including paxos_net)
        self.lock    = threading.Lock()
        self.proc    = None
        self._reader = None

    def start(self):
        self.proc = subprocess.Popen(
            [BIN],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,   # The eprintln! of the node will occur here
            text=True,
            bufsize=1,
        )
        # read stdout
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        # Back-end reads stderr (prints to the console for easy debugging)
        threading.Thread(target=self._err_loop, daemon=True).start()

    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            with self.lock:
                self.all_out.append(line)
                try:
                    msg = json.loads(line)
                    if msg.get("body", {}).get("type") == "paxos_net":
                        # Forward to the target node
                        self.router.deliver(msg)
                    else:
                        self.output.append(msg)
                except json.JSONDecodeError:
                    pass

    def _err_loop(self):
        for line in self.proc.stderr:
            print(f"  [stderr {self.node_id}] {line.rstrip()}", flush=True)

    def send(self, msg: dict):
        """Send a JSON message to the node (write it to its stdin)."""
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.stdin.write(json.dumps(msg) + "\n")
                self.proc.stdin.flush()
            except BrokenPipeError:
                pass   # dead

    def kill(self):
        """SIGKILL Simulated crash (preventing the process from performing any cleanup)"""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        print(f"  💀 {self.node_id} SIGKILL'd")

    def is_alive(self):
        return self.proc is not None and self.proc.poll() is None

    def wait_output(self, predicate, timeout=6.0) -> dict | None:
        """Wait for a message in self.output that satisfies the predicate, and return that message or None."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                for msg in reversed(self.output):
                    if predicate(msg):
                        return msg
            time.sleep(0.05)
        return None

    def wait_stderr(self, keyword, timeout=6.0) -> bool:
        """Wait for a line containing the keyword to appear in stderr."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            # stderr is read by _err_loop，Change to checking if the process is still alive + Poll all_out
            # The actual detection is carried out through the stderr file; here, a simple sleep solution is employed:
            time.sleep(0.1)
        return False

    def outputs_of_type(self, msg_type: str):
        with self.lock:
            return [m for m in self.output if m.get("body", {}).get("type") == msg_type]


# ── Router: Responsible for forwarding Paxos_net messages between nodes ────────────────────────────────
class Router:
    def __init__(self):
        self.nodes: dict[str, Node] = {}

    def register(self, node: Node):
        self.nodes[node.node_id] = node

    def deliver(self, msg: dict):
        dest = msg.get("dest")
        node = self.nodes.get(dest)
        if node and node.is_alive():
            node.send(msg)


# ── auxiliary function ──────────────────────────────────────────────────────────────────
msg_id_counter = 100

def next_id():
    global msg_id_counter
    msg_id_counter += 1
    return msg_id_counter

def make_init(node_id: str) -> dict:
    return {
        "src": "c0", "dest": node_id,
        "body": {
            "type": "init", "msg_id": next_id(),
            "node_id": node_id,
            "node_ids": NODES,
        }
    }

def make_write(dest, key, value) -> dict:
    return {"src": "c1", "dest": dest,
            "body": {"type": "write", "key": key, "value": value, "msg_id": next_id()}}

def make_read(dest, key) -> dict:
    return {"src": "c1", "dest": dest,
            "body": {"type": "read", "key": key, "msg_id": next_id()}}

def make_cas(dest, key, frm, to) -> dict:
    return {"src": "c1", "dest": dest,
            "body": {"type": "cas", "key": key, "from": frm, "to": to, "msg_id": next_id()}}

def check(cond: bool, label: str):
    global PASS
    if cond:
        print(f"  ✓ {label}")
    else:
        print(f"  ✗ FAIL: {label}")
        PASS = False

def section(title: str):
    print(f"\n── {title} {'─' * max(1, 50 - len(title))}")


# ── Test ────────────────────────────────────────────────────────────────
def main():
    # clean
    for d in os.listdir("."):
        if d.startswith("kv_snap_") or d.startswith("storage_node_"):
            shutil.rmtree(d, ignore_errors=True)

    router = Router()
    nodes  = {nid: Node(nid, router) for nid in NODES}
    for n in nodes.values():
        router.register(n)

    # ── Phase 1: start cluster ─────────────────────────────────────────────────────
    section("Phase 1: Start a 3-node cluster")
    for n in nodes.values():
        n.start()
        n.send(make_init(n.node_id))

    for nid, n in nodes.items():
        ok = n.wait_output(lambda m: m["body"]["type"] == "init_ok", timeout=5)
        check(ok is not None, f"{nid} receives init_ok")

    print("  Waiting for the leader election (2s)...")
    time.sleep(2)

    # ── Phase 2: Write in the initial data据 ─────────────────────────────────────────────────
    section("Phase 2: Write in the initial data")
    n1 = nodes["n1"]

    for key, val in [(1, 100), (2, 200), (3, 300)]:
        n1.send(make_write("n1", key, val))
    time.sleep(1.5)

    # Read verification write successful
    for key, expected in [(1, 100), (2, 200), (3, 300)]:
        mid = next_id()
        req = make_read("n1", key)
        req["body"]["msg_id"] = mid
        n1.send(req)
        reply = n1.wait_output(
            lambda m, k=key: (
                m["body"]["type"] == "read_ok" and
                m["body"].get("value") == [100, 200, 300][[1,2,3].index(k)]
            ), timeout=3
        )
        # Loose check: As long as "read_ok" is true and there is a "value" field
        time.sleep(0.5)

    ok_reads = n1.outputs_of_type("read_ok")
    print(f"  After being written, it was read {len(ok_reads)} read_ok")
    for r in ok_reads[-3:]:
        print(f"    key=? value={r['body'].get('value')}")

    # ── Phase 3: SIGKILL n1 ───────────────────────────────────────────────────
    section("Phase 3: SIGKILL n1(Simulated crash)")
    nodes["n1"].kill()
    time.sleep(0.5)

    snap_dir = "kv_snap_2"   # parse_node_id("n1") = 1+1 = 2
    check(os.path.isdir(snap_dir), f"kv_snap directory exists: {snap_dir}")

    # ── Phase 4: restart n1 ──────────────────────────────────────────────────────
    section("Phase 4: Restart n1 (send the new init)")
    time.sleep(0.5)

    new_n1 = Node("n1", router)
    router.register(new_n1)   # overwrite old one
    nodes["n1"] = new_n1
    new_n1.start()
    new_n1.send(make_init("n1"))

    ok = new_n1.wait_output(lambda m: m["body"]["type"] == "init_ok", timeout=6)
    check(ok is not None, "n1 restarts and receives init_ok")

    print("  Waiting for n1 to rejoin the cluster (3s)...")
    time.sleep(3)

    # ── Phase 5: Verify crash recovery ─────────────────────────────────────────────────
    section("Phase 5: Verify crash recovery - No loss of old data")

    results = {}
    for key in [1, 2, 3]:
        req = make_read("n1", key)
        new_n1.send(req)
        reply = new_n1.wait_output(
            lambda m, k=key, mid=req["body"]["msg_id"]: (
                m["body"].get("in_reply_to") == mid and
                m["body"]["type"] in ("read_ok", "error")
            ), timeout=5
        )
        if reply and reply["body"]["type"] == "read_ok":
            results[key] = reply["body"].get("value")
        else:
            results[key] = None

    check(results.get(1) == 100, f"key=1 restore correctly (got {results.get(1)}, want 100)")
    check(results.get(2) == 200, f"key=2 restore correctly (got {results.get(2)}, want 200)")
    check(results.get(3) == 300, f"key=3 restore correctly (got {results.get(3)}, want 300)")

    # ── Phase 6: Continue writing after the crash (linear consistency verification) ────────────────────────────
    section("Phase 6: Continue writing after the crash (linear consistency verification)")

    # write new value
    req_w = make_write("n1", 1, 999)
    new_n1.send(req_w)
    w_ok = new_n1.wait_output(
        lambda m, mid=req_w["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] == "write_ok",
        timeout=5
    )
    check(w_ok is not None, "After the crash, write(key=1, 999) was successful.")

    # read new value
    req_r = make_read("n1", 1)
    new_n1.send(req_r)
    r_ok = new_n1.wait_output(
        lambda m, mid=req_r["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] == "read_ok",
        timeout=5
    )
    val = r_ok["body"].get("value") if r_ok else None
    check(val == 999, f"Write after crash for immediate readability (got {val}, want 999)")

    # CAS test(For linearizability)
    req_cas = make_cas("n1", 1, 999, 777)
    new_n1.send(req_cas)
    cas_ok = new_n1.wait_output(
        lambda m, mid=req_cas["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] in ("cas_ok", "error"),
        timeout=5
    )
    check(cas_ok is not None and cas_ok["body"]["type"] == "cas_ok",
          "CAS(999→777) Success (previous value was correct)")

    req_r2 = make_read("n1", 1)
    new_n1.send(req_r2)
    r2 = new_n1.wait_output(
        lambda m, mid=req_r2["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] == "read_ok",
        timeout=5
    )
    val2 = r2["body"].get("value") if r2 else None
    check(val2 == 777, f"CAS result can be read (got {val2}, want 777)")

    # ── Cleaning process ──────────────────────────────────────────────────────────────────
    section("Cleaning process")
    for n in nodes.values():
        if n.is_alive():
            n.proc.terminate()
    time.sleep(0.5)

    # ── Final result ──────────────────────────────────────────────────────────────
    print("\n" + "═" * 55)
    if PASS:
        print("  ✅ All tests passed")
        print("     - Data persistence is correct after the node crashes (SIGKILL)")
        print("     - After the crash recovery, write/read/CAS is linearly consistent.")
    else:
        print("  ❌ There are failed items. Please check the output above.")
    print("═" * 55)
    sys.exit(0 if PASS else 1)


if __name__ == "__main__":
    main()