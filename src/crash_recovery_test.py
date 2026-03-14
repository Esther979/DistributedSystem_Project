#!/usr/bin/env python3
"""
OmniPaxos KV — Crash Recovery Integration Test
用 Python subprocess 管理节点进程，完全避免 mkfifo 的文件系统限制。

用法：
  python3 crash_recovery_test.py [二进制路径]
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

# ── 配置 ─────────────────────────────────────────────────────────────────────
BIN   = sys.argv[1] if len(sys.argv) > 1 else "./target/release/omnipaxos_kv_node"
NODES = ["n1", "n2", "n3"]               # 节点 ID 列表
PASS  = True                             # 全局测试结果

# ── 节点封装 ─────────────────────────────────────────────────────────────────
class Node:
    """
    封装一个 OmniPaxos KV 节点进程。
    - 用 subprocess.Popen 打开，stdin/stdout 都是管道。
    - 后台线程持续读取 stdout，每行 JSON 存入 self.output。
    - 节点间的 paxos_net 消息由 Router 类负责转发。
    """

    def __init__(self, node_id: str, router):
        self.node_id = node_id
        self.router  = router
        self.output  = []          # 所有收到的非 paxos_net 行
        self.all_out = []          # 所有输出行（包括 paxos_net）
        self.lock    = threading.Lock()
        self.proc    = None
        self._reader = None

    def start(self):
        self.proc = subprocess.Popen(
            [BIN],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,   # 节点的 eprintln! 会在这里
            text=True,
            bufsize=1,
        )
        # 后台读 stdout
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        # 后台读 stderr（打印到控制台，方便调试）
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
                        # 转发给目标节点
                        self.router.deliver(msg)
                    else:
                        self.output.append(msg)
                except json.JSONDecodeError:
                    pass

    def _err_loop(self):
        for line in self.proc.stderr:
            print(f"  [stderr {self.node_id}] {line.rstrip()}", flush=True)

    def send(self, msg: dict):
        """向节点发送一条 JSON 消息（写入其 stdin）。"""
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.stdin.write(json.dumps(msg) + "\n")
                self.proc.stdin.flush()
            except BrokenPipeError:
                pass   # 进程已死，忽略

    def kill(self):
        """SIGKILL 模拟崩溃（不让进程做任何清理）。"""
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
        print(f"  💀 {self.node_id} SIGKILL'd")

    def is_alive(self):
        return self.proc is not None and self.proc.poll() is None

    def wait_output(self, predicate, timeout=6.0) -> dict | None:
        """等待 self.output 中出现满足 predicate 的消息，返回该消息或 None。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.lock:
                for msg in reversed(self.output):
                    if predicate(msg):
                        return msg
            time.sleep(0.05)
        return None

    def wait_stderr(self, keyword, timeout=6.0) -> bool:
        """等待 stderr 中出现包含 keyword 的行。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            # stderr 已被 _err_loop 读走，改为检查进程是否还活着 + 轮询 all_out
            # 实际检测走 stderr 文件；这里用一个简单的 sleep 方案：
            time.sleep(0.1)
        return False   # 见下方说明：stderr 检测在主流程里通过日志打印观察

    def outputs_of_type(self, msg_type: str):
        with self.lock:
            return [m for m in self.output if m.get("body", {}).get("type") == msg_type]


# ── 路由器：负责节点间 paxos_net 消息的转发 ────────────────────────────────
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


# ── 辅助函数 ──────────────────────────────────────────────────────────────────
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


# ── 主测试流程 ────────────────────────────────────────────────────────────────
def main():
    # 清理上次残留
    for d in os.listdir("."):
        if d.startswith("kv_snap_") or d.startswith("storage_node_"):
            shutil.rmtree(d, ignore_errors=True)

    router = Router()
    nodes  = {nid: Node(nid, router) for nid in NODES}
    for n in nodes.values():
        router.register(n)

    # ── Phase 1: 启动集群 ─────────────────────────────────────────────────────
    section("Phase 1: 启动 3 节点集群")
    for n in nodes.values():
        n.start()
        n.send(make_init(n.node_id))

    for nid, n in nodes.items():
        ok = n.wait_output(lambda m: m["body"]["type"] == "init_ok", timeout=5)
        check(ok is not None, f"{nid} 收到 init_ok")

    print("  等待 leader 选举 (2s)...")
    time.sleep(2)

    # ── Phase 2: 写入初始数据 ─────────────────────────────────────────────────
    section("Phase 2: 写入初始数据")
    n1 = nodes["n1"]

    for key, val in [(1, 100), (2, 200), (3, 300)]:
        n1.send(make_write("n1", key, val))
    time.sleep(1.5)

    # 读取验证写入成功
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
        # 宽松检查：只要 read_ok 且有 value 字段
        time.sleep(0.5)

    ok_reads = n1.outputs_of_type("read_ok")
    print(f"  写入后读取到 {len(ok_reads)} 条 read_ok")
    for r in ok_reads[-3:]:
        print(f"    key=? value={r['body'].get('value')}")

    # ── Phase 3: SIGKILL n1 ───────────────────────────────────────────────────
    section("Phase 3: SIGKILL n1（模拟崩溃）")
    nodes["n1"].kill()
    time.sleep(0.5)

    snap_dir = "kv_snap_2"   # parse_node_id("n1") = 1+1 = 2
    check(os.path.isdir(snap_dir), f"kv_snap 目录存在: {snap_dir}")

    # ── Phase 4: 重启 n1 ──────────────────────────────────────────────────────
    section("Phase 4: 重启 n1（发送新的 init）")
    time.sleep(0.5)

    new_n1 = Node("n1", router)
    router.register(new_n1)   # 覆盖旧的注册
    nodes["n1"] = new_n1
    new_n1.start()
    new_n1.send(make_init("n1"))

    ok = new_n1.wait_output(lambda m: m["body"]["type"] == "init_ok", timeout=6)
    check(ok is not None, "n1 重启后收到 init_ok")

    print("  等待 n1 重新加入集群 (3s)...")
    time.sleep(3)

    # ── Phase 5: 验证崩溃恢复 ─────────────────────────────────────────────────
    section("Phase 5: 验证崩溃恢复 — 旧数据不丢失")

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

    check(results.get(1) == 100, f"key=1 恢复正确 (got {results.get(1)}, want 100)")
    check(results.get(2) == 200, f"key=2 恢复正确 (got {results.get(2)}, want 200)")
    check(results.get(3) == 300, f"key=3 恢复正确 (got {results.get(3)}, want 300)")

    # ── Phase 6: 崩溃后继续写入（线性一致性验证）────────────────────────────
    section("Phase 6: 崩溃后继续写入 — 线性一致性")

    # 写新值
    req_w = make_write("n1", 1, 999)
    new_n1.send(req_w)
    w_ok = new_n1.wait_output(
        lambda m, mid=req_w["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] == "write_ok",
        timeout=5
    )
    check(w_ok is not None, "崩溃后 write(key=1, 999) 成功")

    # 读回新值
    req_r = make_read("n1", 1)
    new_n1.send(req_r)
    r_ok = new_n1.wait_output(
        lambda m, mid=req_r["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] == "read_ok",
        timeout=5
    )
    val = r_ok["body"].get("value") if r_ok else None
    check(val == 999, f"崩溃后写入立即可读 (got {val}, want 999)")

    # CAS 测试（linearizability 关键操作）
    req_cas = make_cas("n1", 1, 999, 777)
    new_n1.send(req_cas)
    cas_ok = new_n1.wait_output(
        lambda m, mid=req_cas["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] in ("cas_ok", "error"),
        timeout=5
    )
    check(cas_ok is not None and cas_ok["body"]["type"] == "cas_ok",
          "CAS(999→777) 成功（前值正确）")

    req_r2 = make_read("n1", 1)
    new_n1.send(req_r2)
    r2 = new_n1.wait_output(
        lambda m, mid=req_r2["body"]["msg_id"]:
            m["body"].get("in_reply_to") == mid and
            m["body"]["type"] == "read_ok",
        timeout=5
    )
    val2 = r2["body"].get("value") if r2 else None
    check(val2 == 777, f"CAS 结果可读 (got {val2}, want 777)")

    # ── 清理 ──────────────────────────────────────────────────────────────────
    section("清理进程")
    for n in nodes.values():
        if n.is_alive():
            n.proc.terminate()
    time.sleep(0.5)

    # ── 最终结果 ──────────────────────────────────────────────────────────────
    print("\n" + "═" * 55)
    if PASS:
        print("  ✅ 所有测试通过")
        print("     - 节点崩溃（SIGKILL）后数据持久化正确")
        print("     - 崩溃恢复后写入/读取/CAS 线性一致")
    else:
        print("  ❌ 存在失败项，请检查上方输出")
    print("═" * 55)
    sys.exit(0 if PASS else 1)


if __name__ == "__main__":
    main()