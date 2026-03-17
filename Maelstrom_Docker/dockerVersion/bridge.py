#!/usr/bin/env python3
import asyncio
import os
import sys
from asyncio import IncompleteReadError, LimitOverrunError

STREAM_LIMIT = 8 * 1024 * 1024  # 8MB

HOST = "0.0.0.0"
PORT = int(os.environ.get("NODE_PORT", "7000"))
KV_BIN = os.environ.get("KV_BIN", "/app/kv-node")


# 读取json文件
async def safe_readline(reader: asyncio.StreamReader) -> bytes:
    parts = []
    while True:
        try:
            chunk = await reader.readuntil(b"\n")
            parts.append(chunk)
            return b"".join(parts)
        except LimitOverrunError as e:
            if e.consumed > 0:
                parts.append(await reader.readexactly(e.consumed))
            else:
                parts.append(await reader.read(65536))
        except IncompleteReadError as e:
            if e.partial:
                parts.append(e.partial)
            return b"".join(parts)


class Bridge:
    def __init__(self):
        self.proc = None
        self.client_writer = None
        self.client_connected = asyncio.Event()
    #start kvnode
    async def start_proc(self):
        #接管kvnode的所有输入输出和报错
        self.proc = await asyncio.create_subprocess_exec(
            KV_BIN,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=STREAM_LIMIT,
        )
        # 一直读rust的进程stdout
        asyncio.create_task(self._pump_proc_stdout())
        asyncio.create_task(self._pump_proc_stderr())
    # transfer the data from kvnode to TCP socket 
    async def _pump_proc_stdout(self):
        while True:
            line = await safe_readline(self.proc.stdout)
            if not line:
                break
            if self.client_writer is not None:
                try:
                    self.client_writer.write(line)
                    await self.client_writer.drain()
                except Exception:
                    self.client_writer = None

    # the same as above but error
    async def _pump_proc_stderr(self):
        while True:
            line = await safe_readline(self.proc.stderr)
            if not line:
                break
            sys.stderr.buffer.write(line)
            sys.stderr.flush()
    # 一直监听来自TCP的数据 然后给从reader里面读一个给writer的数据
    #client 
    async def handle_client(self, reader, writer):
        if self.client_writer is not None:
            writer.write(b'{"error":"bridge already has active client"}\n')
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        self.client_writer = writer
        self.client_connected.set()

        try:
            while True:
                line = await safe_readline(reader)
                if not line:
                    break
                #往节点里面写数据
                self.proc.stdin.write(line)
                # 真的flush进去
                await self.proc.stdin.drain()
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self.client_writer = None
            self.client_connected.clear()
    
    
    async def run(self):
        #启动kv服务器，
        await self.start_proc()
        #启动监听TCP docker服务器
        server = await asyncio.start_server(
            self.handle_client,
            HOST,
            PORT,
            limit=STREAM_LIMIT,
        )
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets)
        print(f"[bridge] listening on {addrs}", file=sys.stderr, flush=True)

        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(Bridge().run())