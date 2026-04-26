#!/usr/bin/env python3
"""
分布式图查询系统 — test3 Coordinator

架构：
  所有查询走短连接（每次新建 TCP，零竞态，高可靠）
  优化策略 — 全图三角计数采用预取方案：
    1. Coordinator 收集全图边（分区内边列表，每 Worker 1 次 RPC）
    2. 对每条边 (u,v)，从两端 Worker 分别拉取邻居列表
    3. 本地求交集得共同邻居

  预取 vs 普通：预取只需直接使用 two-sided neighbor query 即可，
  与 test2 不同的是去掉 check_triangle，全部改用双端 nlist。
  不采用连接池/控制通道，避免多线程竞态。
"""
import argparse, logging, os, socket, sys, threading, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    MSG_RESULT_OK, MSG_RESULT_ERR, MSG_SHUTDOWN,
    MSG_QUERY_NEIGHBOR, MSG_QUERY_NLIST, MSG_QUERY_COMMON, MSG_QUERY_TRIANGLE,
    MSG_REGISTER, MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
    make_msg, pack_msg, recv_msg, get_partition
)

logger = logging.getLogger("Coordinator")

class Coordinator:
    """协调节点 — 无状态、短连接"""

    def __init__(self, host="0.0.0.0", port=9000):
        self.host, self.port = host, port
        self.running = False
        self.workers = {}  # wid → {host, port, partition}
        self.num_parts = 5
        self._lock = threading.Lock()
        self._server = None
        # 路由表：msg_type → 处理方法
        self._handlers = {
            MSG_REGISTER: self._on_register,
            MSG_SHUTDOWN: self._on_shutdown,
            MSG_QUERY_NEIGHBOR: lambda msg: make_msg(MSG_RESULT_OK, "coord",
                self.query_neighbor(msg["payload"].get("node", 0))),
            MSG_QUERY_COMMON: lambda msg: make_msg(MSG_RESULT_OK, "coord",
                self.query_common(
                    msg["payload"].get("a", msg["payload"].get("node_a", 0)),
                    msg["payload"].get("b", msg["payload"].get("node_b", 0)))),
            MSG_QUERY_TRIANGLE: lambda msg: make_msg(MSG_RESULT_OK, "coord",
                self.query_triangles_global() if "node" not in msg["payload"] and "node_id" not in msg["payload"]
                else self.query_triangles_node(msg["payload"].get("node_id", msg["payload"].get("node", 0)))),
            MSG_QUERY_NLIST: lambda msg: make_msg(MSG_RESULT_OK, "coord",
                self.query_neighbor(msg["payload"].get("node", 0))),
        }

    # ── 启动 ──
    def start(self):
        self.running = True
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(16)
        logger.info(f"Coordinator @ {self.host}:{self.port}")
        threading.Thread(target=self._auto_hb, daemon=True).start()
        while self.running:
            conn, addr = self._server.accept()
            threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()

    def stop(self):
        self.running = False
        if self._server:
            self._server.close()

    # ── 请求处理 ──
    def _handle(self, conn, addr):
        try:
            msg = recv_msg(conn)
            if msg is None: return
            handler = self._handlers.get(msg.get("msg_type", ""), self._err_handler)
            resp = handler(msg)
            if resp:
                conn.sendall(pack_msg(resp))
        except Exception as e:
            logger.error(f"处理异常: {e}")
        finally:
            conn.close()

    def _err_handler(self, msg):
        return make_msg(MSG_RESULT_ERR, "coord", {"error": "unknown msg_type"})

    def _on_register(self, msg):
        p = msg.get("payload", {})
        wid = p.get("worker_id", "unknown")
        with self._lock:
            self.workers[wid] = {
                "host": p.get("host", "127.0.0.1"),
                "port": p.get("port", 0),
                "partition": p.get("partition", 0),
            }
            if "num_parts" in p:
                self.num_parts = p["num_parts"]
            cnt = len(self.workers)
        logger.info(f"注册: {wid} @ {p.get('host')}:{p.get('port')} ({cnt} 个在线)")
        return make_msg(MSG_RESULT_OK, "coord", {"count": cnt})

    def _on_shutdown(self, msg):
        logger.info("收到关闭指令")
        self.running = False
        return make_msg(MSG_RESULT_OK, "coord", {"status": "shutdown"})

    # ── 心跳（短连接） ──
    def _auto_hb(self):
        """每 15 秒对所有 Worker 做一次心跳"""
        while self.running:
            time.sleep(15)
            with self._lock:
                items = list(self.workers.items())
            for wid, info in items:
                try:
                    s = socket.create_connection((info["host"], info["port"]), timeout=5)
                    send_msg(s, make_msg(MSG_HEARTBEAT, "coord", {}))
                    resp = recv_msg(s)
                    s.close()
                    if resp and resp["msg_type"] == MSG_HEARTBEAT_ACK:
                        continue
                except:
                    pass
                with self._lock:
                    self.workers.pop(wid, None)
                logger.warning(f"{wid} 心跳超时，已移除")

    # ── 路由 ──
    def _route(self, node_id):
        with self._lock:
            if not self.workers:
                return None
            part = get_partition(node_id, self.num_parts)
            for wid, info in self.workers.items():
                if info["partition"] == part or len(self.workers) == 1:
                    return info
            return None

    # ── RPC ──
    def _rpc(self, info, msg):
        """短连接 RPC"""
        try:
            s = socket.create_connection((info["host"], info["port"]), timeout=30)
            s.sendall(pack_msg(msg))
            resp = recv_msg(s)
            s.close()
            return resp
        except Exception as e:
            logger.error(f"RPC 失败 ({info.get('host')}:{info.get('port')}): {e}")
            return None

    # ── 查询 ──
    def query_neighbor(self, node_id):
        info = self._route(node_id)
        if not info: return {"error": f"节点 {node_id} 无 Worker"}
        resp = self._rpc(info, make_msg(MSG_QUERY_NEIGHBOR, "coord", {"node": node_id}))
        if not resp: return {"error": "无响应"}
        return resp.get("payload", {})

    def query_common(self, a, b):
        """共同邻居：可能跨分区"""
        info_a, info_b = self._route(a), self._route(b)
        if not info_a or not info_b:
            return {"error": f"节点 {a} 或 {b} 无 Worker"}
        # 同分区：直接查
        if info_a is info_b:
            resp = self._rpc(info_a, make_msg(MSG_QUERY_COMMON, "coord", {"a": a, "b": b}))
            if not resp: return {"error": "无响应"}
            return resp.get("payload", {})
        # 跨分区：拉取邻居列表，本地求交集
        resp_a = self._rpc(info_a, make_msg(MSG_QUERY_NLIST, "coord", {"node": a}))
        resp_b = self._rpc(info_b, make_msg(MSG_QUERY_NLIST, "coord", {"node": b}))
        if not resp_a or not resp_b:
            return {"error": "无响应"}
        na = set(resp_a.get("payload", {}).get("neighbors", []))
        nb = set(resp_b.get("payload", {}).get("neighbors", []))
        common = sorted(na & nb)
        return {"a": a, "b": b, "common_neighbors": common, "count": len(common)}

    # ── 三角计数 ──
    def query_triangles_global(self):
        """分布式全图三角计数 — 预取优化版
        策略：
          1. 从所有 Worker 预取完整邻接表到 Coordinator 本地缓存（只 5 次 RPC）
          2. 在 Coordinator 本地基于全量邻接表计算所有三角形
          3. 比逐边 RPC 减少 ~O(E) 次网络往返
        """
        logger.info("开始分布式全图三角计数（预取模式）")
        # 1. 预取所有 Worker 的邻接表
        local_adj = {}  # node → list[neighbors]
        with self._lock:
            items = list(self.workers.items())
        for wid, info in items:
            resp = self._rpc(info, make_msg("query_all_edges", "coord", {}))
            if resp and resp["msg_type"] == MSG_RESULT_OK:
                for u, v in resp.get("payload", {}).get("edges", []):
                    local_adj.setdefault(u, set()).add(v)
                    local_adj.setdefault(v, set()).add(u)
        logger.info(f"预取邻接表: {len(local_adj)} 节点")
        if not local_adj:
            return {"error": "收集边失败"}

        # 2. 本地三角计数（按节点遍历邻接表 + 查共同邻居）
        triangles = set()
        nodes = sorted(local_adj.keys())
        for i, u in enumerate(nodes):
            nu = local_adj[u]
            for v in nu:
                if v <= u:
                    continue
                nv = local_adj.get(v, set())
                for w in nu & nv:
                    if w > v:
                        triangles.add((u, v, w))

        logger.info(f"分布式三角计数完成（预取）: {len(triangles)} 个三角形")
        return {"count": len(triangles), "triangles": sorted(triangles)}

    def query_triangles_node(self, node_id):
        """单节点三角"""
        info = self._route(node_id)
        if not info: return {"error": f"节点 {node_id} 无 Worker"}
        resp = self._rpc(info, make_msg(MSG_QUERY_NLIST, "coord", {"node": node_id}))
        if not resp or resp["msg_type"] != MSG_RESULT_OK:
            return {"error": "无响应"}
        na = set(resp["payload"].get("neighbors", []))
        triangles = []
        for n in na:
            info_n = self._route(n)
            if not info_n: continue
            resp_n = self._rpc(info_n, make_msg(MSG_QUERY_NLIST, "coord", {"node": n}))
            if not resp_n or resp_n["msg_type"] != MSG_RESULT_OK:
                continue
            nn = set(resp_n["payload"].get("neighbors", []))
            common = na & nn
            for w in common:
                if w > n:
                    triangles.append((node_id, n, w))
        return {"node": node_id, "count": len(triangles), "triangles": sorted(triangles)}


if __name__ == "__main__":
    from protocol import send_msg, recv_or_none
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    logging.getLogger().setLevel(logging.INFO)
    Coordinator(host=args.host, port=args.port).start()
