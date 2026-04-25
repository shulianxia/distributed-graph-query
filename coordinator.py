#!/usr/bin/env python3
"""
分布式图查询系统 — Coordinator 协调节点

核心设计：
  1. Worker 注册管理 — 维护 worker_id → {host, port, partition}
  2. 一致性哈希路由 — 节点查询按 MD5 取模路由到目标 Worker
  3. 分布式三角计数（关键）—
     两阶段策略保证跨分区三角形不丢失：
       a) 收集全图所有边
       b) 对每条边 (u,v)，并行向 u 和 v 所在 Worker 索取邻居列表
       c) Coordinator 本地求交集 → 共同邻居 w → 三角形 (u,v,w)

  为什么不用 check_triangle(u,v) 委托给单一 Worker？
    三角顶点可能分布在 2~3 台不同的 Worker 上。
    Worker A 有 u 的邻接表但可能没有 v 的完整信息，
    Worker B 有 v 的邻接表但不知道 u 连了谁。
    只有 Coordinator 拿到两端邻居列表后求交集，才能覆盖跨分区三角形。
"""
import argparse, logging, os, socket, sys, threading, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    get_partition, make_msg, pack_msg, recv_msg,
    MSG_REGISTER, MSG_REGISTER_ACK, MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
    MSG_QUERY_NEIGHBOR, MSG_QUERY_COMMON, MSG_QUERY_TRIANGLE,
    MSG_QUERY_EDGES, MSG_QUERY_NLIST,
    MSG_RESULT_OK, MSG_RESULT_ERR, MSG_SHUTDOWN, logger,
)
logger = logging.getLogger("Coordinator")

class CoordServer:
    def __init__(self, host="0.0.0.0", port=9000):
        self.host, self.port = host, port
        self.running = False
        self.workers = {}         # wid → {host, port, partition}
        self.num_parts = 5
        self._lock = threading.Lock()
        self._server = None

    def start(self):
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(64)
        self.running = True
        logger.info(f"Coordinator @ {self.host}:{self.port}")
        while self.running:
            try:
                conn, addr = self._server.accept()
                threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()
            except:
                break

    # ── RPC 原语 ──
    def _rpc(self, wid, msg):
        with self._lock:
            info = self.workers.get(wid)
            if not info:
                return {"error": f"{wid} 未注册"}
            host, port = info["host"], info["port"]
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((host, port))
            sock.sendall(pack_msg(msg))
            resp = recv_msg(sock)
            sock.close()
            return resp or {"error": "无响应"}
        except Exception as e:
            return {"error": f"→ {wid} 失败: {e}"}

    def _extract_payload(self, resp):
        """从 RPC 应答中提取 payload dict（兼容多种返回格式）"""
        if isinstance(resp, dict) and "error" in resp:
            return resp
        if isinstance(resp, dict) and "payload" in resp:
            return resp["payload"]
        return resp

    def _locate(self, nid):
        """找到节点 nid 所在的 Worker"""
        part = get_partition(nid, self.num_parts)
        target = f"node_{part}"
        with self._lock:
            if target in self.workers:
                return target
            # 兼容：遍历找同分区
            for wid, info in self.workers.items():
                if info.get("partition") == part:
                    return wid
        return {"error": f"节点 {nid} (分区 {part}) 无 Worker"}

    # ── 消息处理 ──
    def _handle(self, conn, addr):
        try:
            msg = recv_msg(conn)
            if msg: conn.sendall(pack_msg(self._process(msg)))
        except Exception as e:
            logger.error(f"处理异常: {e}")
        finally:
            conn.close()

    def _process(self, msg):
        mt, payload = msg["msg_type"], msg.get("payload", {})

        if mt == MSG_REGISTER:
            return self._on_register(msg)

        elif mt == MSG_QUERY_NEIGHBOR:
            nid = payload["node_id"]
            target = self._locate(nid)
            if isinstance(target, dict): return make_msg(MSG_RESULT_ERR, "coord", target)
            resp = self._rpc(target, msg)
            return make_msg(MSG_RESULT_OK, "coord", self._extract_payload(resp))

        elif mt == MSG_QUERY_COMMON:
            a, b = payload["node_a"], payload["node_b"]
            wa, wb = self._locate(a), self._locate(b)
            if isinstance(wa, dict) or isinstance(wb, dict):
                return make_msg(MSG_RESULT_ERR, "coord",
                                {"error": wa.get("error", wb.get("error", ""))})
            if wa == wb:
                resp = self._rpc(wa, msg)
                return make_msg(MSG_RESULT_OK, "coord", self._extract_payload(resp))
            else:
                # 跨 Worker：各自查邻居列表 → 本地求交集
                ra = self._extract_payload(self._rpc(wa, make_msg(MSG_QUERY_NLIST, "coord", {"node_id": a})))
                rb = self._extract_payload(self._rpc(wb, make_msg(MSG_QUERY_NLIST, "coord", {"node_id": b})))
                if "error" in ra or "error" in rb:
                    return make_msg(MSG_RESULT_ERR, "coord",
                                    {"error": ra.get("error", "") + rb.get("error", "")})
                na = set(ra.get("neighbors", []))
                nb = set(rb.get("neighbors", []))
                common = sorted(na & nb)
                return make_msg(MSG_RESULT_OK, "coord", {
                    "node_a": a, "node_b": b, "common_neighbors": common, "count": len(common),
                })

        elif mt == MSG_QUERY_TRIANGLE:
            nid = payload.get("node_id")
            if nid is not None:
                return self._triangle_single(nid)
            else:
                return self._triangle_distributed()

        elif mt == MSG_QUERY_EDGES:
            # 收集全图边（转发给所有 Worker）
            return self._collect_all_edges()

        elif mt == MSG_SHUTDOWN:
            self.running = False
            return make_msg(MSG_RESULT_OK, "coord", {"status": "shutdown"})

        return make_msg(MSG_RESULT_ERR, "coord", {"error": f"未知类型 {mt}"})

    # ── 注册 ──
    def _on_register(self, msg):
        p = msg["payload"]
        wid = p["worker_id"]
        with self._lock:
            self.workers[wid] = {
                "host": p["host"], "port": p["port"],
                "partition": p.get("partition", 0),
            }
            if "num_parts" in p:
                self.num_parts = p["num_parts"]
            wlist = {k: {"host": v["host"], "port": v["port"]}
                     for k, v in self.workers.items()}
        logger.info(f"注册: {wid} @ {p['host']}:{p['port']} ({len(self.workers)} 个在线)")
        return make_msg(MSG_REGISTER_ACK, "coord", {"status": "ok", "workers": wlist})

    # ── 单节点三角 ──
    def _triangle_single(self, nid):
        """
        单节点三角：这个节点 nid 参与的所有三角形。
        跨分区时：nid 在 Worker A 上，但 nid 的邻居可能分散在不同 Worker 上。
        策略：先问 nid 的邻居列表，然后对每个邻居 u，问 nid 和 u 的共同邻居。
        如果 u 在另一台 Worker 上，共同邻居查询会自动跨 Worker 处理。
        """
        target = self._locate(nid)
        if isinstance(target, dict):
            return make_msg(MSG_RESULT_ERR, "coord", target)

        # 先拿 nid 的邻居列表
        resp = self._rpc(target, make_msg(MSG_QUERY_NLIST, "coord", {"node_id": nid}))
        payload = self._extract_payload(resp)
        if "error" in payload:
            return make_msg(MSG_RESULT_ERR, "coord", payload)
        nbrs = payload.get("neighbors", [])

        # 对每个邻居 u，查 nid 和 u 的共同邻居
        tris = set()
        for u in nbrs:
            cresp = self._process(make_msg(MSG_QUERY_COMMON, "coord", {"node_a": nid, "node_b": u}))
            cp = cresp.get("payload", {})
            for w in cp.get("common_neighbors", []):
                tris.add(tuple(sorted((nid, u, w))))

        sorted_tris = sorted(tris)
        return make_msg(MSG_RESULT_OK, "coord", {
            "node": nid, "count": len(sorted_tris), "triangles": sorted_tris,
        })

    # ── 全图三角（分布式协同） ──
    def _collect_all_edges(self):
        """从所有 Worker 收集全图边，以 set 返回"""
        with self._lock:
            wids = list(self.workers.keys())
        all_edges = set()
        lock = threading.Lock()
        def collect(wid):
            resp = self._rpc(wid, make_msg(MSG_QUERY_EDGES, "coord", {}))
            payload = self._extract_payload(resp)
            if "error" not in payload:
                with lock:
                    for e in payload.get("edges", []):
                        all_edges.add(tuple(e))
        threads = [threading.Thread(target=collect, args=(w,), daemon=True) for w in wids]
        for t in threads: t.start()
        for t in threads: t.join(timeout=60)
        logger.info(f"收集到 {len(all_edges)} 条全图边")
        return sorted(all_edges)

    def _triangle_distributed(self):
        """
        分布式全图三角计数 — 两阶段策略：

        阶段 1：收集全图所有边（所有 Worker 的 edges 并集）
        阶段 2：对每条边 (u,v)，同时向 u 所在 Worker 和 v 所在 Worker
                索取邻居列表。Coordinator 本地求交集 = 共同邻居。
                每条公共邻居 w 对应一个三角形 (u,v,w)。

        这是正确的分布式三角计数方案，因为：
        - u 的完整邻接表只在 u 所在 Worker 上
        - v 的完整邻接表只在 v 所在 Worker 上
        - Coordinator 拿到两端列表后本地求交集，不依赖任何单一 Worker 的全局视图
        """
        logger.info("开始分布式全图三角计数")
        edges = list(self._collect_all_edges())
        if isinstance(edges and edges[0], dict) and "error" in edges[0]:
            return make_msg(MSG_RESULT_ERR, "coord", {"error": "收集边失败"})

        tris = set()
        lock = threading.Lock()
        BATCH_SIZE = 100

        def process_batch(batch):
            local_tris = set()
            for u, v in batch:
                # 拿到 u 的邻居列表
                tu = self._locate(u)
                tv = self._locate(v)
                if isinstance(tu, dict) or isinstance(tv, dict):
                    continue
                ru = self._extract_payload(
                    self._rpc(tu, make_msg(MSG_QUERY_NLIST, "coord", {"node_id": u})))
                rv = self._extract_payload(
                    self._rpc(tv, make_msg(MSG_QUERY_NLIST, "coord", {"node_id": v})))
                if "error" in ru or "error" in rv:
                    continue
                nu = set(ru.get("neighbors", []))
                nv = set(rv.get("neighbors", []))
                for w in nu & nv:
                    local_tris.add(tuple(sorted((u, v, w))))
            with lock:
                tris.update(local_tris)

        threads = []
        for i in range(0, len(edges), BATCH_SIZE):
            batch = edges[i:i+BATCH_SIZE]
            t = threading.Thread(target=process_batch, args=(batch,), daemon=True)
            t.start(); threads.append(t)
        for t in threads: t.join(timeout=120)

        sorted_tris = sorted(tris)
        logger.info(f"全图三角计数完成: {len(sorted_tris)} 个")
        return make_msg(MSG_RESULT_OK, "coord", {"total": len(sorted_tris), "triangles": sorted_tris})

    def stop(self):
        self.running = False
        if self._server: self._server.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()
    cs = CoordServer(host=args.host, port=args.port)
    try: cs.start()
    except KeyboardInterrupt: cs.stop()

if __name__ == "__main__":
    main()
