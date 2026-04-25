#!/usr/bin/env python3
"""
分布式图查询系统 — Coordinator 协调节点

Coordinator 负责：
  1. 接受 Worker 注册
  2. 维护 Worker 注册表 (worker_id → {host, port, ...})
  3. 接收 Client 查询请求 → 转发到目标 Worker → 返回结果
  4. 分布式三角查询：收集全图边 → 按边分发到对应 Worker → 汇总结果

启动方式：
  python3 coordinator.py --port 9000
"""

import argparse
import logging
import os
import socket
import sys
import threading
import time
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    get_partition,
    make_msg,
    pack_msg,
    recv_msg,
    MSG_REGISTER,
    MSG_REGISTER_ACK,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_QUERY_NEIGHBOR,
    MSG_QUERY_COMMON,
    MSG_QUERY_TRIANGLE,
    MSG_QUERY_EDGES,
    MSG_QUERY_CHECK_TRI,
    MSG_RESULT_OK,
    MSG_RESULT_ERR,
    MSG_SHUTDOWN,
    logger,
)

logger = logging.getLogger("Coordinator")


class CoordServer:
    """协调节点"""

    def __init__(self, host="0.0.0.0", port=9000):
        self.host = host
        self.port = port
        self.running = False

        # Worker 注册表：worker_id → {host, port, directed, connected_sock, ...}
        self.workers = {}
        self._workers_lock = threading.Lock()

        # 已知 Worker 列表（用于分发边进行三角计算时知道要找谁）
        self.num_partitions = 5  # 默认分区数

        self._server = None

    def start(self):
        """启动 TCP 服务端"""
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(64)
        self.running = True
        logger.info(f"Coordinator 启动 @ {self.host}:{self.port}")

        while self.running:
            try:
                conn, addr = self._server.accept()
                threading.Thread(
                    target=self._handle_conn, args=(conn, addr), daemon=True
                ).start()
            except OSError:
                break

    def _rpc_to_worker(self, worker_id, msg) -> dict:
        """向指定 Worker 发送 RPC 请求，返回应答"""
        with self._workers_lock:
            info = self.workers.get(worker_id)
            if not info:
                return {"error": f"Worker {worker_id} 未注册"}
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
            return {"error": f"连接到 Worker {worker_id} 失败: {e}"}

    def _rpc_all_workers(self, msg_fn, skip=None):
        """向所有 Worker 并发发送 RPC"""
        with self._workers_lock:
            worker_ids = list(self.workers.keys())
        results = []
        threads = []
        lock = threading.Lock()

        def do_rpc(wid):
            msg = msg_fn(wid)
            resp = self._rpc_to_worker(wid, msg)
            with lock:
                results.append((wid, resp))

        for wid in worker_ids:
            if wid == skip:
                continue
            t = threading.Thread(target=do_rpc, args=(wid,), daemon=True)
            t.start()
            threads.append(t)
        for t in threads:
            t.join(timeout=60)
        return results

    def _handle_conn(self, conn, addr):
        """处理单个 TCP 连接"""
        try:
            msg = recv_msg(conn)
            if msg is None:
                return
            resp = self._process(msg)
            if resp:
                conn.sendall(pack_msg(resp))
        except Exception as e:
            logger.error(f"处理请求异常: {e}")
        finally:
            conn.close()

    def _process(self, msg):
        """消息路由"""
        sender = msg.get("sender", "unknown")
        payload = msg.get("payload", {})
        mt = msg["msg_type"]

        if mt == MSG_REGISTER:
            return self._on_register(msg)

        elif mt == MSG_HEARTBEAT:
            return make_msg(MSG_HEARTBEAT_ACK, "coordinator", {"status": "ok"})

        elif mt == MSG_QUERY_NEIGHBOR:
            nid = payload["node_id"]
            target = self._locate_node(nid)
            if "error" in target:
                return make_msg(MSG_RESULT_ERR, "coordinator", target)
            resp = self._rpc_to_worker(target, msg)
            if "error" in resp:
                return make_msg(MSG_RESULT_ERR, "coordinator", {"error": resp["error"]})
            return make_msg(MSG_RESULT_OK, "coordinator", resp.get("payload", resp))

        elif mt == MSG_QUERY_COMMON:
            a, b = payload["node_a"], payload["node_b"]
            # 两个节点可能在同一 Worker 或不同 Worker
            wa = self._locate_node(a)
            wb = self._locate_node(b)
            if "error" in wa or "error" in wb:
                return make_msg(MSG_RESULT_ERR, "coordinator",
                                {"error": wa.get("error") or wb.get("error")})

            if wa == wb:
                # 同一 Worker：直接转发
                resp = self._rpc_to_worker(wa, msg)
                if "error" in resp:
                    return make_msg(MSG_RESULT_ERR, "coordinator", {"error": resp["error"]})
                return make_msg(MSG_RESULT_OK, "coordinator", resp.get("payload", resp))
            else:
                # 不同 Worker：各自查邻居，Coordinator 本地求交集
                msg_a = make_msg(MSG_QUERY_NEIGHBOR, "coordinator", {"node_id": a})
                msg_b = make_msg(MSG_QUERY_NEIGHBOR, "coordinator", {"node_id": b})
                ra = self._rpc_to_worker(wa, msg_a)
                rb = self._rpc_to_worker(wb, msg_b)
                if "error" in ra or "error" in rb:
                    return make_msg(MSG_RESULT_ERR, "coordinator",
                                    {"error": ra.get("error", "") + rb.get("error", "")})
                na = set(ra.get("payload", ra).get("neighbors", []))
                nb = set(rb.get("payload", rb).get("neighbors", []))
                common = sorted(na & nb)
                return make_msg(MSG_RESULT_OK, "coordinator", {
                    "node_a": a, "node_b": b,
                    "common_neighbors": common, "count": len(common),
                })

        elif mt == MSG_QUERY_TRIANGLE:
            nid = payload.get("node_id")
            if nid is not None:
                # 单节点三角 — 查找该节点
                target = self._locate_node(nid)
                if "error" in target:
                    return make_msg(MSG_RESULT_ERR, "coordinator", target)
                resp = self._rpc_to_worker(target, msg)
                if "error" in resp:
                    return make_msg(MSG_RESULT_ERR, "coordinator", {"error": resp["error"]})
                return make_msg(MSG_RESULT_OK, "coordinator", resp.get("payload", resp))
            else:
                # 全图三角 — 分布式协同计算
                return self._distributed_triangle_count(msg)

        elif mt == MSG_QUERY_EDGES:
            # 收集全图边
            return self._collect_all_edges()

        elif mt == MSG_SHUTDOWN:
            logger.info("收到关闭指令")
            self.running = False
            return make_msg(MSG_RESULT_OK, "coordinator", {"status": "shutting_down"})

        else:
            return make_msg(MSG_RESULT_ERR, "coordinator", {
                "error": f"未知消息类型: {mt}"
            })

    def _locate_node(self, node_id):
        """根据节点 ID 找到所在 Worker"""
        with self._workers_lock:
            if not self.workers:
                return {"error": "没有注册的 Worker"}

            partition = get_partition(node_id, self.num_partitions)
            worker_id = f"node_{partition}"
            if worker_id not in self.workers:
                # 遍历查找
                for wid, info in self.workers.items():
                    if info.get("partition") == partition:
                        return wid
                return {"error": f"节点 {node_id} (partition={partition}) 无对应 Worker"}
            return worker_id

    def _on_register(self, msg):
        """处理 Worker 注册"""
        p = msg["payload"]
        wid = p["worker_id"]
        with self._workers_lock:
            self.workers[wid] = {
                "host": p["host"],
                "port": p["port"],
                "directed": p.get("directed", False),
                "partition": int(wid.split("_")[-1]) if "_" in wid else 0,
            }
            wlist = {k: v.copy() for k, v in self.workers.items()}
            # 减少 serialization
            wlist_short = {}
            for k, v in wlist.items():
                wlist_short[k] = {"host": v["host"], "port": v["port"]}
            logger.info(f"Worker 注册: {wid} @ {p['host']}:{p['port']} "
                        f"(共 {len(self.workers)} 个)")

        return make_msg(MSG_REGISTER_ACK, "coordinator", {
            "status": "ok",
            "workers": wlist_short,
        })

    def _collect_all_edges(self):
        """从所有 Worker 收集全图边"""
        results = self._rpc_all_workers(
            lambda wid: make_msg(MSG_QUERY_EDGES, "coordinator", {})
        )
        all_edges = set()
        for wid, resp in results:
            if "payload" in resp:
                edges = resp["payload"].get("edges", [])
            elif isinstance(resp, dict):
                edges = resp.get("edges", resp.get("payload", {}).get("edges", []))
            else:
                edges = []
            for e in edges:
                all_edges.add(tuple(e))
        return sorted(all_edges)

    def _distributed_triangle_count(self, original_msg):
        """
        分布式全图三角计数：收集全图边 → Coordinator 汇总 → 对每条边
        发送到 (src 所在 Worker 或 dst 所在 Worker)，让 Worker 检查
        两个端点是否有共同邻居 → 汇总去重
        """
        logger.info("开始分布式全图三角计数")

        # Step 1: 收集全图所有边
        edges = self._collect_all_edges()
        logger.info(f"收集到 {len(edges)} 条全图边")

        # Step 2: 对每条边，确定由哪个 Worker 来检测三角
        # 策略：边 (u,v) 发送给 u 所在 Worker，因为 Worker 存有 u 的完整邻接表
        triangles_set = set()
        lock = threading.Lock()

        def check_edge(u, v):
            target = self._locate_node(u)
            if "error" in target:
                return
            check_msg = make_msg(MSG_QUERY_CHECK_TRI, "coordinator", {"u": u, "v": v})
            resp = self._rpc_to_worker(target, check_msg)
            if "error" in resp:
                return
            pts = (resp.get("payload", {}) if isinstance(resp, dict)
                   else {}).get("triangles", [])
            with lock:
                for tri in pts:
                    triangles_set.add(tuple(tri))

        threads = []
        for u, v in edges:
            t = threading.Thread(target=check_edge, args=(u, v), daemon=True)
            t.start()
            threads.append(t)
            # 控制并发
            if len(threads) >= 100:
                for tt in threads:
                    tt.join(timeout=60)
                threads = []
        for t in threads:
            t.join(timeout=60)

        sorted_tris = sorted(triangles_set)
        logger.info(f"分布式三角计数完成: {len(sorted_tris)} 个三角形")
        return make_msg(MSG_RESULT_OK, "coordinator", {
            "total": len(sorted_tris),
            "triangles": sorted_tris,
        })

    def stop(self):
        self.running = False
        if self._server:
            self._server.close()
        logger.info("Coordinator 已关闭")


def main():
    parser = argparse.ArgumentParser(description="分布式图查询 Coordinator")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址")
    parser.add_argument("--port", type=int, default=9000, help="监听端口")
    args = parser.parse_args()

    cs = CoordServer(host=args.host, port=args.port)
    try:
        cs.start()
    except KeyboardInterrupt:
        cs.stop()


if __name__ == "__main__":
    main()
