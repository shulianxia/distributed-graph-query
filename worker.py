#!/usr/bin/env python3
"""
分布式图查询系统 — Worker 节点

Worker 负责：
  1. 从本地数据文件加载图分区数据
  2. 启动 TCP 服务端，监听 Coordinator 的查询请求
  3. 启动时向 Coordinator 注册自己
  4. 响应 邻居查询 / 共同邻居 / 三角检测 等查询

启动方式：
  python3 worker.py --worker-id node_0 --port 9100 --coord-host 127.0.0.1 --coord-port 9000
  python3 worker.py --worker-id node_1 --port 9101 --coord-host 127.0.0.1 --coord-port 9000
  ...
"""

import argparse
import json
import logging
import os
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    GraphStorage,
    LocalQuery,
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

logger = logging.getLogger("Worker")


class WorkerNode:
    """Worker 节点 — TCP 服务端 + 本地图存储"""

    def __init__(
        self,
        worker_id: str,
        host: str,
        port: int,
        coord_host: str,
        coord_port: int,
        data_file: str = None,
        directed: bool = False,
    ):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.coord_host = coord_host
        self.coord_port = coord_port
        self.directed = directed
        self.running = False

        # 图数据
        self.storage = GraphStorage(directed=directed)
        if data_file and os.path.exists(data_file):
            self._load_data(data_file)
            logger.info(f"从 {data_file} 加载了 {self.storage.num_nodes()} 节点, "
                        f"{self.storage.num_edges()} 条边")

        # 查询引擎
        self.query = LocalQuery(self.storage)

        # TCP 服务端
        self._server = None

        # 已知的其它 Worker (来自 Coordinator 的注册应答)
        self.known_workers = {}

    def _load_data(self, path: str):
        """从 JSON 文件加载图数据"""
        with open(path, "r") as f:
            data = json.load(f)
        self.storage = GraphStorage.from_dict(data)

    def _save_data(self, path: str):
        """保存图数据到 JSON 文件"""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.storage.to_dict(), f, ensure_ascii=False, indent=2)
        logger.info(f"数据保存到 {path}")

    def add_graph_data(self, graph_data: dict):
        """接收 Coordinator 分发的图分区数据"""
        incoming = GraphStorage.from_dict(graph_data)
        # 合并边
        for src in incoming.all_nodes():
            for dst in incoming.get_neighbors(src):
                if not self.storage.has_edge(src, dst):
                    attrs = incoming.get_node_attrs(src)
                    self.storage.add_node(src, **attrs)
                self.storage.add_edge(src, dst)

    def start(self):
        """启动 TCP 服务端"""
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(32)
        self.running = True
        logger.info(f"Worker {self.worker_id} 启动 @ {self.host}:{self.port}")

        # 服务线程
        t_serve = threading.Thread(target=self._serve, daemon=True)
        t_serve.start()

        # 向 Coordinator 注册
        self._register()
        logger.info(f"已向 Coordinator @ {self.coord_host}:{self.coord_port} 注册")

        return self.port

    def _serve(self):
        """接受连接循环"""
        while self.running:
            try:
                conn, addr = self._server.accept()
                threading.Thread(
                    target=self._handle_conn, args=(conn, addr), daemon=True
                ).start()
            except OSError:
                break

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
        """根据消息类型分发到对应的处理方法"""
        sender = msg.get("sender", "unknown")
        payload = msg.get("payload", {})
        mt = msg["msg_type"]

        if mt == MSG_QUERY_NEIGHBOR:
            nid = payload["node_id"]
            result = self.query.query_neighbor(nid)
            return make_msg(MSG_RESULT_OK, self.worker_id, result)

        elif mt == MSG_QUERY_COMMON:
            a, b = payload["node_a"], payload["node_b"]
            result = self.query.query_common(a, b)
            return make_msg(MSG_RESULT_OK, self.worker_id, result)

        elif mt == MSG_QUERY_TRIANGLE:
            nid = payload.get("node_id")
            if nid is not None:
                tris = self.query.query_triangle_node(nid)
                return make_msg(MSG_RESULT_OK, self.worker_id, {
                    "node": nid,
                    "count": len(tris),
                    "triangles": tris,
                })
            else:
                # 全图三角 — 返回本地三角
                # 遍历所有节点，挨个查
                all_tris = set()
                for node in self.storage.all_nodes():
                    for tri in self.query.query_triangle_node(node):
                        all_tris.add(tri)
                return make_msg(MSG_RESULT_OK, self.worker_id, {
                    "total": len(all_tris),
                    "triangles": sorted(all_tris),
                })

        elif mt == MSG_QUERY_EDGES:
            # 返回本地所有边
            edges = list(self.query.all_edges_iter())
            return make_msg(MSG_RESULT_OK, self.worker_id, {
                "edges": [(u, v) for u, v in edges],
            })

        elif mt == MSG_QUERY_CHECK_TRI:
            u, v = payload["u"], payload["v"]
            triangles = self.query.check_triangle(u, v)
            return make_msg(MSG_RESULT_OK, self.worker_id, {
                "triangles": triangles,
            })

        elif mt == MSG_HEARTBEAT:
            return make_msg(MSG_HEARTBEAT_ACK, self.worker_id, {"status": "alive"})

        elif mt == MSG_SHUTDOWN:
            self.running = False
            return make_msg(MSG_RESULT_OK, self.worker_id, {"status": "shutdown"})

        else:
            return make_msg(MSG_RESULT_ERR, self.worker_id, {
                "error": f"未知消息类型: {mt}"
            })

    def _register(self):
        """向 Coordinator 注册自身"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.coord_host, self.coord_port))
            reg_msg = make_msg(
                MSG_REGISTER,
                self.worker_id,
                {
                    "worker_id": self.worker_id,
                    "host": self.host,
                    "port": self.port,
                    "directed": self.directed,
                },
            )
            sock.sendall(pack_msg(reg_msg))
            resp = recv_msg(sock)
            if resp and resp["msg_type"] == MSG_REGISTER_ACK:
                workers = resp["payload"].get("workers", {})
                logger.info(f"注册成功, 已知 {len(workers)} 个 Worker")
            sock.close()
        except Exception as e:
            logger.error(f"注册失败: {e}")

    def stop(self):
        self.running = False
        if self._server:
            self._server.close()
        logger.info(f"Worker {self.worker_id} 已关闭")


def generate_data_file(
    out_path: str,
    num_nodes: int,
    edge_density: float = 0.02,
    seed: int = 42,
    partition: int = 0,
    total_partitions: int = 1,
    directed: bool = False,
):
    """
    生成单个 Worker (partition) 的图数据文件。
    仅包含哈希到此分区的节点以及这些节点的邻接信息。
    """
    import random
    import hashlib
    random.seed(seed)

    full_nodes = list(range(num_nodes))
    my_nodes = [
        n for n in full_nodes
        if (int(hashlib.md5(str(n).encode()).hexdigest(), 16) % total_partitions) == partition
    ]

    g = GraphStorage(directed=directed)
    for n in my_nodes:
        g.add_node(n, label=f"Node_{n}")

    # 添加边：对每个我的节点，随机连一些邻居
    # 邻居可能在我的分区也可能不在
    avg_deg = max(1, int(num_nodes * edge_density))
    for n in my_nodes:
        possible = [x for x in full_nodes if x != n and not g.has_edge(n, x)]
        targets = random.sample(possible, min(avg_deg, len(possible)))
        for t in targets:
            g.add_edge(n, t, weight=random.randint(1, 10))

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(g.to_dict(), f, ensure_ascii=False, indent=2)
    logger.info(
        f"分区 {partition}/{total_partitions}: "
        f"{g.num_nodes()} 节点, {g.num_edges()} 条边 → {out_path}"
    )


def main():
    import hashlib

    parser = argparse.ArgumentParser(description="分布式图查询 Worker 节点")
    parser.add_argument("--worker-id", required=True, help="Worker 标识")
    parser.add_argument("--host", default="0.0.0.0", help="Worker 监听地址")
    parser.add_argument("--port", type=int, default=0, help="Worker 监听端口")
    parser.add_argument("--coord-host", default="127.0.0.1", help="Coordinator 地址")
    parser.add_argument("--coord-port", type=int, default=9000, help="Coordinator 端口")
    parser.add_argument("--data-file", default=None, help="图数据 JSON 文件路径")
    parser.add_argument("--generate", nargs=4, metavar=("NODES", "DENSITY", "SEED", "OUT"),
                        help="生成数据: python3 worker.py --generate 5000 0.02 42 data.json")
    parser.add_argument("--directed", action="store_true", help="有向图模式")
    args = parser.parse_args()

    # 生成数据模式
    if args.generate:
        nodes, density, seed, out = args.generate
        generate_data_file(
            out, int(nodes), float(density), int(seed),
            partition=int(args.worker_id.split("_")[-1]) if "_" in args.worker_id else 0,
            total_partitions=5,
            directed=args.directed,
        )
        return

    # 启动 Worker
    w = WorkerNode(
        worker_id=args.worker_id,
        host=args.host,
        port=args.port,
        coord_host=args.coord_host,
        coord_port=args.coord_port,
        data_file=args.data_file,
        directed=args.directed,
    )
    w.start()

    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        w.stop()


if __name__ == "__main__":
    main()
