#!/usr/bin/env python3
"""
分布式图查询系统 — Worker 节点
独立进程，加载本地图数据，启动 TCP 服务端，向 Coordinator 注册，
响应邻居查询 / 邻居列表 / 共同邻居 / 三角检测。
"""
import argparse, json, logging, os, socket, sys, threading, time, hashlib
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    GraphStorage, LocalQuery, get_partition, make_msg, pack_msg, recv_msg,
    MSG_REGISTER, MSG_REGISTER_ACK, MSG_HEARTBEAT, MSG_HEARTBEAT_ACK,
    MSG_QUERY_NEIGHBOR, MSG_QUERY_COMMON, MSG_QUERY_TRIANGLE,
    MSG_QUERY_EDGES, MSG_QUERY_NLIST,
    MSG_RESULT_OK, MSG_RESULT_ERR, MSG_SHUTDOWN, logger,
)
logger = logging.getLogger("Worker")

class WorkerNode:
    def __init__(self, wid, host, port, coord_host, coord_port, data_file=None, num_parts=5):
        self.wid = wid
        self.host = host
        self.port = port
        self.coord_host = coord_host
        self.coord_port = coord_port
        self.num_parts = num_parts

        # 从 worker_id 解析分区号
        if "_" in wid:
            self.partition = int(wid.split("_")[-1])
        else:
            self.partition = 0

        self.storage = GraphStorage()
        if data_file and os.path.exists(data_file):
            self._load(data_file)
            logger.info(f"加载 {self.storage.num_nodes()} 节点, {self.storage.num_edges()} 边")

        self.query = LocalQuery(self.storage)
        self.running = False
        self._server = None

    def _load(self, path):
        with open(path) as f:
            self.storage = GraphStorage.from_dict(json.load(f))

    def start(self):
        self._server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server.bind((self.host, self.port))
        self._server.listen(32)
        self.running = True
        threading.Thread(target=self._serve, daemon=True).start()
        self._register()
        logger.info(f"Worker {self.wid} @ {self.host}:{self.port} 就绪 (分区 {self.partition})")

    def _serve(self):
        while self.running:
            try:
                conn, addr = self._server.accept()
                threading.Thread(target=self._handle, args=(conn, addr), daemon=True).start()
            except:
                break

    def _handle(self, conn, addr):
        try:
            msg = recv_msg(conn)
            if msg is None: return
            resp = self._process(msg)
            if resp:
                conn.sendall(pack_msg(resp))
        except Exception as e:
            logger.error(f"处理异常: {e}")
        finally:
            conn.close()

    def _process(self, msg):
        sender = msg.get("sender", "?")
        payload = msg.get("payload", {})
        mt = msg["msg_type"]

        if mt == MSG_QUERY_NEIGHBOR:
            return make_msg(MSG_RESULT_OK, self.wid, self.query.query_neighbor(payload["node_id"]))
        elif mt == MSG_QUERY_NLIST:
            return make_msg(MSG_RESULT_OK, self.wid, self.query.query_nlist(payload["node_id"]))
        elif mt == MSG_QUERY_COMMON:
            return make_msg(MSG_RESULT_OK, self.wid, self.query.query_common(payload["node_a"], payload["node_b"]))
        elif mt == MSG_QUERY_TRIANGLE:
            nid = payload.get("node_id")
            if nid is not None:
                tris = self.query.triangle_node_local(nid)
                return make_msg(MSG_RESULT_OK, self.wid, {"node": nid, "count": len(tris), "triangles": tris})
            else:
                tris = self.query.all_triangles_local()
                return make_msg(MSG_RESULT_OK, self.wid, {"total": len(tris), "triangles": tris})
        elif mt == MSG_QUERY_EDGES:
            edges = list(self.query.all_edges_iter())
            return make_msg(MSG_RESULT_OK, self.wid, {"edges": edges})
        elif mt == MSG_HEARTBEAT:
            return make_msg(MSG_HEARTBEAT_ACK, self.wid, {"status": "alive"})
        elif mt == MSG_SHUTDOWN:
            self.running = False
            return make_msg(MSG_RESULT_OK, self.wid, {"status": "shutdown"})
        else:
            return make_msg(MSG_RESULT_ERR, self.wid, {"error": f"未知类型 {mt}"})

    def _register(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.coord_host, self.coord_port))
            sock.sendall(pack_msg(make_msg(MSG_REGISTER, self.wid, {
                "worker_id": self.wid, "host": self.host, "port": self.port,
                "partition": self.partition, "num_parts": self.num_parts,
            })))
            resp = recv_msg(sock)
            sock.close()
            if resp and resp["msg_type"] == MSG_REGISTER_ACK:
                workers = resp["payload"].get("workers", {})
                logger.info(f"注册成功, {len(workers)} 个在线 Worker")
        except Exception as e:
            logger.error(f"注册失败: {e}")

    def stop(self):
        self.running = False
        if self._server: self._server.close()

# ── 数据生成 ──
def generate_data(out_path, num_nodes, density=0.08, seed=42, partition=0, total_parts=5):
    import random; random.seed(seed)
    g = GraphStorage()
    my_nodes = [n for n in range(num_nodes) if get_partition(n, total_parts) == partition]
    for n in my_nodes:
        g.add_node(n, label=f"n{n}")

    # 阶段1: 为每个本分区的节点生成随机边
    avg_deg = max(1, int(num_nodes * density))
    local_edges = []
    for n in my_nodes:
        possible = [x for x in range(num_nodes) if x != n]
        targets = random.sample(possible, min(avg_deg, len(possible)))
        for t in targets:
            local_edges.append((n, t))

    # 阶段2: 每条边加到两个端点的本分区邻接表中
    # 注意：如果两端都在本分区只加一次，否则各加各的
    for u, v in local_edges:
        pu = get_partition(u, total_parts)
        pv = get_partition(v, total_parts)
        if pu == partition and pv == partition:
            g.add_edge(u, v, weight=random.randint(1, 10))
        else:
            # 只加本分区的那个端点的边
            if pu == partition:
                g.add_node(u); g.add_node(v)
                if v not in g.adjacency[u]:
                    g.adjacency[u].append(v)
            if pv == partition:
                g.add_node(v); g.add_node(u)
                if u not in g.adjacency[v]:
                    g.adjacency[v].append(u)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(g.to_dict(), f, ensure_ascii=False, indent=2)
    logger.info(f"分区 {partition}/{total_parts}: {g.num_nodes()} 节点, {g.num_edges()} 边 → {out_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--coord-host", default="127.0.0.1")
    parser.add_argument("--coord-port", type=int, default=9000)
    parser.add_argument("--data-file")
    parser.add_argument("--num-parts", type=int, default=5)
    parser.add_argument("--generate", nargs=4, metavar=("NODES","DENSITY","SEED","OUT"))
    args = parser.parse_args()

    if args.generate:
        nodes, dens, seed, out = args.generate
        part = int(args.worker_id.split("_")[-1]) if "_" in args.worker_id else 0
        generate_data(out, int(nodes), float(dens), int(seed), part, args.num_parts)
        return

    w = WorkerNode(args.worker_id, args.host, args.port, args.coord_host, args.coord_port, args.data_file, args.num_parts)
    w.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        w.stop()

if __name__ == "__main__":
    main()
