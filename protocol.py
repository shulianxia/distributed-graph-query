#!/usr/bin/env python3
"""
分布式图查询系统 — 共享协议与图数据结构

这是所有节点（Coordinator / Worker / Client）共享的模块：
  1. GraphStorage       — 图数据存储（同原版）
  2. 消息类型常量        — TCP/JSON 协议定义
  3. 消息编解码          — pack / recv
  4. 一致性哈希          — partition → Worker 映射
"""

import hashlib
import json
import logging
import socket
import struct
import threading
import time
import uuid
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DGProto")


# ====================================================================
# 1. 图数据结构
# ====================================================================

class GraphStorage:
    """本地图存储 — 邻接表 + 节点/边属性"""

    def __init__(self, directed=False):
        self.directed = directed
        self.adjacency = defaultdict(list)
        self.node_attrs = defaultdict(dict)
        self.edge_attrs = {}

    def add_node(self, node_id, **attrs):
        self.node_attrs[node_id].update(attrs)
        if node_id not in self.adjacency:
            self.adjacency[node_id] = []

    def add_edge(self, src, dst, **attrs):
        self.add_node(src)
        self.add_node(dst)
        if dst not in self.adjacency[src]:
            self.adjacency[src].append(dst)
        if not self.directed and src not in self.adjacency[dst]:
            self.adjacency[dst].append(src)
        self.edge_attrs[(src, dst)] = attrs
        if not self.directed:
            self.edge_attrs[(dst, src)] = attrs

    def get_neighbors(self, node_id):
        return self.adjacency.get(node_id, [])

    def get_node_attrs(self, node_id):
        return self.node_attrs.get(node_id, {})

    def has_edge(self, src, dst):
        return dst in self.adjacency.get(src, [])

    def num_nodes(self):
        return len(self.node_attrs)

    def num_edges(self):
        if self.directed:
            return len(self.edge_attrs)
        return len(self.edge_attrs) // 2

    def all_edges(self):
        seen = set()
        edges = []
        for src, neighbors in self.adjacency.items():
            for dst in neighbors:
                if self.directed:
                    key = (src, dst)
                else:
                    key = tuple(sorted((src, dst)))
                if key not in seen:
                    seen.add(key)
                    edges.append(key)
        return edges

    def all_nodes(self):
        return list(self.node_attrs.keys())

    def to_dict(self):
        """序列化为 JSON 友好的 dict（用于数据分发）"""
        return {
            "directed": self.directed,
            "adjacency": {str(k): v for k, v in self.adjacency.items()},
            "node_attrs": {str(k): dict(v) for k, v in self.node_attrs.items()},
            "edge_attrs": {
                f"{a},{b}": dict(v) for (a, b), v in self.edge_attrs.items()
            },
        }

    @classmethod
    def from_dict(cls, data):
        g = cls(directed=data["directed"])
        for k, v in data["node_attrs"].items():
            g.add_node(int(k), **v)
        for k, v in data["adjacency"].items():
            src = int(k)
            g.adjacency[src] = v
        for k, v in data["edge_attrs"].items():
            a, b = map(int, k.split(","))
            g.edge_attrs[(a, b)] = v
        return g


# ====================================================================
# 2. 一致性哈希
# ====================================================================

def get_partition(node_id, num_partitions):
    """MD5 哈希 → 取模 → 分区号"""
    h = hashlib.md5(str(node_id).encode()).hexdigest()
    return int(h, 16) % num_partitions


# ====================================================================
# 3. TCP/JSON 协议 — 消息类型 & 编解码
# ====================================================================

# ── 消息类型 ──
MSG_REGISTER      = 0x01   # Worker → Coordinator: 注册自身
MSG_REGISTER_ACK  = 0x02   # Coordinator → Worker: 确认注册，返回所有Worker列表
MSG_HEARTBEAT     = 0x10   # 心跳
MSG_HEARTBEAT_ACK = 0x11   # 心跳应答

MSG_QUERY_NEIGHBOR      = 0x20   # 邻居查询
MSG_QUERY_COMMON        = 0x21   # 共同邻居查询
MSG_QUERY_TRIANGLE      = 0x22   # 三角查询（单节点 / 全图边检测）
MSG_QUERY_EDGES         = 0x23   # Coordinator → Worker: 收集全图边
MSG_QUERY_CHECK_TRI     = 0x24   # Coordinator → Worker: 检查指定边是否构成三角

MSG_RESULT_OK    = 0x30   # 成功结果
MSG_RESULT_ERR   = 0x31   # 错误结果
MSG_SHUTDOWN     = 0xFF   # 关闭


def make_msg(msg_type, sender, payload):
    return {
        "msg_id": str(uuid.uuid4()),
        "msg_type": msg_type,
        "sender": sender,
        "timestamp": time.time(),
        "payload": payload,
    }


def pack_msg(msg_dict):
    body = json.dumps(msg_dict, ensure_ascii=False).encode("utf-8")
    return struct.pack("!I", len(body)) + body


def recv_msg(sock):
    raw = sock.recv(4)
    if not raw:
        return None
    length = struct.unpack("!I", raw)[0]
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            return None
        body += chunk
    return json.loads(body.decode("utf-8"))


# ====================================================================
# 4. 查询引擎（可独立运行，也可 Worker 本地调用）
# ====================================================================

class LocalQuery:
    """Worker 本地的查询执行器"""

    def __init__(self, storage: GraphStorage):
        self.storage = storage

    def query_neighbor(self, node_id):
        nbrs = self.storage.get_neighbors(node_id)
        attrs = self.storage.get_node_attrs(node_id)
        return {
            "node": node_id,
            "attrs": dict(attrs),
            "neighbors": sorted(nbrs),
            "degree": len(nbrs),
        }

    def query_common(self, a, b):
        na = set(self.storage.get_neighbors(a))
        nb = set(self.storage.get_neighbors(b))
        common = sorted(na & nb)
        return {
            "node_a": a,
            "node_b": b,
            "common_neighbors": common,
            "count": len(common),
        }

    def query_triangle_node(self, node_id):
        """查询指定节点参与的所有三角形（本地）"""
        neighbors = set(self.storage.get_neighbors(node_id))
        triangles = set()
        for u in neighbors:
            u_nbrs = set(self.storage.get_neighbors(u))
            for v in neighbors & u_nbrs:
                tri = tuple(sorted((node_id, u, v)))
                triangles.add(tri)
        return sorted(triangles)

    def all_edges_iter(self):
        """生成本地所有无重复边"""
        seen = set()
        for src, nbrs in self.storage.adjacency.items():
            for dst in nbrs:
                if self.storage.directed:
                    key = (src, dst)
                else:
                    key = tuple(sorted((src, dst)))
                if key not in seen:
                    seen.add(key)
                    yield key

    def check_triangle(self, u, v):
        """检查两节点是否在本地有共同邻居（三角检测原语）"""
        u_nbrs = set(self.storage.get_neighbors(u))
        v_nbrs = set(self.storage.get_neighbors(v))
        common = u_nbrs & v_nbrs
        triangles = [tuple(sorted((u, v, w))) for w in sorted(common)]
        return triangles
