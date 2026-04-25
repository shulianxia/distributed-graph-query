#!/usr/bin/env python3
"""
分布式图查询系统 — 共享协议
"""
import hashlib, json, logging, socket, struct, time, uuid
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("DGProto")

# ── 图存储 ──
class GraphStorage:
    def __init__(self, directed=False):
        self.directed = directed
        self.adjacency = defaultdict(list)
        self.node_attrs = defaultdict(dict)

    def add_node(self, nid, **attrs):
        self.node_attrs[nid].update(attrs)
        if nid not in self.adjacency:
            self.adjacency[nid] = []

    def add_edge(self, src, dst, **attrs):
        self.add_node(src); self.add_node(dst)
        if dst not in self.adjacency[src]:
            self.adjacency[src].append(dst)
        if not self.directed and src not in self.adjacency[dst]:
            self.adjacency[dst].append(src)

    def get_neighbors(self, nid):
        return list(self.adjacency.get(nid, []))

    def has_edge(self, src, dst):
        return dst in self.adjacency.get(src, [])

    def num_nodes(self):
        return len(self.node_attrs)

    def num_edges(self):
        seen = set()
        for s, nbrs in self.adjacency.items():
            for d in nbrs:
                seen.add(tuple(sorted((s, d))) if not self.directed else (s, d))
        return len(seen)

    def all_edges(self):
        seen = set()
        out = []
        for s, nbrs in self.adjacency.items():
            for d in nbrs:
                k = tuple(sorted((s, d))) if not self.directed else (s, d)
                if k not in seen:
                    seen.add(k); out.append(k)
        return out

    def all_nodes(self):
        return list(self.node_attrs.keys())

    def to_dict(self):
        return {
            "adjacency": {str(k): v for k, v in self.adjacency.items()},
            "node_attrs": {str(k): dict(v) for k, v in self.node_attrs.items()},
            "directed": self.directed,
        }

    @classmethod
    def from_dict(cls, d):
        g = cls(directed=d.get("directed", False))
        for k, v in d["node_attrs"].items():
            g.add_node(int(k), **v)
        for k, v in d["adjacency"].items():
            g.adjacency[int(k)] = v
        return g

# ── 一致性哈希 ──
def get_partition(node_id, num_partitions):
    return int(hashlib.md5(str(node_id).encode()).hexdigest(), 16) % num_partitions

# ── 消息类型 ──
MSG_REGISTER     = 0x01
MSG_REGISTER_ACK = 0x02
MSG_HEARTBEAT    = 0x10
MSG_HEARTBEAT_ACK= 0x11

MSG_QUERY_NEIGHBOR = 0x20   # 邻居查询（完整信息）
MSG_QUERY_COMMON   = 0x21   # 共同邻居
MSG_QUERY_TRIANGLE = 0x22   # 三角查询（单节点 / 全图）
MSG_QUERY_EDGES    = 0x23   # 收集全图边
MSG_QUERY_NLIST    = 0x24   # 只返回邻居列表（轻量，用于三角协同）

MSG_RESULT_OK    = 0x30
MSG_RESULT_ERR   = 0x31
MSG_SHUTDOWN     = 0xFF

def make_msg(msg_type, sender, payload):
    return {"msg_id": str(uuid.uuid4()), "msg_type": msg_type, "sender": sender,
            "timestamp": time.time(), "payload": payload}

def pack_msg(d):
    body = json.dumps(d, ensure_ascii=False).encode("utf-8")
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

# ── 本地查询引擎 ──
class LocalQuery:
    def __init__(self, storage):
        self.storage = storage

    def query_neighbor(self, nid):
        nbrs = self.storage.get_neighbors(nid)
        return {"node": nid, "neighbors": sorted(nbrs), "degree": len(nbrs),
                "attrs": dict(self.storage.node_attrs.get(nid, {}))}

    def query_nlist(self, nid):
        return {"node": nid, "neighbors": sorted(self.storage.get_neighbors(nid))}

    def query_common(self, a, b):
        na = set(self.storage.get_neighbors(a))
        nb = set(self.storage.get_neighbors(b))
        common = sorted(na & nb)
        return {"node_a": a, "node_b": b, "common_neighbors": common, "count": len(common)}

    def triangle_node_local(self, nid):
        """查本 Worker 存储范围内，节点 nid 参与的三角形"""
        nbrs = set(self.storage.get_neighbors(nid))
        tris = set()
        for u in nbrs:
            un = set(self.storage.get_neighbors(u))
            for v in nbrs & un:
                tris.add(tuple(sorted((nid, u, v))))
        return sorted(tris)

    def all_triangles_local(self):
        """本 Worker 能独立算出的所有三角形（仅限于所有顶点都在本分区的三角）"""
        tris = set()
        for n in self.storage.all_nodes():
            for tri in self.triangle_node_local(n):
                tris.add(tri)
        return sorted(tris)

    def all_edges_iter(self):
        seen = set()
        for s, nbrs in self.storage.adjacency.items():
            for d in nbrs:
                k = tuple(sorted((s, d))) if not self.storage.directed else (s, d)
                if k not in seen:
                    seen.add(k); yield k
