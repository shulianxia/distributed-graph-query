#!/usr/bin/env python3
"""
分布式进程通信图查询系统 — 共享协议

与 test3 复用完全相同的协议栈：4字节长度前缀 + JSON
新增：
  - 进程节点属性（cmdline, user, state, comm）
  - 边属性（edge_type: parent_child / pipe / unix_socket / net_socket / signal）
"""
import hashlib, json, logging, socket, struct, threading, time, uuid
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("DGProto")

# ── 图存储 ──
class GraphStorage:
    """继承 test3 的 GraphStorage，扩展边属性支持"""
    def __init__(self, directed=False):
        self.directed = directed
        self.adjacency = defaultdict(list)
        self.edge_attrs = {}         # (u,v) → dict of attrs（无向图存 sorted tuple）
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
        # 存边属性
        key = tuple(sorted((src, dst))) if not self.directed else (src, dst)
        if key not in self.edge_attrs:
            self.edge_attrs[key] = {}
        self.edge_attrs[key].update(attrs)

    def get_neighbors(self, nid):
        return list(self.adjacency.get(int(nid), []))

    def num_nodes(self):
        return len(self.node_attrs)

    def num_edges(self):
        seen = set()
        for s, nbrs in self.adjacency.items():
            for d in nbrs:
                seen.add(tuple(sorted((s, d))) if not self.directed else (s, d))
        return len(seen)

    def all_edges(self):
        seen = set(); out = []
        for s, nbrs in self.adjacency.items():
            for d in nbrs:
                k = tuple(sorted((s, d))) if not self.directed else (s, d)
                if k not in seen:
                    seen.add(k); out.append(k)
        return out

    def all_edges_with_attrs(self):
        """返回带属性的所有边"""
        seen = set(); out = []
        for s, nbrs in self.adjacency.items():
            for d in nbrs:
                k = tuple(sorted((s, d))) if not self.directed else (s, d)
                if k not in seen:
                    seen.add(k)
                    attrs = self.edge_attrs.get(k, {})
                    out.append({"src": k[0], "dst": k[1], **attrs})
        return out

    def to_dict(self):
        return {
            "adjacency": {str(k): v for k, v in self.adjacency.items()},
            "node_attrs": {str(k): dict(v) for k, v in self.node_attrs.items()},
            "edge_attrs": {f"{a},{b}": dict(attrs) for (a,b), attrs in self.edge_attrs.items()},
            "directed": self.directed,
        }

    @classmethod
    def from_dict(cls, d):
        g = cls(directed=d.get("directed", False))
        for k, v in d["node_attrs"].items():
            g.add_node(int(k), **v)
        for k, v in d["adjacency"].items():
            g.adjacency[int(k)] = v
        # 恢复边属性
        for key_str, attrs in d.get("edge_attrs", {}).items():
            parts = key_str.split(",")
            a, b = int(parts[0]), int(parts[1])
            g.edge_attrs[(a, b)] = attrs
        return g

# ── 本地查询（Worker 内部使用） ──
class LocalQuery:
    def __init__(self, storage):
        self.g = storage

    def neighbors(self, node):
        return set(self.g.adjacency.get(int(node), []))

    def common_neighbors(self, a, b):
        return sorted(self.neighbors(a) & self.neighbors(b))

    def get_edges(self):
        return self.g.all_edges()

    def get_edges_with_attrs(self):
        return self.g.all_edges_with_attrs()

    def query_neighbor(self, nid):
        nbrs = self.g.get_neighbors(nid)
        n_attrs = dict(self.g.node_attrs.get(int(nid), {}))
        return {"node": nid, "neighbors": sorted(nbrs), "degree": len(nbrs), "attrs": n_attrs}

    def query_nlist(self, nid):
        return {"node": nid, "neighbors": sorted(self.g.get_neighbors(nid))}

    def query_common(self, a, b):
        na = set(self.g.get_neighbors(a))
        nb = set(self.g.get_neighbors(b))
        common = sorted(na & nb)
        return {"node_a": a, "node_b": b, "common_neighbors": common, "count": len(common)}

    def query_node_info(self, nid):
        """返回进程节点详细信息"""
        nid = int(nid)
        attrs = dict(self.g.node_attrs.get(nid, {}))
        nbrs = self.g.get_neighbors(nid)
        edges_with_type = []
        for nb in nbrs:
            key = tuple(sorted((nid, nb)))
            eattrs = self.g.edge_attrs.get(key, {})
            edges_with_type.append({"neighbor": nb, "type": eattrs.get("type", "unknown"), **eattrs})
        return {
            "node": nid,
            "attrs": attrs,
            "degree": len(nbrs),
            "connections": edges_with_type,
        }

    def all_edges_iter(self):
        seen = set()
        for s, nbrs in self.g.adjacency.items():
            for d in nbrs:
                k = tuple(sorted((s, d))) if not self.g.directed else (s, d)
                if k not in seen:
                    seen.add(k); yield k

# ── 一致性哈希 ──
def get_partition(node_id, num_partitions):
    return int(hashlib.md5(str(node_id).encode()).hexdigest(), 16) % num_partitions

# ── 消息类型 ──
MSG_REGISTER       = 0x01
MSG_REGISTER_ACK   = 0x02
MSG_HEARTBEAT      = 0x10
MSG_HEARTBEAT_ACK  = 0x11
MSG_QUERY_NLIST     = 0x20
MSG_QUERY_NEIGHBOR  = 0x20  # 别名
MSG_QUERY_COMMON    = 0x21
MSG_QUERY_TRIANGLE  = 0x22
MSG_QUERY_EDGES     = 0x23
MSG_QUERY_NLIST_BULK = 0x24
MSG_QUERY_NODE_INFO = 0x25   # 新增：进程详细信息
MSG_RESULT_OK      = 0x30
MSG_RESULT_ERR     = 0x31
MSG_SHUTDOWN       = 0xFF

def make_msg(msg_type, sender, payload):
    return {"msg_id": str(uuid.uuid4()), "msg_type": msg_type, "sender": sender,
            "timestamp": time.time(), "payload": payload}

def pack_msg(d):
    body = json.dumps(d, ensure_ascii=False).encode("utf-8")
    return struct.pack("!I", len(body)) + body

def recv_msg(sock):
    raw = sock.recv(4)
    if not raw: return None
    length = struct.unpack("!I", raw)[0]
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk: return None
        body += chunk
    return json.loads(body.decode("utf-8"))

def send_msg(sock, msg):
    sock.sendall(pack_msg(msg))

def recv_or_none(sock):
    try: return recv_msg(sock)
    except: return None
