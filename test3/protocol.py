#!/usr/bin/env python3
"""
分布式图查询系统 — test3 共享协议

连接池 + 批量 nlist 优化：
- Coordinator 与 Worker 之间复用长连接（ConnPool）
- 批量 nlist 查询代替逐边 RPC
- 普通查询（neighbor/common/triangle）保持短连接
"""
import hashlib, json, logging, socket, struct, threading, time, uuid
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

# ── 本地查询（Worker 内部使用） ──
class LocalQuery:
    def __init__(self, storage):
        self.g = storage  # GraphStorage 实例

    def neighbors(self, node):
        return set(self.g.adjacency.get(int(node), []))

    def common_neighbors(self, a, b):
        return sorted(self.neighbors(a) & self.neighbors(b))

    def get_edges(self):
        return self.g.all_edges()

    def query_neighbor(self, nid):
        nbrs = self.g.get_neighbors(nid)
        return {"node": nid, "neighbors": sorted(nbrs), "degree": len(nbrs)}

    def query_nlist(self, nid):
        return {"node": nid, "neighbors": sorted(self.g.get_neighbors(nid))}

    def query_common(self, a, b):
        na = set(self.g.get_neighbors(a))
        nb = set(self.g.get_neighbors(b))
        common = sorted(na & nb)
        return {"node_a": a, "node_b": b, "common_neighbors": common, "count": len(common)}

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
MSG_QUERY_NEIGHBOR  = 0x20  # 别名（兼容）
MSG_QUERY_COMMON    = 0x21
MSG_QUERY_TRIANGLE  = 0x22
MSG_QUERY_EDGES     = 0x23
MSG_QUERY_NLIST_BULK = 0x24
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

# ── 连接池（test3 核心优化：Coordinator→Worker 长连接复用） ──
class ConnPool:
    def __init__(self):
        self._conns = {}     # wid → (sock, host, port)
        self._lock = threading.Lock()

    def get(self, wid, host, port):
        with self._lock:
            entry = self._conns.get(wid)
            if entry:
                sock, _, _ = entry
                try:
                    sock.settimeout(0.1)
                    sock.sendall(b"")  # 空字节检测活跃
                    sock.settimeout(30)
                    return sock
                except:
                    try: sock.close()
                    except: pass
                    del self._conns[wid]
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((host, port))
            self._conns[wid] = (sock, host, port)
            return sock

    def remove(self, wid):
        with self._lock:
            entry = self._conns.pop(wid, None)
            if entry:
                try: entry[0].close()
                except: pass

    def close_all(self):
        with self._lock:
            for wid, (sock, _, _) in self._conns.items():
                try: sock.close()
                except: pass
            self._conns.clear()

    def all_wids(self):
        with self._lock:
            return list(self._conns.keys())
