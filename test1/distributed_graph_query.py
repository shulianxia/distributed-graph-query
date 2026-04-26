#!/usr/bin/env python3
"""
分布式图查询系统 — 完整实现
选题3：分布式图查询程序设计

功能：
- 分布式图数据存储（一致性哈希分区）
- 邻居查询（单节点 + 批量）
- 三角关系查询（分布式算法）
- 性能基准测试（1k-10k节点规模）

依赖：仅 Python 标准库
"""

import hashlib
import json
import logging
import random
import sys
import time
import uuid
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("DistributedGraphQuery")


# ============================================================
# 1. 图存储引擎（单 Worker）
# ============================================================

class GraphStorage:
    """图数据存储 — 邻接表 + 属性存储"""

    def __init__(self):
        self.adjacency = defaultdict(set)       # node_id -> set(neighbor_ids)
        self.node_attrs = defaultdict(dict)     # node_id -> {attr_name: value}
        self.edge_attrs = {}                    # (src, dst) -> {attr_name: value}

    def add_node(self, node_id, **attrs):
        self.node_attrs[node_id].update(attrs)

    def add_edge(self, src, dst, **attrs):
        self.adjacency[src].add(dst)
        self.adjacency[dst].add(src)
        key_forward = (src, dst)
        key_backward = (dst, src)
        self.edge_attrs[key_forward] = attrs
        self.edge_attrs[key_backward] = attrs

    def get_neighbors(self, node_id):
        return self.adjacency.get(node_id, set())

    def get_node_attrs(self, node_id):
        return self.node_attrs.get(node_id, {})

    def get_edge_attrs(self, src, dst):
        return self.edge_attrs.get((src, dst), {})

    def has_edge(self, src, dst):
        return dst in self.adjacency.get(src, set())

    def num_nodes(self):
        return len(self.node_attrs)

    def num_edges(self):
        return len(self.edge_attrs) // 2  # 无向图，每条边存了两份

    def all_edges(self):
        """返回所有无向边（去重）"""
        seen = set()
        edges = []
        for src, dst_set in self.adjacency.items():
            for dst in dst_set:
                key = tuple(sorted((src, dst)))
                if key not in seen:
                    seen.add(key)
                    edges.append(key)
        return edges

    def all_nodes(self):
        return list(self.node_attrs.keys())

    def __repr__(self):
        return f"GraphStorage(nodes={self.num_nodes()}, edges={self.num_edges()})"


# ============================================================
# 2. 分布式图分区 — 一致性哈希
# ============================================================

class DistributedGraph:
    """分布式图 — 一致性哈希分区，多 Worker 协作"""

    def __init__(self, num_partitions=10):
        self.num_partitions = num_partitions
        self.partitions = [GraphStorage() for _ in range(num_partitions)]
        # 分区 → 虚拟Worker地址映射（单机模拟用 partition_id 作为地址）
        self.worker_map = {i: f"worker_{i}" for i in range(num_partitions)}

    def get_partition(self, node_id):
        """一致性哈希：MD5 → 取模"""
        hash_val = hashlib.md5(str(node_id).encode()).hexdigest()
        return int(hash_val, 16) % self.num_partitions

    def get_responsible_workers(self, node_id):
        """获取负责某节点的 Worker（主副本 + 可选冗余）"""
        return [self.worker_map[self.get_partition(node_id)]]

    def add_node(self, node_id, **attrs):
        pid = self.get_partition(node_id)
        self.partitions[pid].add_node(node_id, **attrs)

    def add_edge(self, src, dst, **attrs):
        """分布式添加边：边同时属于 src 和 dst 两个分区（交叉复制）"""
        src_pid = self.get_partition(src)
        dst_pid = self.get_partition(dst)
        self.partitions[src_pid].add_edge(src, dst, **attrs)
        if src_pid != dst_pid:
            self.partitions[dst_pid].add_edge(src, dst, **attrs)

    def load_graph(self, num_nodes, edge_density=0.02, seed=42):
        """生成随机图用于测试"""
        random.seed(seed)
        nodes = list(range(num_nodes))
        # 添加节点
        for n in nodes:
            self.add_node(n, label=f"Node_{n}", degree=0)
        avg_degree = max(1, int(num_nodes * edge_density))
        for n in nodes:
            possible = [x for x in nodes if x != n and not self.partitions[self.get_partition(n)].has_edge(n, x)]
            targets = random.sample(possible, min(avg_degree, len(possible)))
            for t in targets:
                self.add_edge(n, t, weight=random.randint(1, 10))
        logger.info(f"图加载完成: {num_nodes} 节点, {self.total_edges()} 条边, "
                    f"平均度数 ≈ {2 * self.total_edges() / num_nodes:.1f}")

    def total_edges(self):
        return sum(p.num_edges() for p in self.partitions)

    def total_nodes(self):
        return sum(p.num_nodes() for p in self.partitions)

    def partition_stats(self):
        stats = []
        for i, p in enumerate(self.partitions):
            stats.append({"partition": i, "nodes": p.num_nodes(), "edges": p.num_edges()})
        return stats

    def print_stats(self):
        print("\n" + "=" * 60)
        print(f"分布式图状态 — {self.num_partitions} 个分区")
        print("=" * 60)
        total_n, total_e = 0, 0
        for i, p in enumerate(self.partitions):
            print(f"  分区 {i:2d}: {p.num_nodes():>5} 节点, {p.num_edges():>6} 条边")
            total_n += p.num_nodes()
            total_e += p.num_edges()
        print(f"  {'合计':>8}: {total_n:>5} 节点, {total_e:>6} 条边 (去重后 ≈ {total_e // 2})")
        avg_deg = 2 * total_e / total_n if total_n > 0 else 0
        print(f"  平均度数: {avg_deg:.2f}")
        print("=" * 60)


# ============================================================
# 3. 查询引擎 — 邻居查询
# ============================================================

class NeighborQuery:
    """邻居查询 — 跨分区协调"""

    @staticmethod
    def query(graph, node_id):
        """查询某节点的所有邻居（跨分区）"""
        pid = graph.get_partition(node_id)
        neighbors = graph.partitions[pid].get_neighbors(node_id)
        attrs = graph.partitions[pid].get_node_attrs(node_id)
        return {"node": node_id, "attrs": attrs, "neighbors": sorted(neighbors), "degree": len(neighbors)}

    @staticmethod
    def batch_query(graph, node_ids):
        """批量邻居查询"""
        return [NeighborQuery.query(graph, nid) for nid in node_ids]

    @staticmethod
    def common_neighbors(graph, node_a, node_b):
        """查询两个节点的共同邻居"""
        pid_a = graph.get_partition(node_a)
        pid_b = graph.get_partition(node_b)
        neighbors_a = graph.partitions[pid_a].get_neighbors(node_a)
        neighbors_b = graph.partitions[pid_b].get_neighbors(node_b)
        common = neighbors_a & neighbors_b
        return {"node_a": node_a, "node_b": node_b,
                "common_neighbors": sorted(common), "count": len(common)}


# ============================================================
# 4. 三角关系查询 — 分布式算法
# ============================================================

class TriangleQuery:
    """
    分布式三角关系查询算法（基于边迭代）

    策略：
    1. 每个 Worker 本地枚举可能形成三角形的边对
    2. 按度排序优化：只从度数更小的节点出发检查
    3. 去重：统一由 Coordinator 去重
    """

    def __init__(self):
        self.results_cache = {}

    def find_triangles(self, graph, node_id=None):
        """
        分布式三角查找。
        如果 node_id 为 None，扫描全图（所有分区）。
        否则只查包含该节点的三角形。
        """
        if node_id is not None:
            return self._find_triangles_for_node(graph, node_id)
        else:
            return self._find_all_triangles(graph)

    def _find_triangles_for_node(self, graph, node_id):
        """查找包含指定节点的所有三角形"""
        pid = graph.get_partition(node_id)
        neighbors = graph.partitions[pid].get_neighbors(node_id)
        triangles = set()

        # 对每个邻居，查询其邻居列表（可能跨分区）
        for u in neighbors:
            u_pid = graph.get_partition(u)
            u_neighbors = graph.partitions[u_pid].get_neighbors(u)
            common = neighbors & u_neighbors
            for v in common:
                tri = tuple(sorted((node_id, u, v)))
                triangles.add(tri)

        return sorted(triangles)

    def _find_all_triangles(self, graph):
        """
        全图三角查找 — 分布式算法
        思路：对每条边 (u, v)，检查 u 和 v 的共同邻居。
        按度排序优化：只从度数较小的节点出发枚举邻居。
        """
        triangles = set()

        # 收集各分区所有边，去重
        all_edges = set()
        for p in graph.partitions:
            for (src, dst) in p.all_edges():
                edge = tuple(sorted((src, dst)))
                all_edges.add(edge)

        # 计算每个节点的度（用于优化）
        degree_map = defaultdict(int)
        for u, v in all_edges:
            degree_map[u] += 1
            degree_map[v] += 1

        # 对每条边，从度数更小的节点出发查找共同邻居
        for u, v in all_edges:
            # 优化：从度数较小的节点出发查询邻居
            if degree_map[u] < degree_map[v]:
                pivot, other = u, v
            else:
                pivot, other = v, u

            pivot_pid = graph.get_partition(pivot)
            pivot_neighbors = graph.partitions[pivot_pid].get_neighbors(pivot)

            other_pid = graph.get_partition(other)
            other_neighbors = graph.partitions[other_pid].get_neighbors(other)

            common = pivot_neighbors & other_neighbors
            for w in common:
                tri = tuple(sorted((u, v, w)))
                triangles.add(tri)

        return sorted(triangles)

    def count_triangles(self, graph, node_id=None):
        """统计三角形数量"""
        tris = self.find_triangles(graph, node_id)
        if node_id is not None:
            return {"node": node_id, "triangle_count": len(tris), "triangles": tris}
        return {"total_triangles": len(tris), "triangles": tris}


# ============================================================
# 5. 交互协议（消息层）
# ============================================================

class GraphQueryProtocol:
    """查询交互协议 — JSON over TCP（消息定义）"""

    # 消息类型
    MSG_QUERY_NEIGHBORS = 0x01      # 查询邻居
    MSG_QUERY_TRIANGLES = 0x02      # 查询三角关系
    MSG_QUERY_COMMON_NEIGHBORS = 0x03  # 查询共同邻居
    MSG_RESULT_PARTIAL = 0x04       # 部分结果
    MSG_RESULT_FINAL = 0x05         # 最终结果
    MSG_ERROR = 0x06                # 错误信息
    MSG_HEARTBEAT = 0x07            # 心跳
    MSG_PARTITION_SYNC = 0x08       # 分区同步请求

    @staticmethod
    def create_message(msg_type, sender, payload):
        return {
            "msg_id": str(uuid.uuid4()),
            "msg_type": msg_type,
            "sender": sender,
            "timestamp": time.time(),
            "payload": payload,
        }

    @staticmethod
    def serialize(msg):
        return json.dumps(msg, ensure_ascii=False)

    @staticmethod
    def deserialize(data):
        return json.loads(data)


# ============================================================
# 6. 协调节点（Coordinator）
# ============================================================

class Coordinator:
    """协调节点 — 查询计划生成 + 结果合并"""

    def __init__(self, graph):
        self.graph = graph
        self.protocol = GraphQueryProtocol()

    def query_neighbors(self, node_id):
        """邻居查询协调"""
        start = time.perf_counter()
        result = NeighborQuery.query(self.graph, node_id)
        elapsed = time.perf_counter() - start
        result["query_time_ms"] = round(elapsed * 1000, 2)
        return result

    def query_common_neighbors(self, node_a, node_b):
        """共同邻居查询协调"""
        start = time.perf_counter()
        result = NeighborQuery.common_neighbors(self.graph, node_a, node_b)
        elapsed = time.perf_counter() - start
        result["query_time_ms"] = round(elapsed * 1000, 2)
        return result

    def query_triangles(self, node_id=None):
        """三角关系查询协调"""
        start = time.perf_counter()
        tq = TriangleQuery()
        result = tq.count_triangles(self.graph, node_id)
        elapsed = time.perf_counter() - start
        result["query_time_ms"] = round(elapsed * 1000, 2)
        return result


# ============================================================
# 7. 性能基准测试
# ============================================================

class Benchmark:
    """性能基准测试 — 验证 1k-10k 节点规模"""

    def __init__(self):
        self.results = []

    def run_single(self, num_nodes, edge_density=0.02, num_partitions=10):
        logger.info(f"\n{'=' * 60}")
        logger.info(f"基准测试: {num_nodes} 节点, {num_partitions} 分区, 密度={edge_density}")
        logger.info(f"{'=' * 60}")

        graph = DistributedGraph(num_partitions=num_partitions)
        load_start = time.perf_counter()
        graph.load_graph(num_nodes, edge_density)
        load_time = time.perf_counter() - load_start

        coord = Coordinator(graph)
        report = {"num_nodes": num_nodes, "num_partitions": num_partitions,
                  "edge_density": edge_density, "load_time_s": round(load_time, 3)}

        # 邻居查询测试（随机采样20个节点）
        sample_nodes = random.sample(range(num_nodes), min(20, num_nodes))
        nq_times = []
        for n in sample_nodes:
            r = coord.query_neighbors(n)
            nq_times.append(r["query_time_ms"])
        report["neighbor_query"] = {
            "samples": len(nq_times),
            "avg_ms": round(sum(nq_times) / len(nq_times), 3),
            "min_ms": round(min(nq_times), 3),
            "max_ms": round(max(nq_times), 3),
        }

        # 共同邻居查询测试
        cn_times = []
        for i in range(min(10, num_nodes // 2)):
            a = random.randint(0, num_nodes - 1)
            b = random.randint(0, num_nodes - 1)
            r = coord.query_common_neighbors(a, b)
            cn_times.append(r["query_time_ms"])
        report["common_neighbor_query"] = {
            "samples": len(cn_times),
            "avg_ms": round(sum(cn_times) / len(cn_times), 3),
        }

        # 三角关系查询（单节点）
        tq = TriangleQuery()
        tri_start = time.perf_counter()
        tri_result = tq.find_triangles(graph, sample_nodes[0])
        tri_time = (time.perf_counter() - tri_start) * 1000
        report["triangle_query_single"] = {
            "node": sample_nodes[0],
            "triangles_found": len(tri_result),
            "time_ms": round(tri_time, 3),
        }

        # 全图三角计数（仅小规模）
        if num_nodes <= 2000:
            all_tri_start = time.perf_counter()
            all_tri = tq.find_triangles(graph)
            all_tri_time = (time.perf_counter() - all_tri_start) * 1000
            report["triangle_query_full"] = {
                "total_triangles": len(all_tri),
                "time_ms": round(all_tri_time, 3),
            }

        # 分区均衡性统计
        stats = graph.partition_stats()
        node_counts = [s["nodes"] for s in stats]
        edge_counts = [s["edges"] for s in stats]
        report["partition_balance"] = {
            "nodes": {"mean": round(sum(node_counts) / len(node_counts), 1),
                      "std": round((sum((c - sum(node_counts) / len(node_counts)) ** 2
                                       for c in node_counts) / len(node_counts)) ** 0.5, 1),
                      "min": min(node_counts), "max": max(node_counts)},
            "edges": {"mean": round(sum(edge_counts) / len(edge_counts), 1),
                      "std": round((sum((c - sum(edge_counts) / len(edge_counts)) ** 2
                                       for c in edge_counts) / len(edge_counts)) ** 0.5, 1),
                      "min": min(edge_counts), "max": max(edge_counts)},
        }

        self.results.append(report)
        graph.print_stats()
        print(f"\n[邻居查询] 均值={report['neighbor_query']['avg_ms']}ms "
              f"(范围 {report['neighbor_query']['min_ms']}-{report['neighbor_query']['max_ms']}ms)")
        print(f"[共同邻居] 均值={report['common_neighbor_query']['avg_ms']}ms")
        print(f"[三角查询(单节点)] {report['triangle_query_single']['triangles_found']} 个三角形, "
              f"耗时 {report['triangle_query_single']['time_ms']}ms")
        if "triangle_query_full" in report:
            print(f"[全图三角计数] {report['triangle_query_full']['total_triangles']} 个三角形, "
                  f"耗时 {report['triangle_query_full']['time_ms']}ms")
        print(f"[分区均衡] 节点 std={report['partition_balance']['nodes']['std']}, "
              f"边 std={report['partition_balance']['edges']['std']}")

        return report

    def run_all(self):
        scales = [1000, 2000, 5000, 10000]
        for nodes in scales:
            self.run_single(nodes, edge_density=0.02, num_partitions=10)
        self.print_summary()

    def print_summary(self):
        print("\n\n" + "=" * 70)
        print("                      性能基准测试汇总")
        print("=" * 70)
        print(f"{'节点数':>8} {'加载时间':>10} {'邻居查询(ms)':>14} {'共同邻居(ms)':>14} "
              f"{'三角(单节点)':>14} {'分区均衡(std)':>12}")
        print("-" * 70)
        for r in self.results:
            nq = r["neighbor_query"]
            cn = r["common_neighbor_query"]
            tq = r["triangle_query_single"]
            pb = r["partition_balance"]["nodes"]["std"]
            print(f"{r['num_nodes']:>8} {r['load_time_s']:>8.3f}s {nq['avg_ms']:>10.3f}ms "
                  f"{cn['avg_ms']:>10.3f}ms {tq['time_ms']:>10.3f}ms "
                  f"{pb:>8.1f}")
        print("=" * 70)


# ============================================================
# 8. 演示 / 交互式查询
# ============================================================

def demo():
    """完整演示：建图 → 查询邻居 → 查询共同邻居 → 三角关系"""
    print("\n" + "=" * 60)
    print("  分布式图查询系统 — 功能演示")
    print("=" * 60)

    # 创建500节点的小图用于演示
    graph = DistributedGraph(num_partitions=5)
    graph.load_graph(500, edge_density=0.03, seed=123)
    graph.print_stats()

    coord = Coordinator(graph)
    tq = TriangleQuery()

    # 1. 邻居查询
    print("\n--- 1. 邻居查询 ---")
    for node_id in [5, 42, 100]:
        result = coord.query_neighbors(node_id)
        deg = result["degree"]
        sample = result["neighbors"][:8]
        print(f"  节点 {result['node']}: 度={deg}, 部分邻居={sample}...")

    # 2. 共同邻居
    print("\n--- 2. 共同邻居查询 ---")
    for a, b in [(5, 42), (10, 20), (100, 200)]:
        result = coord.query_common_neighbors(a, b)
        print(f"  节点 {a} & {b}: 共同邻居={result['count']} 个 -> {result['common_neighbors'][:6]}...")

    # 3. 三角关系（单节点）
    print("\n--- 3. 三角关系查询（单节点） ---")
    for node in [5, 42]:
        tris = tq.find_triangles(graph, node)
        print(f"  节点 {node}: 包含在 {len(tris)} 个三角形中")
        if tris:
            print(f"    示例: {tris[:5]}")

    # 4. 全图三角计数
    print("\n--- 4. 全图三角计数 ---")
    all_tris = tq.find_triangles(graph)
    print(f"  全图共 {len(all_tris)} 个三角形")
    if all_tris:
        print(f"  示例: {all_tris[:5]}")

    print("\n" + "=" * 60)
    print("  演示完成")
    print("=" * 60)


def run_benchmark():
    bm = Benchmark()
    bm.run_all()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "benchmark":
        run_benchmark()
    else:
        demo()
