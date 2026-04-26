#!/usr/bin/env python3
"""验证每个 Worker 是否有足够数据做本地全图三角计数"""
import json, sys
from collections import defaultdict
sys.path.insert(0, ".")
from protocol import GraphStorage, get_partition

# 加载各分区
gt_nodes = set()
gt_edges = set()
part_edges = {}

for i in range(5):
    with open(f"workers/part_{i}.json") as f:
        d = json.load(f)
    g = GraphStorage.from_dict(d)
    edges = set(g.all_edges())
    part_edges[i] = edges
    gt_edges.update(edges)
    for src_str in d["adjacency"]:
        gt_nodes.add(int(src_str))

print(f"全图: {len(gt_nodes)} 节点, {len(gt_edges)} 条边")

# 每个分区数据里的边占全图边百分比
for i, edges in part_edges.items():
    pct = len(edges) / len(gt_edges) * 100
    print(f"  part_{i}: {len(edges)}/{len(gt_edges)} 边 = {pct:.1f}%")

# 用每个分区本地算三角，再汇总去重
all_tris = set()
for i, edges in part_edges.items():
    with open(f"workers/part_{i}.json") as f:
        d = json.load(f)
    g = GraphStorage.from_dict(d)
    local = set()
    for u in g.all_nodes():
        nbrs = set(g.get_neighbors(u))
        for v in nbrs:
            if v <= u: continue
            for w in nbrs & set(g.get_neighbors(v)):
                if w > v:
                    local.add(tuple(sorted((u,v,w))))
    all_tris.update(local)
    print(f"  part_{i} 本地三角: {len(local)}")

print(f"\n各Worker本地三角汇总（去重后）: {len(all_tris)}")

# 完整图地面真值
full_adj = defaultdict(set)
for i in range(5):
    with open(f"workers/part_{i}.json") as f:
        d = json.load(f)
    for s_str, nbrs in d["adjacency"].items():
        s = int(s_str)
        for n in nbrs:
            full_adj[s].add(n)
            full_adj[n].add(s)

true_tris = set()
for u in full_adj:
    for v in full_adj[u]:
        if v <= u: continue
        for w in full_adj[u] & full_adj[v]:
            if w > v:
                true_tris.add(tuple(sorted((u,v,w))))

print(f"完整图真实三角: {len(true_tris)}")
print(f"匹配: {all_tris == true_tris}")
