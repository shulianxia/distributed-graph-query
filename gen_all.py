#!/usr/bin/env python3
"""生成分布式图数据 — 一次性生成全图边，按分区分发到各文件"""
import hashlib, json, os, random, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import get_partition

def generate_all(num_nodes, density=0.08, seed=42, num_parts=5, out_dir="workers"):
    os.makedirs(out_dir, exist_ok=True)
    random.seed(seed)

    # 生成所有边
    avg_deg = max(1, int(num_nodes * density))
    edges = set()
    for u in range(num_nodes):
        possible = [v for v in range(num_nodes) if v != u and (u, v) not in edges and (v, u) not in edges]
        targets = random.sample(possible, min(avg_deg, len(possible)))
        for v in targets:
            edges.add(tuple(sorted((u, v))))

    print(f"全图边总数: {len(edges)} （{num_nodes} 节点）")

    # 按分区分配边的存储关系
    # 边 (u,v) 应该出现在：
    #   - 分区 get_partition(u) 的 adjacency[u] 中
    #   - 分区 get_partition(v) 的 adjacency[v] 中
    from collections import defaultdict

    part_adj = [defaultdict(list) for _ in range(num_parts)]
    part_nodes = [set() for _ in range(num_parts)]

    for u, v in edges:
        pu = get_partition(u, num_parts)
        pv = get_partition(v, num_parts)
        # u 的邻接表加到 pu 分区
        part_adj[pu][u].append(v)
        part_nodes[pu].add(u)
        part_nodes[pu].add(v)
        # v 的邻接表加到 pv 分区
        part_adj[pv][v].append(u)
        part_nodes[pv].add(u)
        part_nodes[pv].add(v)

    for i in range(num_parts):
        data = {
            "adjacency": {str(k): sorted(v) for k, v in sorted(part_adj[i].items())},
            "node_attrs": {str(n): {"label": f"n{n}"} for n in sorted(part_nodes[i])},
            "directed": False,
        }
        out = os.path.join(out_dir, f"part_{i}.json")
        with open(out, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        total_edges = sum(len(v) for v in part_adj[i].values())
        print(f"  part_{i}: {len(part_nodes[i])} 节点, {total_edges} 条邻接条目 → {out}")

    # 验证：每条边在两个分区中倒序出现
    for u, v in edges:
        pu, pv = get_partition(u, num_parts), get_partition(v, num_parts)
        if v not in part_adj[pu].get(u, []):
            print(f"警告: 边 ({u},{v}) 在分区 {pu} 中缺失!")
        if u not in part_adj[pv].get(v, []):
            print(f"警告: 边 ({u},{v}) 在分区 {pv} 中缺失!")

    # 完整图三角验证
    full = defaultdict(set)
    for i in range(num_parts):
        for k, vals in part_adj[i].items():
            for v in vals:
                full[k].add(v)
                full[v].add(k)
    gt = set()
    for u in full:
        for v in full[u]:
            if v <= u: continue
            for w in full[u] & full[v]:
                if w > v:
                    gt.add(tuple(sorted((u,v,w))))
    print(f"完整图三角（验证基准）: {len(gt)} 个")
    return len(gt)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("nodes", type=int, default=50, nargs="?")
    parser.add_argument("density", type=float, default=0.08, nargs="?")
    parser.add_argument("seed", type=int, default=42, nargs="?")
    parser.add_argument("--num-parts", type=int, default=5)
    parser.add_argument("--out-dir", default="workers")
    args = parser.parse_args()
    generate_all(args.nodes, args.density, args.seed, args.num_parts, args.out_dir)
