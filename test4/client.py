#!/usr/bin/env python3
"""
进程通信图查询系统 — 命令行客户端

进程通信场景专属查询：
  info <pid>            — 进程详细信息（名称、命令、通信方式列表）
  neighbor <pid>        — 进程通信伙伴列表
  common <a> <b>        — 两个进程的共同通信伙伴
  triangle [pid]        — 三角通信关系（无pid则全图）
  cluster <pid>         — 进程的通信子网（2跳内连通子图）
  stats                 — 系统通信图统计概览
  edges                 — 列出所有通信边（含类型）
  shutdown              — 关闭系统
"""
import argparse, os, socket, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    make_msg, pack_msg, recv_msg,
    MSG_QUERY_NEIGHBOR, MSG_QUERY_COMMON, MSG_QUERY_TRIANGLE,
    MSG_QUERY_NODE_INFO, MSG_RESULT_OK, MSG_RESULT_ERR, MSG_SHUTDOWN, logger
)

def rpc(chost, cport, msg):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(120)
        sock.connect((chost, cport))
        sock.sendall(pack_msg(msg))
        resp = recv_msg(sock)
        sock.close()
        if resp is None: return {"error": "无响应"}
        if resp["msg_type"] == MSG_RESULT_ERR:
            return {"error": resp.get("payload", {}).get("error", "未知错误")}
        return resp.get("payload", {})
    except Exception as e:
        return {"error": str(e)}

# ── 格式化输出 ──

def fmt_process_attrs(attrs):
    """格式化进程属性"""
    info = []
    if "name" in attrs:
        info.append(f"进程名: {attrs['name']}")
    if "cmdline" in attrs and attrs["cmdline"]:
        info.append(f"命令行: {attrs['cmdline']}")
    if "state" in attrs:
        state_map = {"R": "运行", "S": "睡眠", "D": "不可中断", "Z": "僵尸", "T": "停止", "t": "跟踪"}
        info.append(f"状态: {state_map.get(attrs['state'], attrs['state'])}")
    if "uid" in attrs:
        info.append(f"UID: {attrs['uid']}")
    if "ppid" in attrs:
        info.append(f"父进程: {attrs['ppid']}")
    return " | ".join(info)

def fmt_edge_type(t):
    type_map = {
        "parent_child": "父子关系",
        "pipe": "管道通信",
        "unix_socket": "UNIX Socket",
        "net_socket": "网络Socket",
        "signal": "信号",
    }
    return type_map.get(t, t)

# ── 命令处理 ──

def cmd_info(chost, cport, pid):
    p = rpc(chost, cport, make_msg(MSG_QUERY_NODE_INFO, "client", {"node": pid}))
    if "error" in p:
        print(f"错误: {p['error']}"); return

    print(f"{'='*60}")
    print(f"  进程 {pid} 详细信息")
    print(f"{'='*60}")

    attrs = p.get("attrs", {})
    if attrs:
        print(f"\n  {fmt_process_attrs(attrs)}")
    print(f"\n  通信度: {p.get('degree', 0)}")

    conns = p.get("connections", [])
    if conns:
        print(f"\n  通信关系 ({len(conns)} 个):")
        # 按类型分组
        by_type = {}
        for c in conns:
            t = c.get("type", "unknown")
            by_type.setdefault(t, []).append(c["neighbor"])
        for t, nbrs in sorted(by_type.items(), key=lambda x: -len(x[1])):
            print(f"    {fmt_edge_type(t)} ({len(nbrs)}): {nbrs[:10]}{'...' if len(nbrs) > 10 else ''}")
    else:
        print("\n  无通信关系")

def cmd_neighbor(chost, cport, pid):
    p = rpc(chost, cport, make_msg(MSG_QUERY_NEIGHBOR, "client", {"node_id": pid}))
    if "error" in p: print(f"错误: {p['error']}"); return

    nbrs = p.get("connections", [])
    degree = p.get("degree", len(nbrs))
    attrs = p.get("attrs", {})
    pname = attrs.get("name", "?")
    print(f"进程 {pid} ({pname}) 的通信伙伴:")
    print(f"  度数: {degree}")
    if nbrs:
        # 按类型分组去重显示
        seen = set()
        unique_nbrs = []
        for c in nbrs:
            nb = c.get("neighbor")
            if nb not in seen:
                seen.add(nb)
                unique_nbrs.append(c)
        print(f"  共 {len(unique_nbrs)} 个:")
        by_type = {}
        for c in unique_nbrs:
            t = c.get("type", "unknown")
            by_type.setdefault(t, []).append(c["neighbor"])
        for t, nbr_list in sorted(by_type.items(), key=lambda x: -len(x[1])):
            type_label = fmt_edge_type(t)
            print(f"    [{type_label}]({len(nbr_list)}): {nbr_list[:12]}{'...' if len(nbr_list) > 12 else ''}")
    else:
        print("  (无通信伙伴)")

def cmd_common(chost, cport, a, b):
    p = rpc(chost, cport, make_msg(MSG_QUERY_COMMON, "client", {"node_a": a, "node_b": b}))
    if "error" in p: print(f"错误: {p['error']}"); return

    common = p.get("common_neighbors", [])
    print(f"进程 {a} 和 {b} 的共同通信伙伴: {len(common)} 个")
    if common:
        print(f"  {common}")

def cmd_triangle(chost, cport, pid=None):
    if pid is not None:
        p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "client", {"node_id": pid}))
        if "error" in p: print(f"错误: {p['error']}"); return
        tris = p.get("triangles", [])
        print(f"进程 {pid} 参与的三角通信关系 ({p.get('count', len(tris))} 个):")
        for t in tris:
            print(f"  {t[0]} ↔ {t[1]} ↔ {t[2]}  (三者两两通信)")
    else:
        p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "client", {}))
        if "error" in p: print(f"错误: {p['error']}"); return
        tris = p.get("triangles", [])
        print(f"全系统三角通信关系总数: {p.get('count', len(tris))} 个")
        if tris and len(tris) <= 20:
            for t in tris:
                print(f"  {t[0]} ↔ {t[1]} ↔ {t[2]}")
        elif tris:
            print(f"  (前 10 个):")
            for t in tris[:10]:
                print(f"  {t[0]} ↔ {t[1]} ↔ {t[2]}")
            print(f"  ... 还有 {len(tris)-10} 个")

def cmd_cluster(chost, cport, pid, depth=2):
    """2 跳内通信子网"""
    visited = {pid: 0}
    frontier = {pid}

    for d in range(depth):
        next_frontier = set()
        for node in frontier:
            p = rpc(chost, cport, make_msg(MSG_QUERY_NEIGHBOR, "client", {"node_id": node}))
            if "error" in p: continue
            for c in p.get("connections", []):
                nb = c.get("neighbor")
                if nb not in visited:
                    visited[nb] = d + 1
                    next_frontier.add(nb)
        frontier = next_frontier
        if not frontier:
            break

    print(f"进程 {pid} 的 {depth} 跳通信子网:")
    print(f"  包含 {len(visited)} 个进程")
    by_depth = {}
    for node, dist in visited.items():
        by_depth.setdefault(dist, []).append(node)
    for d in range(depth + 1):
        nodes = sorted(by_depth.get(d, []))
        if nodes:
            label = "自身" if d == 0 else f"{d}跳邻居"
            print(f"  [{label}] ({len(nodes)}): {', '.join(str(n) for n in nodes[:15])}{'...' if len(nodes) > 15 else ''}")

def cmd_stats(chost, cport):
    p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "client", {}))
    if "error" in p: print(f"错误: {p['error']}"); return
    print(f"{'='*60}")
    print(f"  系统进程通信图 - 统计概览")
    print(f"{'='*60}")
    print(f"\n  三角通信关系: {p.get('count', 0)} 组")

def cmd_edges(chost, cport):
    p = rpc(chost, cport, make_msg("query_all_edges", "client", {}))
    if "error" in p: print(f"错误: {p['error']}"); return
    edges = p.get("edges", [])
    print(f"系统通信边列表 ({len(edges)} 条):")
    for e in edges:
        if isinstance(e, dict):
            t = e.get("type", "unknown")
            print(f"  {e['src']} ↔ {e['dst']}  [{t}]")
        elif isinstance(e, (list, tuple)) and len(e) >= 2:
            print(f"  {e[0]} ↔ {e[1]}")
        else:
            print(f"  {e}")

def cmd_shutdown(chost, cport):
    p = rpc(chost, cport, make_msg(MSG_SHUTDOWN, "client", {}))
    print("Coordinator 已关闭")

def main():
    parser = argparse.ArgumentParser(description="进程通信图查询客户端")
    parser.add_argument("--coord-host", default="127.0.0.1")
    parser.add_argument("--coord-port", type=int, default=9900)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("info", help="进程详细信息"); p.add_argument("pid", type=int)
    p = sub.add_parser("neighbor", aliases=["n"], help="进程通信伙伴"); p.add_argument("pid", type=int)
    p = sub.add_parser("common", aliases=["c"], help="共同通信伙伴"); p.add_argument("a", type=int); p.add_argument("b", type=int)
    p = sub.add_parser("triangle", aliases=["t"], help="三角通信关系"); p.add_argument("pid", nargs="?", type=int, default=None)
    p = sub.add_parser("cluster", aliases=["cl"], help="通信子网"); p.add_argument("pid", type=int); p.add_argument("--depth", type=int, default=2)
    p = sub.add_parser("stats", help="系统统计概览")
    p = sub.add_parser("edges", help="所有通信边")
    p = sub.add_parser("shutdown", aliases=["exit"], help="关闭系统")

    args = parser.parse_args()
    t0 = time.time()

    cmds = {
        "info": lambda: cmd_info(args.coord_host, args.coord_port, args.pid),
        "neighbor": lambda: cmd_neighbor(args.coord_host, args.coord_port, args.pid),
        "n": lambda: cmd_neighbor(args.coord_host, args.coord_port, args.pid),
        "common": lambda: cmd_common(args.coord_host, args.coord_port, args.a, args.b),
        "c": lambda: cmd_common(args.coord_host, args.coord_port, args.a, args.b),
        "triangle": lambda: cmd_triangle(args.coord_host, args.coord_port, args.pid),
        "t": lambda: cmd_triangle(args.coord_host, args.coord_port, args.pid),
        "cluster": lambda: cmd_cluster(args.coord_host, args.coord_port, args.pid, getattr(args, "depth", 2)),
        "cl": lambda: cmd_cluster(args.coord_host, args.coord_port, args.pid, getattr(args, "depth", 2)),
        "stats": lambda: cmd_stats(args.coord_host, args.coord_port),
        "edges": lambda: cmd_edges(args.coord_host, args.coord_port),
        "shutdown": lambda: cmd_shutdown(args.coord_host, args.coord_port),
        "exit": lambda: cmd_shutdown(args.coord_host, args.coord_port),
    }

    fn = cmds.get(args.cmd)
    if fn:
        fn()
        print(f"\n耗时: {time.time()-t0:.3f}s")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()