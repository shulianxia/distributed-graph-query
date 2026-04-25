#!/usr/bin/env python3
"""
分布式图查询系统 — 命令行客户端
"""
import argparse, os, socket, sys, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import make_msg, pack_msg, recv_msg, MSG_QUERY_NEIGHBOR, MSG_QUERY_COMMON, MSG_QUERY_TRIANGLE, MSG_RESULT_OK, MSG_RESULT_ERR, MSG_SHUTDOWN

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

def cmd_neighbor(chost, cport, nid):
    p = rpc(chost, cport, make_msg(MSG_QUERY_NEIGHBOR, "client", {"node_id": nid}))
    if "error" in p: print(f"错误: {p['error']}"); return
    print(f"节点 {p.get('node', nid)}:")
    print(f"  度数: {p.get('degree', 0)}")
    nbrs = p.get("neighbors", [])
    print(f"  邻居 ({len(nbrs)} 个): {nbrs}")

def cmd_common(chost, cport, a, b):
    p = rpc(chost, cport, make_msg(MSG_QUERY_COMMON, "client", {"node_a": a, "node_b": b}))
    if "error" in p: print(f"错误: {p['error']}"); return
    common = p.get("common_neighbors", [])
    print(f"节点 {a} 和 {b} 的共同邻居 ({len(common)} 个): {common}")

def cmd_triangle(chost, cport, nid=None):
    if nid is not None:
        p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "client", {"node_id": nid}))
        if "error" in p: print(f"错误: {p['error']}"); return
        tris = p.get("triangles", [])
        print(f"节点 {nid} 参与的三角形 ({p.get('count', len(tris))} 个):")
        for t in tris: print(f"  {t}")
    else:
        p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "client", {}))
        if "error" in p: print(f"错误: {p['error']}"); return
        tris = p.get("triangles", [])
        print(f"全图三角形计数: {p.get('total', len(tris))} 个")
        for t in tris: print(f"  {t}")

def cmd_shutdown(chost, cport):
    p = rpc(chost, cport, make_msg(MSG_SHUTDOWN, "client", {}))
    print("Coordinator 已关闭")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coord-host", default="127.0.0.1")
    parser.add_argument("--coord-port", type=int, default=9000)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("neighbor", aliases=["n"]); p1.add_argument("node_id", type=int)
    p2 = sub.add_parser("common", aliases=["c"]); p2.add_argument("node_a", type=int); p2.add_argument("node_b", type=int)
    p3 = sub.add_parser("triangle", aliases=["t"]); p3.add_argument("node_id", nargs="?", type=int, default=None)
    p4 = sub.add_parser("shutdown", aliases=["exit"])

    args = parser.parse_args()
    t0 = time.time()

    if args.cmd in ("neighbor", "n"): cmd_neighbor(args.coord_host, args.coord_port, args.node_id)
    elif args.cmd in ("common", "c"): cmd_common(args.coord_host, args.coord_port, args.node_a, args.node_b)
    elif args.cmd in ("triangle", "t"): cmd_triangle(args.coord_host, args.coord_port, args.node_id)
    elif args.cmd in ("shutdown", "exit"): cmd_shutdown(args.coord_host, args.coord_port)

    print(f"\n耗时: {time.time()-t0:.3f}s")

if __name__ == "__main__":
    main()
