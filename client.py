#!/usr/bin/env python3
"""
分布式图查询系统 — 命令行客户端

连接 Coordinator 发送查询请求，显示结果。

用法：
  python3 client.py --coord-host 127.0.0.1 --coord-port 9000 neighbor 42
  python3 client.py --coord-host 127.0.0.1 --coord-port 9000 common 10 20
  python3 client.py --coord-host 127.0.0.1 --coord-port 9000 triangle 42
  python3 client.py --coord-host 127.0.0.1 --coord-port 9000 triangles
"""

import argparse
import socket
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    make_msg,
    pack_msg,
    recv_msg,
    MSG_QUERY_NEIGHBOR,
    MSG_QUERY_COMMON,
    MSG_QUERY_TRIANGLE,
    MSG_QUERY_EDGES,
    MSG_RESULT_OK,
    MSG_RESULT_ERR,
    MSG_SHUTDOWN,
)


def rpc_coord(coord_host, coord_port, msg):
    """向 Coordinator 发送 RPC 请求，返回解析后的 payload"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(60)
        sock.connect((coord_host, coord_port))
        sock.sendall(pack_msg(msg))
        resp = recv_msg(sock)
        sock.close()

        if resp is None:
            print("错误: Coordinator 无响应")
            return None

        if resp["msg_type"] == MSG_RESULT_ERR:
            print(f"错误: {resp.get('payload', {}).get('error', '未知错误')}")
            return None

        return resp.get("payload")
    except Exception as e:
        print(f"连接失败: {e}")
        return None


def cmd_neighbor(coord_host, coord_port, node_id):
    payload = rpc_coord(coord_host, coord_port,
                        make_msg(MSG_QUERY_NEIGHBOR, "client", {"node_id": node_id}))
    if payload is None:
        return
    node = payload.get("node", node_id)
    neighbors = payload.get("neighbors", [])
    degree = payload.get("degree", len(neighbors))
    attrs = payload.get("attrs", {})
    print(f"节点 {node}:")
    print(f"  属性: {attrs}")
    print(f"  度数: {degree}")
    print(f"  邻居 ({len(neighbors)} 个): {neighbors}")


def cmd_common(coord_host, coord_port, a, b):
    payload = rpc_coord(coord_host, coord_port,
                        make_msg(MSG_QUERY_COMMON, "client", {"node_a": a, "node_b": b}))
    if payload is None:
        return
    common = payload.get("common_neighbors", [])
    print(f"节点 {a} 和 {b} 的共同邻居 ({len(common)} 个): {common}")


def cmd_triangle(coord_host, coord_port, node_id=None):
    if node_id is not None:
        payload = rpc_coord(
            coord_host, coord_port,
            make_msg(MSG_QUERY_TRIANGLE, "client", {"node_id": node_id}),
        )
        if payload is None:
            return
        tris = payload.get("triangles", [])
        print(f"节点 {node_id} 参与的三角形 ({payload.get('count', len(tris))} 个):")
        for tri in tris:
            print(f"  {tri}")
    else:
        payload = rpc_coord(
            coord_host, coord_port,
            make_msg(MSG_QUERY_TRIANGLE, "client", {}),
        )
        if payload is None:
            return
        tris = payload.get("triangles", [])
        print(f"全图三角形计数: {payload.get('total', len(tris))} 个")
        if tris:
            for tri in tris:
                print(f"  {tri}")


def cmd_shutdown(coord_host, coord_port):
    payload = rpc_coord(coord_host, coord_port,
                        make_msg(MSG_SHUTDOWN, "client", {}))
    print("Coordinator 已关闭")


def main():
    parser = argparse.ArgumentParser(description="分布式图查询客户端")
    parser.add_argument("--coord-host", default="127.0.0.1", help="Coordinator 地址")
    parser.add_argument("--coord-port", type=int, default=9000, help="Coordinator 端口")
    sub = parser.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("neighbor", aliases=["n"], help="查询邻居")
    p1.add_argument("node_id", type=int, help="节点 ID")

    p2 = sub.add_parser("common", aliases=["c"], help="查询共同邻居")
    p2.add_argument("node_a", type=int, help="节点 A")
    p2.add_argument("node_b", type=int, help="节点 B")

    p3 = sub.add_parser("triangle", aliases=["t"], help="查询三角形(可带节点ID)")
    p3.add_argument("node_id", nargs="?", type=int, default=None, help="节点 ID (可选)")

    p4 = sub.add_parser("shutdown", aliases=["exit"], help="关闭 Coordinator")

    args = parser.parse_args()

    start = time.time()

    if args.command in ("neighbor", "n"):
        cmd_neighbor(args.coord_host, args.coord_port, args.node_id)
    elif args.command in ("common", "c"):
        cmd_common(args.coord_host, args.coord_port, args.node_a, args.node_b)
    elif args.command in ("triangle", "t"):
        cmd_triangle(args.coord_host, args.coord_port, args.node_id)
    elif args.command in ("shutdown", "exit"):
        cmd_shutdown(args.coord_host, args.coord_port)

    elapsed = time.time() - start
    print(f"\n耗时: {elapsed:.3f}s")


if __name__ == "__main__":
    main()
