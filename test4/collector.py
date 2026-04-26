#!/usr/bin/env python3
"""
进程通信图数据采集器 — 从 /proc 文件系统采集进程通信关系

采集类型：
1. parent_child — PPID 关系（子进程创建关系）
2. pipe — 管道通信（通过共享 pipe inode 识别）
3. unix_socket — UNIX domain socket 连接
4. net_socket — TCP/UDP socket（IP:port 级别）
5. signal — 实时 signal 发送关系（通过 /proc/PID/task 和 fd 分析）
"""
import json, logging, os, re, sys, time
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    GraphStorage, get_partition, logger
)

# ── 辅助：安全读取 /proc 文件 ──
def _read_proc(path, default=""):
    try:
        with open(path, "r", errors="replace") as f:
            return f.read().strip()
    except (FileNotFoundError, PermissionError, ProcessLookupError, OSError):
        return default

def _readlink(path, default=""):
    try:
        return os.readlink(path)
    except (FileNotFoundError, PermissionError, OSError):
        return default

# ── 1. 扫描所有进程 ──
def scan_processes():
    """返回所有进程 PID 列表"""
    pids = []
    for entry in os.listdir("/proc"):
        if entry.isdigit():
            try:
                os.kill(int(entry), 0)  # 检测进程是否存活
                pids.append(int(entry))
            except (ProcessLookupError, PermissionError):
                pass
    return sorted(pids)

# ── 2. 获取进程基本信息 ──
def get_process_info(pid):
    """返回进程属性字典"""
    status = _read_proc(f"/proc/{pid}/status")
    info = {}

    # 基础属性
    for line in status.split("\n"):
        if line.startswith("Name:"):
            info["name"] = line.split(":", 1)[1].strip()
        elif line.startswith("Pid:"):
            info["pid"] = int(line.split(":", 1)[1].strip())
        elif line.startswith("PPid:"):
            info["ppid"] = int(line.split(":", 1)[1].strip())
        elif line.startswith("Uid:"):
            parts = line.split()
            info["uid"] = int(parts[1]) if len(parts) > 1 else 0
        elif line.startswith("State:"):
            s = line.split(":", 1)[1].strip()
            info["state"] = s.split()[0] if s else "?"

    # cmdline
    cmdline = _read_proc(f"/proc/{pid}/cmdline", "")
    info["cmdline"] = cmdline.replace("\0", " ").strip() if cmdline else f"[{info.get('name', '?')}]"

    return info

# ── 3. 扫描文件描述符 ──
def scan_fds(pid):
    """返回该进程的 fd 列表，每条含 fd_num, target (readlink结果), inode"""
    fds = []
    fd_dir = f"/proc/{pid}/fd"
    try:
        for entry in os.listdir(fd_dir):
            target = _readlink(f"{fd_dir}/{entry}")
            # 提取 inode
            inode_match = re.search(r"\[?(\d+)\]?$", target) if target else None
            inode = int(inode_match.group(1)) if inode_match else 0

            # 识别类型
            ftype = "file"
            if "pipe:" in target:
                ftype = "pipe"
            elif target.startswith("socket:"):
                ftype = "socket"
            elif target.startswith("[socket:"):
                ftype = "socket"

            fds.append({
                "fd": int(entry),
                "target": target,
                "type": ftype,
                "inode": inode,
            })
    except (PermissionError, FileNotFoundError, ProcessLookupError):
        pass
    return fds

# ── 4. 扫描进程网络 socket ──
def scan_net_sockets():
    """
    扫描 /proc/PID/net/tcp 等获取每个进程的 TCP/UDP 连接信息
    返回 { (ip, port): [pid1, pid2, ...] }
    """
    import glob

    # 正则：匹配 TCP 连接条目，提取 local_address:port 和 rem_address:port
    # 格式： sl  local_address rem_address st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
    tcp_pattern = re.compile(
        r"\s*\d+:\s+([0-9A-Fa-f]+):([0-9A-Fa-f]+)\s+"
        r"([0-9A-Fa-f]+):([0-9A-Fa-f]+)\s+"
        r"([0-9A-Fa-f]+)"  # state
    )

    # 解析 hex IP (小端32位或128位)
    def _hex_to_ip(hex_str):
        try:
            v = int(hex_str, 16)
            return f"{(v>>24)&0xFF}.{(v>>16)&0xFF}.{(v>>8)&0xFF}.{v&0xFF}"
        except:
            return hex_str

    # global_inode_to_addr: inode -> (ip, port, peer_ip, peer_port)
    inode_to_addr = {}

    for proc_dir in glob.glob("/proc/[0-9]*/net/tcp"):
        pid = int(proc_dir.split("/")[2])
        data = _read_proc(proc_dir, "")
        for line in data.split("\n")[1:]:  # 跳过标题
            if not line.strip():
                continue
            m = tcp_pattern.match(line)
            if not m:
                continue
            local_ip = _hex_to_ip(m.group(1))
            local_port = int(m.group(2), 16)
            peer_ip = _hex_to_ip(m.group(3))
            peer_port = int(m.group(4), 16)
            state = int(m.group(5), 16)

            # skmem行或inode — 用另一种方式
            # 实际上 inode 在最后几列
            parts = line.split()
            if len(parts) >= 10:
                inode_str = parts[9]
                try:
                    inode_val = int(inode_str)
                except:
                    continue
            else:
                continue

            inode_to_addr[inode_val] = {
                "pid": pid, "local_ip": local_ip, "local_port": local_port,
                "peer_ip": peer_ip, "peer_port": peer_port, "state": state,
            }

    # 按 peer (ip:port) 建立连接关系
    # 如果两个进程连接到同一对 (ip, port) → 它们通过共享目标通信
    # 更好的方法：查找 LISTEN 端口 + 已连接端口的关系
    listen_ports = defaultdict(set)     # (local_ip, local_port) → set of pids （LISTEN状态）
    est_connections = defaultdict(list) # peer(ip,port) → list of {pid, local_ip, local_port}

    for inode, info in inode_to_addr.items():
        pid = info["pid"]
        if info["state"] == 0x0A:  # LISTEN
            listen_ports[(info["local_ip"], info["local_port"])].add(pid)
        elif info["state"] == 0x01:  # ESTABLISHED
            peer_key = (info["peer_ip"], info["peer_port"])
            est_connections[peer_key].append({
                "pid": pid,
                "local_ip": info["local_ip"],
                "local_port": info["local_port"],
            })

    # 构建边：相同 peer (ip,port) 的一对进程间有网络连接
    net_edges = defaultdict(set)  # (pid1, pid2) -> set of connection keys
    for peer_key, conns in est_connections.items():
        for i in range(len(conns)):
            for j in range(i + 1, len(conns)):
                a, b = conns[i]["pid"], conns[j]["pid"]
                net_edges[(a, b)].add(f"TCP {peer_key[0]}:{peer_key[1]}")

    return listen_ports, net_edges

# ── 5. 扫描 UNIX socket ──
def scan_unix_sockets():
    """扫描 /proc/PID/fd/ 识别 UNIX domain socket 连接"""
    # UNIX socket 由 inode 关联
    # socket:[INODE] 的 fd 如果有两个进程引用同一个 inode 的 socket fd → 它们通过 UNIX socket 通信
    import glob

    # inode → set of pids
    unix_inode_pids = defaultdict(set)

    for proc_dir in glob.glob("/proc/[0-9]*"):
        pid = int(proc_dir.split("/")[2])
        fds = scan_fds(pid)
        for fd_info in fds:
            if fd_info["type"] == "socket" and fd_info["inode"] > 0:
                unix_inode_pids[fd_info["inode"]].add(pid)

    # 共享同一个 socket inode 的进程对 → 它们通过 UNIX socket 通信
    unix_edges = set()
    for inode, pids in unix_inode_pids.items():
        pid_list = list(pids)
        if len(pid_list) >= 2:
            for i in range(len(pid_list)):
                for j in range(i + 1, len(pid_list)):
                    a, b = min(pid_list[i], pid_list[j]), max(pid_list[i], pid_list[j])
                    unix_edges.add((a, b))

    return unix_edges

# ── 6. 扫描管道 ──
def scan_pipes():
    """扫描所有进程管道 fd 的 inode，共享同 inode 的 pipe 连接两个进程"""
    import glob

    # inode → set of pids
    pipe_inode_pids = defaultdict(set)

    for proc_dir in glob.glob("/proc/[0-9]*"):
        pid = int(proc_dir.split("/")[2])
        fds = scan_fds(pid)
        for fd_info in fds:
            if fd_info["type"] == "pipe" and fd_info["inode"] > 0:
                pipe_inode_pids[fd_info["inode"]].add(pid)

    # 共享同一个 pipe inode 的进程对
    pipe_edges = set()
    for inode, pids in pipe_inode_pids.items():
        pid_list = list(pids)
        if len(pid_list) >= 2:
            for i in range(len(pid_list)):
                for j in range(i + 1, len(pid_list)):
                    a, b = min(pid_list[i], pid_list[j]), max(pid_list[i], pid_list[j])
                    pipe_edges.add((a, b))

    return pipe_edges

# ── 主采集函数 ──
def collect_proc_graph(filter_pids=None, max_pids=200):
    """
    从 /proc 采集进程通信图
    返回 GraphStorage 实例
    """
    g = GraphStorage(directed=False)

    all_pids = scan_processes()
    if filter_pids:
        all_pids = [p for p in all_pids if p in set(filter_pids)]
    if max_pids and len(all_pids) > max_pids:
        # 尽量保留关键进程（低PID）
        all_pids = sorted(all_pids)[:max_pids]

    logger.info(f"扫描 {len(all_pids)} 个进程...")

    # 1. 添加所有进程节点
    pid_set = set(all_pids)
    for pid in all_pids:
        info = get_process_info(pid)
        if "name" not in info:
            info["name"] = f"proc_{pid}"
        g.add_node(pid, **info)

    logger.info(f"  添加 {len(all_pids)} 个进程节点")

    # 2. 添加 parent_child 边
    parent_edges = 0
    for pid in all_pids:
        ppid = g.node_attrs[pid].get("ppid", -1)
        if ppid > 0 and ppid in pid_set:
            g.add_edge(pid, ppid, type="parent_child")
            parent_edges += 1
    logger.info(f"  父/子关系边: {parent_edges}")

    # 3. 扫描管道
    pipe_conns = scan_pipes()
    pipe_edges = 0
    for a, b in pipe_conns:
        if a in pid_set and b in pid_set:
            g.add_edge(a, b, type="pipe")
            pipe_edges += 1
    logger.info(f"  管道通信边: {pipe_edges}")

    # 4. 扫描 UNIX socket
    unix_conns = scan_unix_sockets()
    unix_edges = 0
    for a, b in unix_conns:
        if a in pid_set and b in pid_set:
            g.add_edge(a, b, type="unix_socket")
            unix_edges += 1
    logger.info(f"  UNIX socket 边: {unix_edges}")

    # 5. 扫描网络 socket
    listen_ports, net_edges = scan_net_sockets()
    net_conns = 0
    for (a, b), descs in net_edges.items():
        if a in pid_set and b in pid_set:
            for desc in descs:
                g.add_edge(a, b, type="net_socket", conn_info=desc)
                net_conns += 1
    logger.info(f"  网络 socket 边: {net_conns}")

    total_nodes = g.num_nodes()
    total_edges = g.num_edges()
    logger.info(f"通信图构建完成: {total_nodes} 节点, {total_edges} 条边")

    return g

# ── 分区分发 ──
def distribute_to_partitions(g, num_parts=5, out_dir="workers"):
    """按一致性哈希分区，将图数据分发到各 Worker 数据文件"""
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.join(out_dir, "..", "workers"), exist_ok=True)

    from collections import defaultdict

    part_adj = [defaultdict(list) for _ in range(num_parts)]
    part_nodes = [set() for _ in range(num_parts)]
    part_edge_attrs = [{} for _ in range(num_parts)]

    for u, nbrs in g.adjacency.items():
        for v in nbrs:
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
            # 边属性（只存一次）
            key = tuple(sorted((u, v)))
            attrs = g.edge_attrs.get(key, {type: "unknown"})
            part_edge_attrs[pu][key] = attrs
            part_edge_attrs[pv][key] = attrs

    for i in range(num_parts):
        # 构建每个分区的 node_attrs
        node_attrs = {}
        for n in sorted(part_nodes[i]):
            node_attrs[str(n)] = dict(g.node_attrs.get(n, {}))

        # 构建每个分区的 edge_attrs
        edge_attrs = {}
        for (a, b), attrs in part_edge_attrs[i].items():
            edge_attrs[f"{a},{b}"] = attrs

        data = {
            "adjacency": {str(k): sorted(v) for k, v in sorted(part_adj[i].items())},
            "node_attrs": node_attrs,
            "edge_attrs": edge_attrs,
            "directed": False,
        }
        out = os.path.join(out_dir, f"part_{i}.json")
        with open(out, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        total_edges = sum(len(v) for v in part_adj[i].values())
        logger.info(f"  part_{i}: {len(part_nodes[i])} 节点, {total_edges} 条邻接条目 → {os.path.abspath(out)}")

    # 验证
    full_adj = defaultdict(set)
    for i in range(num_parts):
        for k, vals in part_adj[i].items():
            for v in vals:
                full_adj[k].add(v)
                full_adj[v].add(k)

    triangles = set()
    for u in full_adj:
        for v in full_adj[u]:
            if v <= u: continue
            for w in full_adj[u] & full_adj[v]:
                if w > v:
                    triangles.add(tuple(sorted((u,v,w))))
    logger.info(f"三角验证: {len(triangles)} 个三角形 (仅包含在数据中的节点)")

    return len(triangles)

# ── CLI ──
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="进程通信图数据采集器")
    ap.add_argument("--max-pids", type=int, default=200, help="最大采集进程数")
    ap.add_argument("--num-parts", type=int, default=5, help="分区数")
    ap.add_argument("--out-dir", default="workers", help="输出目录")
    args = ap.parse_args()

    logging.getLogger().setLevel(logging.INFO)

    print("=" * 60)
    print("  进程通信图采集器")
    print("=" * 60)
    t0 = time.time()

    g = collect_proc_graph(max_pids=args.max_pids)

    print(f"\n采集耗时: {time.time()-t0:.2f}s")
    print(f"总节点: {g.num_nodes()}")
    print(f"总边数: {g.num_edges()}")

    # 统计通信类型
    type_counts = defaultdict(int)
    for attrs in g.edge_attrs.values():
        type_counts[attrs.get("type", "unknown")] += 1
    print(f"\n通信类型分布:")
    for tname, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {tname}: {cnt}")

    if args.num_parts > 1:
        print(f"\n分发到 {args.num_parts} 个分区...")
        triangles = distribute_to_partitions(g, args.num_parts, args.out_dir)
        print(f"三角验证: {triangles} 个三角形")
    print(f"\n总耗时: {time.time()-t0:.2f}s")
