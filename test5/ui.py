#!/usr/bin/env python3
"""
test5 — 交互式图查询 TUI 界面

零外部依赖（仅 Python 标准库）。
彩色终端交互，支持 Tab 自动补全、命令历史、格式化输出。

用法：
  python3 ui.py --coord-host 127.0.0.1 --coord-port 9000

命令列表：
  help                             显示帮助
  info <node_id>                   查询节点详细信息（含属性）
  n <node_id> / neighbor <node>    查询邻居列表
  c <a> <b> / common <a> <b>      共同邻居
  t [node_id] / triangle [node]    三角计数
  stats / st                       全图统计
  edges [limit]                    查看边列表
  seek <keyword>                   搜索节点（按标签/ID）
  clear / cls                      清屏
  history / h                      命令历史
  exit / quit                      退出
"""
import argparse, os, socket, sys, time, readline, atexit
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    make_msg, pack_msg, recv_msg,
    MSG_QUERY_NEIGHBOR, MSG_QUERY_NLIST, MSG_QUERY_COMMON, MSG_QUERY_TRIANGLE,
    MSG_QUERY_STATS, MSG_QUERY_NODE_INFO, MSG_RESULT_OK, MSG_RESULT_ERR, MSG_SHUTDOWN
)

# ── 终端颜色 ──
class Style:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"
    BG_BLUE = "\033[44m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_RED = "\033[41m"

    @staticmethod
    def fmt(c, text):
        return f"{c}{text}{Style.RESET}"

    @staticmethod
    def bold(t): return f"{Style.BOLD}{t}{Style.RESET}"
    @staticmethod
    def dim(t): return f"{Style.DIM}{t}{Style.RESET}"
    @staticmethod
    def green(t): return f"{Style.GREEN}{t}{Style.RESET}"
    @staticmethod
    def red(t): return f"{Style.RED}{t}{Style.RESET}"
    @staticmethod
    def blue(t): return f"{Style.BLUE}{t}{Style.RESET}"
    @staticmethod
    def cyan(t): return f"{Style.CYAN}{t}{Style.RESET}"
    @staticmethod
    def yellow(t): return f"{Style.YELLOW}{t}{Style.RESET}"
    @staticmethod
    def magenta(t): return f"{Style.MAGENTA}{t}{Style.RESET}"
    @staticmethod
    def gray(t): return f"{Style.GRAY}{t}{Style.RESET}"


# ── RPC ──
def rpc(chost, cport, msg_obj):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30)
        sock.connect((chost, cport))
        sock.sendall(pack_msg(msg_obj))
        resp = recv_msg(sock)
        sock.close()
        if resp is None: return {"error": "无响应"}
        if resp["msg_type"] == MSG_RESULT_ERR:
            return {"error": resp.get("payload", {}).get("error", "未知错误")}
        return resp.get("payload", {})
    except socket.timeout:
        return {"error": "连接超时"}
    except ConnectionRefusedError:
        return {"error": "连接被拒绝 — Coordinator 是否在运行？"}
    except Exception as e:
        return {"error": str(e)}


# ── 表格输出 ──
def print_table(headers, rows):
    """打印对齐表格"""
    if not rows:
        print(Style.dim("  (空)"))
        return
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    # 确保至少有 1 字符间距
    sep = "  "
    header_line = sep.join(Style.bold(h.ljust(col_widths[i])) for i, h in enumerate(headers))
    print(f"  {header_line}")
    print(f"  {Style.gray('─' * (sum(col_widths) + len(sep) * (len(headers) - 1)))}")
    for row in rows:
        line = sep.join(str(c).ljust(col_widths[i]) for i, c in enumerate(row))
        print(f"  {line}")


# ── 渲染函数 ──
def render_banner():
    """显示欢迎标题"""
    b = r"""
   ╔══════════════════════════════════╗
   ║   分布式图查询系统  v5           ║
   ║   Distributed Graph Query        ║
   ╚══════════════════════════════════╝"""
    print(Style.cyan(b))
    print()


def render_help():
    print(f"""
  {Style.bold('命令列表')}

  {Style.green('查询类')}
    {Style.bold('info')} <node_id>        节点详细信息（含属性和标签）
    {Style.bold('n')} <node_id>           邻居列表（也可用 neighbor）
    {Style.bold('c')} <a> <b>             共同邻居（也可用 common）
    {Style.bold('t')} [node_id]           三角计数（无参数 = 全图预取）
    {Style.bold('stats')}                  全图统计信息
    {Style.bold('edges')} [limit]         查看边列表

  {Style.green('辅助类')}
    {Style.bold('seek')} <keyword>        模糊搜索节点（按 ID 或标签）
    {Style.bold('clear')} / cls           清屏
    {Style.bold('history')} / h           查看命令历史
    {Style.bold('help')}                  显示此帮助
    {Style.bold('version')}               显示版本信息

  {Style.green('系统类')}
    {Style.bold('shutdown')}              关闭 Coordinator
    {Style.bold('exit')} / quit           退出客户端

  {Style.dim('提示: Tab 自动补全命令 · ↑↓ 浏览历史')}
""")


def render_version(coord_host, coord_port):
    p = rpc(coord_host, coord_port, make_msg(MSG_QUERY_STATS, "ui", {}))
    if "error" in p:
        print(f"  {Style.red('✗')} {p['error']}")
        return
    print(f"""
  {Style.bold('系统状态')}
  {Style.gray('Coordinator:')} {coord_host}:{coord_port}
  {Style.gray('Worker 节点:')} {p.get('num_workers', '?')}
  {Style.gray('数据分区:')} {p.get('num_parts', '?')}
  {Style.gray('总节点数:')} {p.get('num_nodes', '?')}
  {Style.gray('全图三角:')} {p.get('num_triangles', '?')}
  {Style.gray('协议:')} TCP/JSON (4字节长度前缀)
  {Style.dim('test5 — 交互式图查询 TUI')}
""")


def render_info(chost, cport, nid):
    p = rpc(chost, cport, make_msg(MSG_QUERY_NODE_INFO, "ui", {"node": nid}))
    if "error" in p:
        print(f"  {Style.red('✗')} {p['error']}")
        return
    attrs = p.get("attrs", {})
    degree = p.get("degree", 0)
    label = attrs.get("label", "未知")
    group = attrs.get("group", "?")

    print(f"""
  {Style.bold(f'节点 {nid}')}  {Style.gray(f'({label})')}
  {Style.gray('┌─ 属性')}
  {Style.gray('│')}  标签: {Style.green(label)}   组: {group}
  {Style.gray('│')}  度数: {Style.yellow(str(degree))}
  {Style.gray('└─ 邻居列表')}""")
    nbrs = p.get("neighbors", [])
    if nbrs:
        # 显示前 20 个
        display = nbrs[:20]
        for nn in display:
            print(f"     {Style.cyan(str(nn))}")
        if len(nbrs) > 20:
            print(f"     {Style.dim(f'... 还有 {len(nbrs) - 20} 个')}")
        print(f"     {Style.dim(f'共 {len(nbrs)} 个邻居')}")
    else:
        print(f"     {Style.dim('(无邻居)')}")


def render_neighbor(chost, cport, nid):
    p = rpc(chost, cport, make_msg(MSG_QUERY_NEIGHBOR, "ui", {"node": nid}))
    if "error" in p:
        print(f"  {Style.red('✗')} {p['error']}")
        return
    degree = p.get("degree", 0)
    nbrs = p.get("neighbors", [])
    print(f"  {Style.bold(f'节点 {nid}')}  →  度数: {Style.yellow(str(degree))}")
    if nbrs:
        # 紧凑显示：每行 10 个
        for i in range(0, len(nbrs), 10):
            chunk = nbrs[i:i+10]
            print(f"    {'  '.join(Style.cyan(str(nn)) for nn in chunk)}")
    else:
        print(f"    {Style.dim('(无邻居)')}")


def render_common(chost, cport, a, b):
    p = rpc(chost, cport, make_msg(MSG_QUERY_COMMON, "ui", {"node_a": a, "node_b": b}))
    if "error" in p:
        print(f"  {Style.red('✗')} {p['error']}")
        return
    common = p.get("common_neighbors", [])
    count = p.get("count", len(common))
    print(f"  {Style.bold(f'节点 {a} 和 {b} 的共同邻居')}  ({Style.yellow(str(count))} 个)")
    if common:
        for i in range(0, len(common), 10):
            chunk = common[i:i+10]
            print(f"    {'  '.join(Style.cyan(str(nn)) for nn in chunk)}")
    else:
        print(f"    {Style.dim('(无共同邻居)')}")


def render_triangle(chost, cport, nid=None):
    t0 = time.time()
    if nid is not None:
        p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "ui", {"node_id": nid}))
        if "error" in p:
            print(f"  {Style.red('✗')} {p['error']}")
            return
        tris = p.get("triangles", [])
        count = p.get("count", len(tris))
        elapsed = time.time() - t0
        print(f"  {Style.bold(f'节点 {nid} 参与的三角形')}  ({Style.yellow(str(count))} 个, {elapsed:.3f}s)")
        for t in tris[:30]:
            print(f"    {Style.cyan(str(t))}")
        if len(tris) > 30:
            print(f"    {Style.dim(f'... 还有 {len(tris) - 30} 个')}")
    else:
        print(f"  {Style.bold('全图三角计数（预取模式）')}")
        p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "ui", {}))
        if "error" in p:
            print(f"  {Style.red('✗')} {p['error']}")
            return
        tris = p.get("triangles", [])
        count = p.get("count", len(tris))
        elapsed = time.time() - t0
        print(f"  {Style.green('✓')} 总计: {Style.yellow(str(count))} 个三角形  ({elapsed:.3f}s)")
        if tris:
            print(f"\n  {Style.dim('前 20 个:')}")
            for t in tris[:20]:
                u, v, w = t
                print(f"    ({u}, {v}, {w})")


def render_stats(chost, cport):
    p = rpc(chost, cport, make_msg(MSG_QUERY_STATS, "ui", {}))
    if "error" in p:
        print(f"  {Style.red('✗')} {p['error']}")
        return
    # 争取也获取全图三角
    t0 = time.time()
    tp = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "ui", {}))
    elapsed = time.time() - t0
    triangles = tp.get("count", "?") if "error" not in tp else "?"

    print(f"""
  {Style.bold('全图统计')}
  {Style.gray('┌─ 概览')}
  {Style.gray('│')}  分区数:     {Style.cyan(str(p.get('num_parts', '?')))}
  {Style.gray('│')}  Worker 数:  {Style.cyan(str(p.get('num_workers', '?')))}
  {Style.gray('│')}  总节点数:   {Style.cyan(str(p.get('num_nodes', '?')))}
  {Style.gray('│')}  三角形数:   {Style.yellow(str(triangles))} ({elapsed:.3f}s 预取)
  {Style.gray('└─')}
""")


def render_edges(chost, cport, limit=20):
    p = rpc(chost, cport, make_msg(MSG_QUERY_TRIANGLE, "ui", {"node_id": -1}))
    # 用全局三角的预取代价遍历各 Worker 获取边
    # 实际上通过三角的 edges 获取足够了
    # 直接通过三角计数回调获取更合适
    # 这里用 info 遍历前 limit 个节点来实现
    # 更好的：用 stats 获取总节点数
    sp = rpc(chost, cport, make_msg(MSG_QUERY_STATS, "ui", {}))
    total = sp.get("num_nodes", 0)
    edges_set = set()
    # 采样前 limit 个节点查看其邻居
    sample = min(total, limit * 5)
    for n in range(sample):
        np = rpc(chost, cport, make_msg(MSG_QUERY_NEIGHBOR, "ui", {"node": n}))
        if "error" not in np:
            for nb in np.get("neighbors", []):
                edges_set.add(tuple(sorted((n, nb))))
            if len(edges_set) >= limit:
                break
    print(f"  {Style.bold('边列表')}  (显示前 {min(limit, len(edges_set))} 条)")
    if edges_set:
        print_table(
            ["源节点", "目标节点", "边"],
            [[Style.cyan(str(u)), Style.cyan(str(v)), Style.gray(f"({u} ↔ {v})")]
             for i, (u, v) in enumerate(sorted(edges_set)[:limit])]
        )
    else:
        print(f"    {Style.dim('(无边数据)')}")


def render_seek(chost, cport, keyword):
    """模糊搜索节点"""
    try:
        nid = int(keyword)
        # 按 ID 精确搜索
        p = rpc(chost, cport, make_msg(MSG_QUERY_NODE_INFO, "ui", {"node": nid}))
        if "error" not in p and p.get("neighbors") is not None:
            attrs = p.get("attrs", {})
            label = attrs.get("label", "?")
            print(f"  {Style.green('✓')} 节点 {Style.bold(str(nid))}  ({label}) 度={p.get('degree', 0)}")
            return
    except ValueError:
        pass

    # 按标签模糊搜索：不能直接搜索，获取 stats 中总节点数后遍历
    sp = rpc(chost, cport, make_msg(MSG_QUERY_STATS, "ui", {}))
    total = sp.get("num_nodes", 0)
    if total > 500:
        print(f"  {Style.yellow('⚠')} 节点数过多 ({total})，只在 0~500 范围内搜索")
        total = 500
    matches = []
    keyword_lower = keyword.lower()
    for n in range(total):
        p = rpc(chost, cport, make_msg(MSG_QUERY_NODE_INFO, "ui", {"node": n}))
        if "error" in p:
            continue
        attrs = p.get("attrs", {})
        label = str(attrs.get("label", ""))
        if keyword_lower in label.lower() or keyword_lower in str(n):
            matches.append((n, label, p.get("degree", 0)))
            if len(matches) >= 20:
                break

    if matches:
        print(f"  {Style.bold(f'搜索结果 ({len(matches)} 个):')}")
        print_table(
            ["节点 ID", "标签", "度数"],
            [[Style.cyan(str(m[0])), m[1], Style.yellow(str(m[2]))] for m in matches]
        )
    else:
        print(f"  {Style.dim('无匹配结果')}")


# ── Tab 补全 ──
class Completer:
    COMMANDS = [
        "help", "info", "neighbor", "n", "common", "c",
        "triangle", "t", "stats", "edges", "seek",
        "clear", "cls", "history", "h", "shutdown", "exit", "quit", "version",
    ]

    def complete(self, text, state):
        if state == 0:
            self.matches = [cmd for cmd in self.COMPLETER_COMMANDS if cmd.startswith(text)]
        try:
            return self.matches[state]
        except IndexError:
            return None


COMPLETER_COMMANDS = Completer.COMMANDS


def complete(text, state):
    # 简化版补全
    matches = [cmd for cmd in COMPLETER_COMMANDS if cmd.startswith(text)]
    try:
        return matches[state]
    except IndexError:
        return None


# ── 历史记录 ──
HISTFILE = os.path.expanduser("~/.dgraph_history")

def save_history():
    try:
        readline.write_history_file(HISTFILE)
    except:
        pass

def load_history():
    try:
        readline.read_history_file(HISTFILE)
    except:
        pass


# ── 主循环 ──
def main_loop(coord_host, coord_port):
    # CLI 环境设置
    readline.set_completer(complete)
    readline.parse_and_bind("tab: complete")
    load_history()
    atexit.register(save_history)

    history_list = []
    render_banner()
    render_help()

    prompt_color = Style.cyan
    prompt = f"{Style.bold('dgraph')} {prompt_color('>')} "

    while True:
        try:
            line = input(prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print()
            print(f"\n  {Style.gray('再见!')}")
            break

        if not line:
            continue

        history_list.append(line)

        parts = line.split()
        cmd = parts[0]
        args_list = parts[1:]

        t0 = time.time()

        try:
            if cmd in ("exit", "quit"):
                print(f"  {Style.gray('再见!')}")
                break

            elif cmd in ("help", "?"):
                render_help()

            elif cmd == "version":
                render_version(coord_host, coord_port)

            elif cmd in ("clear", "cls"):
                os.system("clear" if os.name == "posix" else "cls")
                render_banner()

            elif cmd in ("history", "h"):
                print(f"  {Style.bold('命令历史')}")
                for i, h in enumerate(history_list[-20:], 1):
                    print(f"  {Style.dim(str(i))}  {Style.cyan(h)}")

            elif cmd == "info":
                if not args_list:
                    print(f"  {Style.red('✗')} 用法: info <node_id>")
                    continue
                try:
                    nid = int(args_list[0])
                    render_info(coord_host, coord_port, nid)
                except ValueError:
                    print(f"  {Style.red('✗')} node_id 必须为数字")

            elif cmd in ("n", "neighbor"):
                if not args_list:
                    print(f"  {Style.red('✗')} 用法: n <node_id>")
                    continue
                try:
                    nid = int(args_list[0])
                    render_neighbor(coord_host, coord_port, nid)
                except ValueError:
                    print(f"  {Style.red('✗')} node_id 必须为数字")

            elif cmd in ("c", "common"):
                if len(args_list) < 2:
                    print(f"  {Style.red('✗')} 用法: c <node_a> <node_b>")
                    continue
                try:
                    a, b = int(args_list[0]), int(args_list[1])
                    render_common(coord_host, coord_port, a, b)
                except ValueError:
                    print(f"  {Style.red('✗')} 节点 ID 必须为数字")

            elif cmd in ("t", "triangle"):
                nid = None
                if args_list:
                    try:
                        nid = int(args_list[0])
                    except ValueError:
                        print(f"  {Style.red('✗')} node_id 必须为数字")
                        continue
                render_triangle(coord_host, coord_port, nid)

            elif cmd in ("stats", "st"):
                render_stats(coord_host, coord_port)

            elif cmd == "edges":
                limit = 20
                if args_list:
                    try:
                        limit = int(args_list[0])
                    except ValueError:
                        pass
                render_edges(coord_host, coord_port, limit)

            elif cmd == "seek":
                if not args_list:
                    print(f"  {Style.red('✗')} 用法: seek <keyword>")
                    continue
                render_seek(coord_host, coord_port, " ".join(args_list))

            elif cmd == "shutdown":
                print(f"  {Style.yellow('⚠')} 确认关闭 Coordinator? (yes/no): ", end="", flush=True)
                try:
                    confirm = input().strip().lower()
                except (EOFError, KeyboardInterrupt):
                    print()
                    continue
                if confirm in ("yes", "y"):
                    p = rpc(coord_host, coord_port, make_msg(MSG_SHUTDOWN, "ui", {}))
                    print(f"  {Style.green('✓')} Coordinator 已关闭")
                    break
                else:
                    print(f"  {Style.dim('已取消')}")
                    continue

            else:
                print(f"  {Style.red('✗')} 未知命令: {cmd}  (输入 help 查看帮助)")
                continue

        except Exception as e:
            print(f"  {Style.red('ERROR')}: {e}")

        elapsed = time.time() - t0
        print(f"  {Style.dim(f'({elapsed:.3f}s)')}")


def main():
    ap = argparse.ArgumentParser(description="test5 交互式图查询 TUI")
    ap.add_argument("--coord-host", default="127.0.0.1")
    ap.add_argument("--coord-port", type=int, default=9000)
    args = ap.parse_args()

    try:
        main_loop(args.coord_host, args.coord_port)
    except KeyboardInterrupt:
        print(f"\n  {Style.gray('再见!')}")
    except Exception as e:
        print(f"\n  {Style.red(f'致命错误: {e}')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
