#!/usr/bin/env python3
"""
test6 一键启动脚本 — 生成数据 → 启动 Coordinator → 启动 Workers → 打开 GUI

用法：
  python3 start_demo.py              # 默认 200 节点
  python3 start_demo.py 500 0.06     # 自定义规模
  python3 start_demo.py --no-gui     # 只启动服务，不打开 GUI
  python3 start_demo.py 100 0.1 --parts 8
"""
import os, signal, subprocess, sys, time, socket

BASE = os.path.dirname(os.path.abspath(__file__))
COORD_PORT = 9000
WORKER_BASE_PORT = 9100


def print_title(text):
    print(f"\n  {text}")

def print_ok(text):
    print(f"  {chr(10003)} {text}")

def print_warn(text):
    print(f"  {chr(9888)} {text}")

def print_info(text):
    print(f"    {text}")

def wait_port(host, port, timeout=15):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except:
            time.sleep(0.5)
    return False

def cleanup():
    subprocess.run(["pkill", "-f", "coordinator.py --port"], capture_output=True)
    subprocess.run(["pkill", "-f", "worker.py --worker-id w"], capture_output=True)
    time.sleep(1.5)


def main():
    import argparse
    ap = argparse.ArgumentParser(description="test6 分布式图查询 GUI 版")
    ap.add_argument("nodes", type=int, nargs="?", default=200,
                    help="节点数 (默认 200)")
    ap.add_argument("density", type=float, nargs="?", default=0.08,
                    help="边密度 (默认 0.08)")
    ap.add_argument("--parts", type=int, default=5,
                    help="分区数 (默认 5)")
    ap.add_argument("--no-gui", action="store_true",
                    help="只启动服务，不打开 GUI")
    ap.add_argument("--coord-port", type=int, default=COORD_PORT,
                    help=f"Coordinator 端口 (默认 {COORD_PORT})")
    args = ap.parse_args()

    workers_dir = os.path.join(BASE, "workers")
    coord_port = args.coord_port
    num_parts = args.parts
    worker_base = WORKER_BASE_PORT

    print()
    print("  " + "=" * 48)
    print("  test6  分布式图查询系统 · GUI 版")
    print("  " + "=" * 48)

    # 1. 清理
    print_title("[1/4] 清理旧进程...")
    cleanup()
    print_ok("旧进程已清理")

    # 2. 生成数据
    print_title(f"[2/4] 生成 {args.nodes} 节点图数据 ({args.density} 密度)...")
    result = subprocess.run([
        sys.executable, os.path.join(BASE, "gen_all.py"),
        str(args.nodes), str(args.density), "42",
        "--num-parts", str(num_parts),
        "--out-dir", workers_dir,
    ], cwd=BASE, capture_output=True, text=True)
    for line in result.stdout.strip().split("\n"):
        print_info(line.strip())
    if result.returncode != 0:
        print_warn(f"数据生成失败: {result.stderr}")
        sys.exit(1)
    print_ok("数据已生成")

    # 3. 启动 Coordinator
    print_title(f"[3/4] 启动 Coordinator @ :{coord_port}...")
    proc_coord = subprocess.Popen(
        [sys.executable, "-u", os.path.join(BASE, "coordinator.py"),
         "--port", str(coord_port)],
        cwd=BASE,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    )
    if not wait_port("127.0.0.1", coord_port):
        print_warn("Coordinator 启动失败")
        proc_coord.kill()
        sys.exit(1)
    print_ok(f"Coordinator 就绪 (: {coord_port})")

    # 4. 启动 Workers
    print_title(f"[4/4] 启动 {num_parts} 个 Worker...")
    workers = []
    for i in range(num_parts):
        port = worker_base + i
        proc = subprocess.Popen(
            [sys.executable, os.path.join(BASE, "worker.py"),
             f"--worker-id=w{i}",
             f"--port={port}",
             f"--partition={i}",
             "--coord-host=127.0.0.1",
             f"--coord-port={coord_port}",
             f"--data={workers_dir}/part_{i}.json"],
            cwd=BASE,
            stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        )
        workers.append(proc)
        print_info(f"  w{i} @ :{port}")

    time.sleep(2)

    all_ok = True
    for i in range(num_parts):
        if not wait_port("0.0.0.0", worker_base + i, timeout=5):
            print_warn(f"  w{i} 未就绪")
            all_ok = False
    if all_ok:
        print_ok(f"{num_parts} 个 Worker 全部就绪")

    # 摘要
    print()
    print("  " + chr(9472) * 46)
    print("  服务状态:")
    print_info(f"Coordinator:  127.0.0.1:{coord_port}")
    for i in range(num_parts):
        print_info(f"Worker w{i}:    :{worker_base + i}")
    print("  " + chr(9472) * 46)
    print()

    # 5. 启动 GUI
    if args.no_gui:
        print_info("GUI 未启动，请在另一终端运行:")
        print_info(f"  python3 ui_app.py --coord-port {coord_port}")
        print()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
    else:
        print_info("启动图形界面...")
        try:
            subprocess.run([
                sys.executable, os.path.join(BASE, "ui_app.py"),
                "--coord-host", "127.0.0.1",
                "--coord-port", str(coord_port),
            ])
        except KeyboardInterrupt:
            pass

    # 关闭
    print_title("正在关闭服务...")
    cleanup()
    print_ok("已关闭")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
        cleanup()
        print_ok("已关闭")
