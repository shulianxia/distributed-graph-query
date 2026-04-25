#!/usr/bin/env python3
"""
test2 快速演示脚本（纯 Python 版）
用法:  python3 start_demo.py

等同于 deploy.sh，但更可控（日志可见）。
"""
import logging
import os
import subprocess
import sys
import time

DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(DIR, "workers")
os.makedirs(DATA_DIR, exist_ok=True)

NUM_WORKERS = 5
NUM_NODES = 50
EDGE_DENSITY = 0.08
COORD_PORT = 9999
BASE_PORT = 9100

procs = []
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

print("=" * 60)
print(" Distributed Graph Query System — Demo")
print("=" * 60)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")


def run_py(args, capture=False):
    """运行 python3 脚本"""
    cmd = [sys.executable] + args
    if capture:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return r.stdout, r.stderr
    else:
        return subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True, bufsize=1
        )


log(f"Step 1/5: 生成 {NUM_WORKERS} 个分区数据文件...")
for i in range(NUM_WORKERS):
    data_file = os.path.join(DATA_DIR, f"part_{i}.json")
    run_py([
        os.path.join(DIR, "worker.py"),
        "--generate", str(NUM_NODES), str(EDGE_DENSITY), "42", data_file,
        "--worker-id", f"node_{i}",
    ], capture=True)
log("  ✓ 数据文件就绪")

log(f"Step 2/5: 启动 Coordinator (:{COORD_PORT})...")
c = run_py([os.path.join(DIR, "coordinator.py"), "--port", str(COORD_PORT)])
procs.append(c)
time.sleep(1)
log(f"  ✓ Coordinator PID={c.pid}")

log(f"Step 3/5: 启动 {NUM_WORKERS} 个 Worker...")
for i in range(NUM_WORKERS):
    port = BASE_PORT + i
    data_file = os.path.join(DATA_DIR, f"part_{i}.json")
    w = run_py([
        os.path.join(DIR, "worker.py"),
        "--worker-id", f"node_{i}",
        "--port", str(port),
        "--coord-host", "127.0.0.1",
        "--coord-port", str(COORD_PORT),
        "--data-file", data_file,
    ])
    procs.append(w)
    log(f"  ✓ Worker node_{i} @ :{port} (PID={w.pid})")
    time.sleep(0.3)

log("Step 4/5: 等待系统就绪...")
time.sleep(3)
log("  ✓ 系统就绪\n")

log("Step 5/5: 运行演示查询...\n")

client = os.path.join(DIR, "client.py")
coord_args = ["--coord-host", "127.0.0.1", "--coord-port", str(COORD_PORT)]

queries = [
    ("neighbor 5", ["neighbor", "5"]),
    ("common 10 20", ["common", "10", "20"]),
    ("triangle 5", ["triangle", "5"]),
    ("triangle (全图)", ["triangle"]),
]

for label, args in queries:
    log(f"── {label} ──")
    out, err = run_py([client] + coord_args + args, capture=True)
    if out:
        print(out)
    if err:
        print(err)
    print()

log("演示完成，关闭所有进程...")
for p in list(procs):
    p.terminate()
    p.wait(timeout=5)
log("所有进程已关闭")
