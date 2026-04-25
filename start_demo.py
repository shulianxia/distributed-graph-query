#!/usr/bin/env python3
"""
test2 快速演示脚本 — 生成数据 → 启动 Coordinator → 启动 5 个 Worker → 查询演示
"""
import os, subprocess, sys, time, json, signal

DIR = os.path.dirname(os.path.abspath(__file__))
WORKERS_DIR = os.path.join(DIR, "workers")
os.makedirs(WORKERS_DIR, exist_ok=True)

NUM_NODES = 50
DENSITY = 0.08
SEED = 42
NUM_WORKERS = 5
COORD_PORT = 9000

procs = []

def log(msg):
    print(f"[demo] {msg}", flush=True)

def cleanup():
    log("清理进程...")
    for p in procs:
        try: p.terminate()
        except: pass
    for p in procs:
        try: p.wait(timeout=3)
        except: pass

signal.signal(signal.SIGINT, lambda *a: (cleanup(), sys.exit(0)))
signal.signal(signal.SIGTERM, lambda *a: (cleanup(), sys.exit(0)))

gen_data = os.path.join(DIR, "gen_all.py")
log("使用 gen_all.py 生成图数据...")
subprocess.run([sys.executable, gen_data, str(NUM_NODES), str(DENSITY), str(SEED),
                "--num-parts", str(NUM_WORKERS), "--out-dir", WORKERS_DIR],
               check=True, capture_output=True)

# 2. 启动 Coordinator
log("启动 Coordinator...")
p = subprocess.Popen([
    sys.executable, os.path.join(DIR, "coordinator.py"),
    "--port", str(COORD_PORT),
], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
procs.append(p)
time.sleep(1)

# 3. 启动 Worker
log("启动 Workers...")
for i in range(NUM_WORKERS):
    port = 9100 + i
    data_file = os.path.join(WORKERS_DIR, f"part_{i}.json")
    p = subprocess.Popen([
        sys.executable, os.path.join(DIR, "worker.py"),
        "--worker-id", f"node_{i}",
        "--host", "127.0.0.1",
        "--port", str(port),
        "--coord-host", "127.0.0.1",
        "--coord-port", str(COORD_PORT),
        "--data-file", data_file,
        "--num-parts", str(NUM_WORKERS),
    ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    procs.append(p)

log("等待 Worker 注册...")
time.sleep(2)

# 4. 执行查询
def q(args):
    r = subprocess.run([
        sys.executable, os.path.join(DIR, "client.py"),
        "--coord-host", "127.0.0.1",
        "--coord-port", str(COORD_PORT),
    ] + args, capture_output=True, text=True)
    return r.stdout + r.stderr

log("=== 测试 1: 邻居查询 (节点 5) ===")
print(q(["neighbor", "5"]))

log("=== 测试 2: 邻居查询 (节点 42) ===")
print(q(["neighbor", "42"]))

log("=== 测试 3: 共同邻居 (跨分区) ===")
print(q(["common", "5", "18"]))

log("=== 测试 4: 单节点三角 (节点 5) ===")
print(q(["triangle", "5"]))

log("=== 测试 5: 全图三角计数 (分布式) ===")
print(q(["triangle"]))

log("=== 测试 6: shutdown ===")
print(q(["shutdown"]))

cleanup()
log("完成")
