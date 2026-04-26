#!/usr/bin/env python3
"""
test3 快速演示脚本 — 生成数据 → 启动 Coordinator → 启动 5 个 Worker → 查询演示
"""
import os, subprocess, sys, time, json, signal

DIR = os.path.dirname(os.path.abspath(__file__))
WORKERS_DIR = os.path.join(DIR, "workers")
NUM_WORKERS = 5
NUM_NODES = 50
DENSITY = 0.08
SEED = 42

os.makedirs(WORKERS_DIR, exist_ok=True)

def log(msg): print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# 1. 生成数据
log("生成图数据...")
gen_data = os.path.join(DIR, "gen_all.py")
subprocess.run([sys.executable, gen_data, str(NUM_NODES), str(DENSITY), str(SEED),
                "--num-parts", str(NUM_WORKERS), "--out-dir", WORKERS_DIR], check=True)

# 2. 启动 Coordinator
log("启动 Coordinator...")
coord = subprocess.Popen([sys.executable, os.path.join(DIR, "coordinator.py"),
                           "--host", "127.0.0.1", "--port", "9000"])
time.sleep(1)

# 3. 启动 Workers
workers = {}
for i in range(NUM_WORKERS):
    wid = f"node_{i}"
    port = 10000 + i
    data_file = os.path.join(WORKERS_DIR, f"part_{i}.json")
    log(f"启动 Worker {wid} @ :{port} ...")
    wp = subprocess.Popen([sys.executable, os.path.join(DIR, "worker.py"),
                           "--worker-id", wid,
                           "--port", str(port),
                           "--coord-host", "127.0.0.1",
                           "--coord-port", "9000",
                           "--data", data_file,
                           "--partition", str(i)])
    workers[wid] = wp
    time.sleep(0.5)

time.sleep(2)

# 4. 运行查询演示
def client_query(cmd, *args):
    cli = [sys.executable, os.path.join(DIR, "client.py"),
           "--coord-host", "127.0.0.1", "--coord-port", "9000", cmd, *map(str, args)]
    subprocess.run(cli)

log("\n============== 邻居查询 ==============")
client_query("neighbor", 5)
client_query("neighbor", 12)

log("\n============== 共同邻居 ==============")
client_query("common", 5, 20)

log("\n============== 全图三角计数 ==============")
client_query("triangle")

log("\n============== 单节点三角 ==============")
client_query("triangle", 5)
client_query("triangle", 12)

# 5. 清理
log("\n关闭 Worker...")
for wid, wp in workers.items():
    wp.terminate()
    wp.wait(timeout=5)
log("关闭 Coordinator...")
coord.terminate()
coord.wait(timeout=5)
log("完成 ✅")
