#!/usr/bin/env python3
"""test4 快速启动助手 — 逐个启动各组件，后台运行"""
import subprocess, sys, time, os, socket

BASE = os.path.dirname(os.path.abspath(__file__))
COORD_PORT = 9900

def wait_port(host, port, timeout=10):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except:
            time.sleep(0.2)
    return False

# 启动 Coordinator
coord = subprocess.Popen(
    [sys.executable, "-u", os.path.join(BASE, "coordinator.py"), "--port", str(COORD_PORT)],
    stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
)
print(f"Coordinator started (PID {coord.pid})")
time.sleep(1)
if not wait_port("127.0.0.1", COORD_PORT, 5):
    print("FAILED: Coordinator not listening")
    coord.kill()
    sys.exit(1)

# 启动 Workers
workers = []
for i in range(5):
    port = 10000 + i
    data = os.path.join(BASE, "workers", f"part_{i}.json")
    if not os.path.exists(data):
        print(f"Skip worker_{i}: {data} not found")
        continue
    w = subprocess.Popen(
        [sys.executable, "-u", os.path.join(BASE, "worker.py"),
         "--worker-id", f"worker_{i}", "--port", str(port),
         "--partition", str(i),
         "--coord-host", "127.0.0.1", "--coord-port", str(COORD_PORT),
         "--data", data],
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
    )
    workers.append(w)
    print(f"Worker_{i} started (PID {w.pid}, :{port})")
    wait_port("0.0.0.0", port, 5)

print(f"\n=== System ready! PID {os.getpid()} ===")
print(f"Coordinator: 127.0.0.1:{COORD_PORT}")
print(f"Workers: {len(workers)} running")
print()

# Keep running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nShutting down...")
finally:
    for w in workers:
        w.terminate()
    coord.terminate()
    print("Done")
