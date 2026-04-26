#!/usr/bin/env python3
"""
进程通信图查询系统 — 一键启动脚本

流程：
  1. 运行 collector.py 采集 /proc 数据并分发到分区
  2. 启动 Coordinator
  3. 启动 5 个 Worker（各加载一个分区）
  4. 输出客户端连接提示
"""
import os, signal, subprocess, sys, time

BASE = os.path.dirname(os.path.abspath(__file__))
COORD_PORT = 9900
WORKER_BASE_PORT = 10000
NUM_PARTS = 5

def log(msg):
    print(f"[start_demo] {msg}")

def wait_for_port(host, port, timeout=10):
    import socket
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection((host, port), timeout=1)
            s.close()
            return True
        except:
            time.sleep(0.2)
    return False

def main():
    procs = []

    try:
        print("=" * 60)
        print("  进程通信图查询系统 v1.0")
        print("=" * 60)
        print()

        # Step 1: 采集数据
        log("采集 /proc 进程通信数据...")
        workers_dir = os.path.join(BASE, "workers")
        collector = os.path.join(BASE, "collector.py")
        # 先清理旧数据
        import shutil
        if os.path.exists(workers_dir):
            shutil.rmtree(workers_dir)
        result = subprocess.run(
            [sys.executable, collector, "--max-pids", "200", "--num-parts", str(NUM_PARTS), "--out-dir", workers_dir],
            capture_output=True, text=True, timeout=60,
        )
        print(result.stdout)
        if result.returncode != 0:
            log(f"采集失败:\n{result.stderr}")
            sys.exit(1)
        log("数据采集完成")
        print()

        # Step 2: 启动 Coordinator
        log(f"启动 Coordinator @ :{COORD_PORT}...")
        coord = subprocess.Popen(
            [sys.executable, os.path.join(BASE, "coordinator.py"),
             "--port", str(COORD_PORT)],
        )
        procs.append(("Coordinator", coord))
        time.sleep(1)
        if not wait_for_port("127.0.0.1", COORD_PORT, 5):
            log("Coordinator 启动失败!")
            sys.exit(1)
        log("Coordinator 已就绪")
        print()

        # Step 3: 启动 Workers
        for i in range(NUM_PARTS):
            wid = f"worker_{i}"
            port = WORKER_BASE_PORT + i
            data = os.path.join(workers_dir, f"part_{i}.json")
            if not os.path.exists(data):
                log(f"分区数据 {data} 不存在，跳过")
                continue
            log(f"启动 {wid} @ :{port} (分区 {i})...")
            worker = subprocess.Popen(
                [sys.executable, os.path.join(BASE, "worker.py"),
                 "--worker-id", wid,
                 "--port", str(port),
                 "--partition", str(i),
                 "--coord-host", "127.0.0.1",
                 "--coord-port", str(COORD_PORT),
                 "--data", data],
            )
            procs.append((wid, worker))

        # 等待 Worker 注册
        time.sleep(2)

        print()
        print("=" * 60)
        print("  系统就绪！")
        print("=" * 60)
        print(f"  Coordinator: 127.0.0.1:{COORD_PORT}")
        print(f"  Workers: {NUM_PARTS} 个")
        print()
        print("  客户端查询示例:")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} info 1            # PID=1 的进程信息")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} neighbor 1        # PID=1 的通信伙伴")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} common 1 2        # PID 1 和 2 的共同伙伴")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} triangle           # 全系统三角通信")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} cluster 1         # PID=1 的通信子网")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} stats              # 统计概览")
        print(f"    python3 {os.path.join(BASE, 'client.py')} --coord-port {COORD_PORT} shutdown           # 关闭系统")
        print("=" * 60)

        # 等待直到用户 Ctrl+C
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        log("收到中断信号，关闭系统...")
    finally:
        for name, proc in reversed(procs):
            log(f"关闭 {name}...")
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except:
                try:
                    proc.kill()
                except:
                    pass
        log("系统已关闭")

if __name__ == "__main__":
    main()
