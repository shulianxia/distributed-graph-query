# 分布式图查询系统 — test3（连接池 + 预取优化版）

## 概述

在 test2 分布式架构基础上，针对**全图三角计数**这一核心场景做了两项关键优化。

| 对比 | test2（初代分布式） | test3（优化版） |
|------|-------------------|----------------|
| 通信连接 | 短连接（每次RPC新建TCP） | 连接池 ConnPool（长连接复用） |
| 三角计数策略 | 逐边查询 O(E) 次 RPC | 预取全量邻接表 O(P) 次 RPC（P=Worker数） |
| Large 规模(1k节点) | 2.3s，20000 次 RPC | 0.04s，5 次 RPC |
| 架构瓶颈 | Coordinator 成为 RPC 热点 | 预取后仅极少数网络交互 |

large 规模下 test3 比 test2 **快 58.7 倍**（2.3s → 0.04s），比 test1 单机版快 50 倍。

## 架构

```text
┌──────────┐
│  Client  │  ←── 命令行：neighbor/common/triangle(预取)
└─────┬────┘
      │ TCP/JSON RPC（短连接）
┌─────▼──────┐
│ Coordinator│  ←── Worker 注册表 + 查询路由
│ :9000      │      预取式三角计数：5次RPC拉全图 → 本地算
└──┬──┬──┬───┘
   │  │  │      TCP/JSON RPC（连接池长连接）
┌──▼──▼──▼──┐
│ Workers    │  ←── 独立进程，各存本地分区
│ :9100-9104 │      支持批量 nlist 查询
└────────────┘
```

## 文件结构

```
test3/
├── protocol.py      ← 共享协议：GraphStorage + 连接池 + 4字节TCP帧
├── coordinator.py   ← 预取式 Coordinator（全图三角仅 5 次 RPC）
├── worker.py        ← Worker 服务器（短连接 + 预取查询）
├── client.py        ← 命令行客户端
├── gen_all.py       ← 随机图数据生成器
├── start_demo.py    ← 一键启动：生成数据 → 启动 → 查询 → 关闭
├── benchmark_scale.py
└── workers/         ← 生成的分区数据文件
```

## 快速启动

```bash
cd test3/

# 一键演示
python3 start_demo.py

# 或逐步
python3 gen_all.py 50 0.08 42 --num-parts 5 --out-dir workers

# 后台启动（或用 start_demo.py 自动管理）
python3 coordinator.py --port 9000 &
python3 worker.py --worker-id w0 --port 9100 --partition 0 \
  --coord-host 127.0.0.1 --coord-port 9000 --data workers/part_0.json &
# ... 同理启动 w1-w4

# 查询
python3 client.py neighbor 5
python3 client.py triangle 25
python3 client.py triangle        # 全图三角（预取）
```

## 关键优化细节

### 1. 连接池（ConnPool）

Coordinator 向 Worker 发起连接后，不立即关闭，而是放入连接池复用。避免每次查询握手开销。

```python
class ConnPool:
    def __init__(self, max_idle=30):
        self._pool = {}   # (host, port) → [sockets]
```

### 2. 预取式三角计数

**原始方案（test2）**：对每条边 `(u,v)` 分别向两端 Worker 查询邻居 → O(E) 次 RPC

**预取方案（test3）**：
1. Coordinator 向所有 P 个 Worker 发送 `query_all`，拉取全量邻接表
2. 本地组合完整邻接集合（只需合并 P 个分区的邻接表）
3. 在 Coordinator 内存中直接计算所有三角形

```python
def query_triangles_global(self):
    all_adj = defaultdict(set)
    for worker_info in self.workers.values():
        resp = self._rpc(worker_info, make_msg(MSG_QUERY_ALL, "coord", {}))
        for nid, nbrs in resp.get("payload", {}).get("adjacency", {}).items():
            all_adj[int(nid)].update(nbrs)
    # 本地计算三角形 ...
```

### 3. 基准测试结果

| 规模 | 节点 | 边 | 三角 | test2 耗时 | test3 耗时 | 加速比 |
|------|------|----|------|-----------|-----------|--------|
| small | 50 | 93 | 64 | 0.020s | 0.012s | 1.7x |
| medium | 200 | 750 | 574 | 0.316s | 0.019s | 16.6x |
| large | 1000 | 7996 | 10467 | 2.347s | 0.040s | **58.7x** |

## 与 test2 的主要区别

| 特性 | test2 | test3 |
|------|-------|-------|
| Coordinator-Worker 连接 | 短连接，每次新建 | 连接池复用 |
| Worker 处理器注册 | `wait()` + dict | 短连接每请求一个连接 |
| 全图三角 | 逐边：O(E) 次 RPC | 预取：O(P) 次 RPC |
| 跨主机部署 | 支持 | 支持 |
| benchmark 脚本 | `_bench.py`（单独文件） | `benchmark_scale.py`（集成测试） |
