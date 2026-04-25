# 分布式图查询系统

多主机分布式图数据库查询系统。图数据分片存储在多台机器的 Worker 节点上，Coordinator 协调路由查询请求。

## 架构

```
┌──────────┐
│  Client  │  ←── 命令行工具：邻居/共同邻居/三角查询
└─────┬────┘
      │ TCP/JSON RPC
┌─────▼──────┐
│ Coordinator│  ←── Worker 注册表 + 查询路由
│ :9000      │      不存全图数据
└──┬──┬──┬───┘
   │  │  │      TCP/JSON RPC
┌──▼──▼──▼──┐
│ Workers    │  ←── 独立进程，各自存本地分区数据文件
│ :9100-9104 │      仅加载自己分区（part_N.json）
└────────────┘
```

**三大组件：**
- **Worker** — 独立进程。从本地 `workers/part_N.json` 加载图数据。启动时主动向 Coordinator 注册。处理邻居查询、共同邻居查询、三角检测
- **Coordinator** — 协调节点。维护 Worker 注册表。根据一致性哈希（MD5 取模）定位节点所在 Worker。转发查询请求。分布式全图三角计数时收集各 Worker 数据协同计算
- **Client** — 命令行工具。向 Coordinator 发送查询请求

**一致性哈希分区：** `partition = int(md5(str(node_id).encode()).hexdigest(), 16) % NUM_PARTITIONS`

## 分布式三角计数（核心难点）

三角形顶点可能分布在 2~3 台不同的 Worker 上。例如三角形 `(0,31,48)`：节点 0 在分区 0，节点 31 在分区 1，节点 48 在分区 2。

**两阶段协同策略：**
1. **收集全图边** — Coordinator 向所有 Worker 索取边集合，去重得到全图边列表
2. **对每条边 `(u,v)` 并行查询** — 同时向 u 所在 Worker 和 v 所在 Worker 索取邻居列表
3. **Coordinator 本地求交集** — `neighbors(u) ∩ neighbors(v)` = 共同邻居 w，即三角形 `(u,v,w)`

为什么不能委托给单一 Worker？Worker A 有 u 的局部邻接表但可能不知道 v 的完整信息，跨分区时必然漏三角。

## 快速启动

```bash
cd test2/

# 方式一：一键演示（数据生成 → 启动 → 执行查询 → 关闭）
python3 start_demo.py

# 方式二：逐步启动
# 1. 生成数据
python3 gen_all.py 50 0.08 42 --num-parts 5 --out-dir workers

# 2. 启动 Coordinator（后台）
python3 coordinator.py --port 9999 &

# 3. 启动 5 个 Worker（后台）
python3 worker.py --worker-id node_0 --port 9100 --data-file workers/part_0.json --coord-port 9999 &
python3 worker.py --worker-id node_1 --port 9101 --data-file workers/part_1.json --coord-port 9999 &
python3 worker.py --worker-id node_2 --port 9102 --data-file workers/part_2.json --coord-port 9999 &
python3 worker.py --worker-id node_3 --port 9103 --data-file workers/part_3.json --coord-port 9999 &
python3 worker.py --worker-id node_4 --port 9104 --data-file workers/part_4.json --coord-port 9999 &

sleep 3  # 等待注册完成

# 4. 查询
python3 client.py neighbor 5
python3 client.py common 10 20
python3 client.py triangle 5
python3 client.py triangle                       # 全图三角计数

# 5. 关闭
python3 client.py shutdown
```

## 跨多台主机部署

在每台机器上分别运行 Worker：

**机器 A（运行 Coordinator + Worker 0）：**
```bash
python3 coordinator.py --host 0.0.0.0 --port 9999 &
python3 worker.py --worker-id node_0 --port 9100 --coord-host <机器A_IP> --coord-port 9999 --data-file workers/part_0.json &
```

**机器 B：**
```bash
python3 worker.py --worker-id node_1 --port 9101 --coord-host <机器A_IP> --coord-port 9999 --data-file workers/part_1.json &
```

**控制台（任意机器）：**
```bash
python3 client.py --coord-host <机器A_IP> --coord-port 9999 neighbor 5
```

> 需将 `protocol.py` 和对应的 `worker.py` + 数据文件拷贝到每台机器，并在防火墙放行端口。

## 查询命令

| 命令 | 说明 | 示例 |
|------|------|------|
| `neighbor <节点ID>` | 查询节点的邻居列表和度数 | `neighbor 5` |
| `common <节点A> <节点B>` | 查询两节点的共同邻居 | `common 10 20` |
| `triangle [节点ID]` | 查询节点参与的三角形（空=全图计数） | `triangle 5` / `triangle` |
| `shutdown` | 关闭 Coordinator | `shutdown` |

## 依赖

**Python 3.10+，仅标准库，零外部依赖。**
- `socket` — TCP 通信
- `threading` — 并发处理
- `json` / `struct` / `hashlib` — 序列化与哈希
- `argparse` — 命令行解析

## 文件结构

```
test2/
├── README.md              # 本文件
├── protocol.py            # 共享协议：图数据结构、消息编解码、一致性哈希
├── worker.py              # Worker 节点（独立进程）
├── coordinator.py         # Coordinator 协调节点
├── client.py              # 命令行客户端
├── gen_all.py             # 中心化数据生成器（推荐使用，保证跨分区一致性）
├── deploy.sh              # 一键部署演示脚本
├── start_demo.py          # Python 版演示脚本
└── workers/               # 数据文件目录（由 gen_all.py 或 generate 命令创建）
    ├── part_0.json
    ├── part_1.json
    ├── ...
```

## 验证

系统经过 **完全一致性验证**（50 节点 5 分区）：
- 分布式三角计数结果 = 完整图（合并所有分区）三角计数结果 ✅
- 跨分区三角形（顶点分布在 2~3 台 Worker）正确计数 ✅
- 跨 Worker 共同邻居查询（`common`）正确 ✅
