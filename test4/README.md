# 进程通信图查询系统 — test4（应用版）

## 概述

将分布式图查询算法（test3 预取版）应用到**真实场景**：Linux 进程间通信关系图。

与 test1-test3 使用随机生成图数据不同，test4 直接从 `/proc` 文件系统**实时采集**当前操作系统的进程通信关系，构建图并分布式存储，然后通过 test3 的查询框架进行分析。

## 采集的通信类型

| 边类型 | 数据源 | 说明 |
|--------|--------|------|
| `parent_child` | `/proc/PID/status` (PPID) | 父进程创建子进程的层次关系 |
| `pipe` | `/proc/PID/fd` → `pipe:inode` | 进程通过管道通信（共享 inode 匹配对端） |
| `unix_socket` | `/proc/PID/net/unix` | UNIX domain socket 连接 |
| `net_socket` | `/proc/PID/net/tcp` + `/proc/PID/net/udp` | 网络 socket 连接 |

## 架构

```text
┌──────────┐
│  Client  │  ←── 交互式查询：info/neighbor/common/triangle/cluster/stats
└─────┬────┘
      │ TCP/JSON（短连接）
┌─────▼──────┐
│ Coordinator│  ←── Worker 注册表 + 预取式三角
│ :9900      │
└──┬──┬──┬───┘
   │  │  │      TCP/JSON（连接池）
┌──▼──▼──▼──┐
│ Workers    │  ←── 进程通信图分区数据
│ :10000-4   │      」
└────────────┘
```

## 文件结构

```
test4/
├── collector.py     ← 从 /proc 采集进程通信关系，哈希分区到 N 个文件
├── protocol.py      ← 共享协议（复用 test3，增加进程属性查询）
├── coordinator.py   ← 预取式 Coordinator（路由：info/nlist/triangle/common）
├── worker.py        ← Worker 服务器
├── client.py        ← 命令行客户端（6 种查询命令）
├── launcher.py      ← 快速启动脚本（Coordinator + 5 Workers）
├── start_demo.py    ← 一键演示：采集 → 分区 → 启动 → 查询 → 关闭
├── workers/         ← 采集生成的分区数据 JSON
└── __pycache__/
```

## 查询命令大全

```bash
# 进程详细信息
python3 client.py info <pid>

# 通信伙伴列表（按类型分组）
python3 client.py neighbor <pid>     # 或 n

# 共同通信伙伴
python3 client.py common <a> <b>     # 或 c

# 三角通信关系（全图 / 单节点）
python3 client.py triangle            # 全图三角计数
python3 client.py triangle <pid>      # 该进程参与的三角

# 2 跳通信子网
python3 client.py cluster <pid>       # 或 cl

# 统计概览
python3 client.py stats

# 列出所有通信边
python3 client.py edges
```

示例输出：

```text
$ python3 client.py info 1
============================================================
  进程 1 详细信息
============================================================

  进程名: systemd | 状态: 睡眠 | UID: 0 | 父进程: 0

  通信度: 126

  通信关系 (126 个):
    父子关系 (114): [2762, 2769, 2808, ...]
    UNIX Socket (12): [2768, 2770, 2802, ...]

耗时: 0.003s
```

```text
$ python3 client.py triangle
全系统三角通信关系总数: 1552 个

耗时: 0.009s
```

## 快速启动

```bash
cd test4/

# 一键启动（自动采集+分区+启动+等待客户端）
python3 launcher.py

# 另开终端查询
python3 client.py info 1
python3 client.py triangle
python3 client.py shutdown
```

或者完整的 start_demo（自动关闭）：

```bash
python3 start_demo.py
```

## 技术要点

### 管道匹配算法

管道在 Linux 中通过 `pipe` 系统调用创建，两端 fd 指向同一个内核 inode。识别流程：

1. 遍历所有进程的 `/proc/PID/fd/` 目录
2. 对每个 fd 符号链接，stat 检查是否为 FIFO/pipe 类型
3. 提取 inode 号：`os.readlink()` → 解析 `pipe:[INODE]` 格式
4. 相同 inode 号出现在两个不同进程 → 管道通信边

### UNIX Socket 匹配

1. 从 `/proc/PID/net/unix` 读取 socket 条目
2. 提取 3 列（slot、flags、inode）
3. 筛选非 LISTEN 状态的 socket
4. 不同进程使用相同 inode 的 socket → UNIX socket 通信边

### 预取三角计数的优势

采集 200 进程、401 条边、1552 个三角：
- **5 次 RPC** 拉取全量邻接表 → 本地计算
- Coord 端耗时 **9ms**（含网络通信）
- 如果沿用逐边方案：需要 401 次 RPC，预计数百毫秒

## 与 test3 的关系

test4 完全复用 test3 的分布式查询框架（protocol/protocol.py），差异仅在于数据来源：

| 维度 | test3 | test4 |
|------|-------|-------|
| 数据来源 | 随机生成 Erdos-Renyi 图 | 实时采集 `/proc` |
| 图语义 | 无属性、无意义的节点ID | 进程 PID + 属性（名称/状态/UID） |
| 边属性 | 无 | `parent_child` / `pipe` / `unix_socket` / `net_socket` |
| 查询 | 通用邻居/三角 | 进程专属：info + 按通信类型分组展示 |
| 应用场景 | 算法验证/性能测试 | 真实系统诊断：进程通信关系分析 |
