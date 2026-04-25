#!/usr/bin/env bash
# 分布式图查询系统 — 一键部署 + 演示脚本
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
PROTOCOL="$DIR/protocol.py"
WORKER="$DIR/worker.py"
COORD="$DIR/coordinator.py"
CLIENT="$DIR/client.py"
DATA_DIR="$DIR/workers"
COORD_PORT=9000

# 颜色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  分布式图查询系统 — 一键部署 + 演示${NC}"
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo ""

# ─── 1. 生成 Worker 数据文件 ───
echo -e "${GREEN}[1/5] 生成图数据文件...${NC}"
mkdir -p "$DATA_DIR"

NUM_WORKERS=5
NUM_NODES=50
EDGE_DENSITY=0.08

for i in $(seq 0 $((NUM_WORKERS - 1))); do
    python3 "$WORKER" \
        --generate "$NUM_NODES" "$EDGE_DENSITY" 42 "$DATA_DIR/part_${i}.json" \
        --worker-id "node_${i}" 2>&1 | grep -v "^$"
done

# 生成 Worker 注册信息映射文件
echo "{" > "$DATA_DIR/partition_map.json"
for i in $(seq 0 $((NUM_WORKERS - 1))); do
    wid="node_${i}"
    port=$((9100 + i))
    if [ $i -lt $((NUM_WORKERS - 1)) ]; then
        echo "  \"${wid}\": {\"host\": \"0.0.0.0\", \"port\": ${port}}," >> "$DATA_DIR/partition_map.json"
    else
        echo "  \"${wid}\": {\"host\": \"0.0.0.0\", \"port\": ${port}}" >> "$DATA_DIR/partition_map.json"
    fi
done
echo "}" >> "$DATA_DIR/partition_map.json"

echo -e "${GREEN}  ✓ 生成 ${NUM_WORKERS} 个分区数据文件 (${NUM_NODES} 节点)${NC}"
echo ""

# ─── 2. 启动 Coordinator ───
echo -e "${GREEN}[2/5] 启动 Coordinator (端口 ${COORD_PORT})...${NC}"
python3 "$COORD" --port $COORD_PORT &
COORD_PID=$!
sleep 1
echo -e "${GREEN}  ✓ PID: ${COORD_PID}${NC}"
echo ""

# ─── 3. 启动 Worker 节点 ───
echo -e "${GREEN}[3/5] 启动 ${NUM_WORKERS} 个 Worker 节点...${NC}"
WORKER_PIDS=()
for i in $(seq 0 $((NUM_WORKERS - 1))); do
    port=$((9100 + i))
    python3 "$WORKER" \
        --worker-id "node_${i}" \
        --port $port \
        --coord-host 127.0.0.1 \
        --coord-port $COORD_PORT \
        --data-file "$DATA_DIR/part_${i}.json" \
        &
    WPID=$!
    WORKER_PIDS+=($WPID)
    echo -e "${GREEN}  ✓ Worker node_${i} @ :${port} (PID ${WPID})${NC}"
    sleep 0.5
done
echo ""

# ─── 4. 等待就绪 ───
echo -e "${GREEN}[4/5] 等待系统就绪 (5s)...${NC}"
sleep 3
echo -e "${GREEN}  ✓ 系统就绪${NC}"
echo ""

# ─── 5. 演示查询 ───
echo -e "${GREEN}[5/5] 运行演示查询...${NC}"
echo ""

echo -e "${YELLOW}─── 邻居查询 (节点 5) ───${NC}"
python3 "$CLIENT" --coord-host 127.0.0.1 --coord-port $COORD_PORT neighbor 5
echo ""

echo -e "${YELLOW}─── 共同邻居 (节点 10 和 20) ───${NC}"
python3 "$CLIENT" --coord-host 127.0.0.1 --coord-port $COORD_PORT common 10 20
echo ""

echo -e "${YELLOW}─── 单节点三角 (节点 5) ───${NC}"
python3 "$CLIENT" --coord-host 127.0.0.1 --coord-port $COORD_PORT triangle 5
echo ""

echo -e "${YELLOW}─── 全图三角计数 ───${NC}"
python3 "$CLIENT" --coord-host 127.0.0.1 --coord-port $COORD_PORT triangle
echo ""

# ─── 退出 ───
echo -e "${CYAN}══════════════════════════════════════════════════${NC}"
echo -e "${BLUE}演示完成，关闭所有进程...${NC}"

# 关闭 Coordinator（会触发 Worker 退出）
python3 "$CLIENT" --coord-host 127.0.0.1 --coord-port $COORD_PORT shutdown 2>/dev/null || true
sleep 1

# 确保所有进程退出
for pid in "${WORKER_PIDS[@]}"; do
    kill $pid 2>/dev/null || true
done
kill $COORD_PID 2>/dev/null || true

echo -e "${GREEN}所有进程已关闭${NC}"
