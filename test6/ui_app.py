#!/usr/bin/env python3
"""
test6 — PyQt5 图形界面版分布式图查询系统

设计理念：
  - 简洁：主界面只放最常用的 5 个功能按钮 + 结果面板
  - 直观：左侧功能面板，右侧结果显示区域
  - 零学习成本：输入节点ID，点击按钮，结果即刻显示

使用 Python 标准库 + PyQt5（仅此两个依赖）。
"""

import sys, os, socket, json, time, threading
from collections import defaultdict

# ── 协议层 ──
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from protocol import (
    make_msg, pack_msg, recv_msg, MSG_RESULT_OK,
    MSG_QUERY_NEIGHBOR, MSG_QUERY_NLIST, MSG_QUERY_COMMON,
    MSG_QUERY_TRIANGLE, MSG_QUERY_STATS, MSG_QUERY_NODE_INFO,
)

try:
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QGridLayout, QLabel, QLineEdit, QPushButton, QTextEdit,
        QGroupBox, QSplitter, QStatusBar, QFrame, QMessageBox,
        QInputDialog, QDialog, QDialogButtonBox,
    )
    from PyQt5.QtCore import Qt, QThread, pyqtSignal, QSize
    from PyQt5.QtGui import QFont, QTextCursor, QColor, QPalette, QIcon
except ImportError:
    print("需要 PyQt5: pip3 install PyQt5")
    sys.exit(1)


# ── 常量 ──
COLOR_BG = "#1e1e2e"
COLOR_SURFACE = "#2a2a3e"
COLOR_PRIMARY = "#7c3aed"
COLOR_PRIMARY_LIGHT = "#a78bfa"
COLOR_SUCCESS = "#22c55e"
COLOR_WARN = "#f59e0b"
COLOR_TEXT = "#e2e8f0"
COLOR_TEXT_DIM = "#94a3b8"
COLOR_BORDER = "#3b3b5c"
COLOR_NODE_BG = "#312e81"
COLOR_EDGE_BG = "#1e3a5f"

FONT_MONO = "JetBrains Mono, Fira Code, Consolas, monospace"
FONT_UI = "Noto Sans CJK SC, Segoe UI, sans-serif"


# ── 网络客户端（线程安全） ──
class DGraphClient:
    """封装与 Coordinator 的 RPC 通信"""

    def __init__(self, host="127.0.0.1", port=9000):
        self.host = host
        self.port = port
        self.sender = "test6_gui"

    def _rpc(self, msg_type, payload, timeout=30):
        try:
            s = socket.create_connection((self.host, self.port), timeout=5)
            msg = make_msg(msg_type, self.sender, payload)
            s.sendall(pack_msg(msg))
            resp = recv_msg(s)
            s.close()
            if resp and resp["msg_type"] == MSG_RESULT_OK:
                return resp.get("payload", {})
            return {"error": resp.get("payload", {}).get("error", "查询失败") if resp else "无响应"}
        except socket.timeout:
            return {"error": "连接超时，请确认 Coordinator 是否在运行"}
        except ConnectionRefusedError:
            return {"error": f"连接被拒绝 (127.0.0.1:{self.port})，请先启动系统"}
        except Exception as e:
            return {"error": str(e)}

    def get_node_info(self, node_id):
        return self._rpc(MSG_QUERY_NODE_INFO, {"node": int(node_id)})

    def get_neighbors(self, node_id):
        return self._rpc(MSG_QUERY_NEIGHBOR, {"node": int(node_id)})

    def get_common(self, a, b):
        return self._rpc(MSG_QUERY_COMMON, {"a": int(a), "b": int(b)})

    def get_triangles(self, node_id=None):
        if node_id is not None:
            return self._rpc(MSG_QUERY_TRIANGLE, {"node_id": int(node_id)})
        return self._rpc(MSG_QUERY_TRIANGLE, {})

    def get_stats(self):
        return self._rpc(MSG_QUERY_STATS, {})

    def search_nodes(self, keyword):
        """模糊搜索节点 — 遍历所有 Worker 查询"""
        stats = self.get_stats()
        # 从 stats 拿到 worker 信息后，逐个查询
        return stats


# ── 后台查询线程 ──
class QueryThread(QThread):
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, client, method, *args):
        super().__init__()
        self.client = client
        self.method = method
        self.args = args

    def run(self):
        try:
            fn = getattr(self.client, self.method)
            result = fn(*self.args)
            if "error" in result and result["error"]:
                self.error.emit(str(result["error"]))
            else:
                self.finished.emit(result)
        except Exception as e:
            self.error.emit(str(e))


# ── 状态监控线程 ──
class StatusPoller(QThread):
    status_updated = pyqtSignal(dict)

    def __init__(self, client, interval=3):
        super().__init__()
        self.client = client
        self.interval = interval
        self.running = True

    def run(self):
        while self.running:
            stats = self.client.get_stats()
            if "error" not in stats:
                self.status_updated.emit(stats)
            time.sleep(self.interval)

    def stop(self):
        self.running = False


# ── 样式表 ──
def make_stylesheet():
    return f"""
    QMainWindow {{ background: {COLOR_BG}; }}
    QWidget {{ background: transparent; color: {COLOR_TEXT}; font-family: {FONT_UI}; }}
    QGroupBox {{
        background: {COLOR_SURFACE};
        border: 1px solid {COLOR_BORDER};
        border-radius: 8px;
        margin-top: 16px;
        padding: 16px 12px 12px 12px;
        font-weight: bold;
    }}
    QGroupBox::title {{
        subcontrol-origin: margin;
        left: 12px;
        padding: 0 6px;
        color: {COLOR_PRIMARY_LIGHT};
    }}
    QLineEdit {{
        background: {COLOR_BG};
        border: 1px solid {COLOR_BORDER};
        border-radius: 6px;
        padding: 8px 12px;
        font-size: 14px;
        font-family: {FONT_MONO};
        color: {COLOR_TEXT};
    }}
    QLineEdit:focus {{
        border: 1px solid {COLOR_PRIMARY};
    }}
    QPushButton {{
        background: {COLOR_PRIMARY};
        color: white;
        border: none;
        border-radius: 6px;
        padding: 10px 20px;
        font-size: 14px;
        font-weight: bold;
        min-height: 20px;
    }}
    QPushButton:hover {{
        background: #6d28d9;
    }}
    QPushButton:pressed {{
        background: #5b21b6;
    }}
    QPushButton:disabled {{
        background: #4a4a6a;
        color: #8888aa;
    }}
    QPushButton#btn_info {{
        background: {COLOR_PRIMARY};
    }}
    QPushButton#btn_neighbor {{
        background: #0891b2;
    }}
    QPushButton#btn_neighbor:hover {{ background: #0e7490; }}
    QPushButton#btn_common {{
        background: #059669;
    }}
    QPushButton#btn_common:hover {{ background: #047857; }}
    QPushButton#btn_tri {{
        background: #d97706;
    }}
    QPushButton#btn_tri:hover {{ background: #b45309; }}
    QPushButton#btn_stats {{
        background: #7c3aed;
    }}
    QPushButton#btn_search {{
        background: #6366f1;
    }}
    QTextEdit {{
        background: {COLOR_BG};
        border: 1px solid {COLOR_BORDER};
        border-radius: 8px;
        padding: 12px;
        font-family: {FONT_MONO};
        font-size: 13px;
        color: {COLOR_TEXT};
        selection-background-color: {COLOR_PRIMARY};
    }}
    QStatusBar {{
        background: {COLOR_SURFACE};
        border-top: 1px solid {COLOR_BORDER};
        color: {COLOR_TEXT_DIM};
        font-size: 12px;
    }}
    QSplitter::handle {{
        background: {COLOR_BORDER};
        width: 2px;
    }}
    QLabel#title_bar {{
        font-size: 18px;
        font-weight: bold;
        color: {COLOR_PRIMARY_LIGHT};
        padding: 8px 0;
    }}
    QLabel#node_id_label {{
        font-size: 14px;
        color: {COLOR_TEXT_DIM};
        min-width: 70px;
    }}
    QLabel#result_title {{
        font-size: 16px;
        font-weight: bold;
        color: {COLOR_PRIMARY_LIGHT};
    }}
    """


# ── 主窗口 ──
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.client = DGraphClient(host="127.0.0.1", port=9000)
        self._poller = None
        self._query_threads = []
        self._init_ui()
        self._start_poller()

    def _init_ui(self):
        self.setWindowTitle("test6 · 分布式图查询系统")
        self.setMinimumSize(960, 680)
        self.resize(1100, 760)
        self.setStyleSheet(make_stylesheet())

        # ── 中央部件 ──
        central = QWidget()
        self.setCentralWidget(central)
        layout = QHBoxLayout(central)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(16)

        # ── 左侧面板 ──
        left = QWidget()
        left.setMaximumWidth(420)
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.setSpacing(12)

        # 标题
        title = QLabel("  test6  ·  图查询")
        title.setObjectName("title_bar")

        # ── 查询面板 ──
        query_group = QGroupBox("查询操作")
        qg = QVBoxLayout(query_group)
        qg.setSpacing(10)

        # 节点信息 / 邻居
        row1 = QHBoxLayout()
        self.inp_single = QLineEdit()
        self.inp_single.setPlaceholderText("节点 ID")
        self.btn_info = QPushButton("查看节点")
        self.btn_info.setObjectName("btn_info")
        self.btn_info.clicked.connect(lambda: self._query("get_node_info", self.inp_single.text()))
        row1.addWidget(self.inp_single)
        row1.addWidget(self.btn_info)

        row2 = QHBoxLayout()
        self.btn_neighbor = QPushButton("查询邻居")
        self.btn_neighbor.setObjectName("btn_neighbor")
        self.btn_neighbor.clicked.connect(lambda: self._query("get_neighbors", self.inp_single.text()))
        row2.addWidget(self.btn_neighbor)

        btn_row2 = QHBoxLayout()
        btn_row2.addWidget(self.btn_neighbor)
        btn_row2.addStretch()

        # 共同邻居
        row_common = QHBoxLayout()
        self.inp_a = QLineEdit()
        self.inp_a.setPlaceholderText("节点 A")
        self.inp_b = QLineEdit()
        self.inp_b.setPlaceholderText("节点 B")
        self.btn_common = QPushButton("共同邻居")
        self.btn_common.setObjectName("btn_common")
        self.btn_common.clicked.connect(lambda: self._query("get_common", self.inp_a.text(), self.inp_b.text()))
        row_common.addWidget(self.inp_a)
        row_common.addWidget(self.inp_b)
        row_common.addWidget(self.btn_common)

        # 三角计数 + 全图统计
        row3 = QHBoxLayout()
        self.inp_tri = QLineEdit()
        self.inp_tri.setPlaceholderText("节点 ID（留空=全图）")
        self.btn_tri = QPushButton("三角计数")
        self.btn_tri.setObjectName("btn_tri")
        self.btn_tri.clicked.connect(lambda: self._query("get_triangles",
                                                          self.inp_tri.text() if self.inp_tri.text().strip() else None))
        self.btn_stats = QPushButton("全图统计")
        self.btn_stats.setObjectName("btn_stats")
        self.btn_stats.clicked.connect(lambda: self._query("get_stats"))
        row3.addWidget(self.inp_tri)
        row3.addWidget(self.btn_tri)
        row3.addWidget(self.btn_stats)

        # 搜索
        row4 = QHBoxLayout()
        self.inp_search = QLineEdit()
        self.inp_search.setPlaceholderText("关键词搜索节点...")
        self.btn_search = QPushButton("搜索")
        self.btn_search.setObjectName("btn_search")
        self.btn_search.clicked.connect(self._on_search)
        row4.addWidget(self.inp_search)
        row4.addWidget(self.btn_search)

        qg.addLayout(row1)
        qg.addLayout(btn_row2)
        qg.addLayout(row_common)
        qg.addLayout(row3)
        qg.addLayout(row4)

        # ── 状态面板 ──
        status_group = QGroupBox("系统状态")
        sg = QVBoxLayout(status_group)
        sg.setSpacing(6)
        self.lbl_status = QLabel("等待连接...")
        self.lbl_status.setWordWrap(True)
        self.lbl_status.setStyleSheet(f"color: {COLOR_TEXT_DIM}; font-size: 13px;")
        sg.addWidget(self.lbl_status)
        sg.addStretch()

        left_layout.addWidget(title)
        left_layout.addWidget(query_group)
        left_layout.addWidget(status_group)
        left_layout.addStretch()

        # ── 右侧结果面板 ──
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(8)

        result_header = QHBoxLayout()
        self.lbl_result_title = QLabel("查询结果")
        self.lbl_result_title.setObjectName("result_title")
        self.btn_clear = QPushButton("清空")
        self.btn_clear.setFixedWidth(80)
        self.btn_clear.setStyleSheet(f"""
            QPushButton {{
                background: {COLOR_SURFACE};
                border: 1px solid {COLOR_BORDER};
                color: {COLOR_TEXT_DIM};
                padding: 6px 12px;
                font-size: 12px;
            }}
            QPushButton:hover {{
                background: #3b3b5c;
                color: {COLOR_TEXT};
            }}
        """)
        self.btn_clear.clicked.connect(self._clear_result)
        result_header.addWidget(self.lbl_result_title)
        result_header.addStretch()
        result_header.addWidget(self.btn_clear)

        self.result_area = QTextEdit()
        self.result_area.setReadOnly(True)
        self.result_area.setMinimumWidth(500)

        right_layout.addLayout(result_header)
        right_layout.addWidget(self.result_area)

        # ── 分割器 ──
        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([380, 620])
        splitter.setHandleWidth(2)

        layout.addWidget(splitter)

        # 状态栏
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.lbl_conn = QLabel("未连接")
        self.status_bar.addPermanentWidget(self.lbl_conn)

        # 回车键绑定
        self.inp_single.returnPressed.connect(self.btn_info.click)
        self.inp_a.returnPressed.connect(lambda: self.inp_b.setFocus())
        self.inp_b.returnPressed.connect(self.btn_common.click)
        self.inp_tri.returnPressed.connect(self.btn_tri.click)
        self.inp_search.returnPressed.connect(self.btn_search.click)

    def _start_poller(self):
        if self._poller:
            self._poller.stop()
        self._poller = StatusPoller(self.client)
        self._poller.status_updated.connect(self._on_status)
        self._poller.start()

    def _on_status(self, stats):
        nodes = stats.get("num_nodes", "?")
        edges = stats.get("num_edges", "?")
        tri = stats.get("num_triangles", "?")
        workers = stats.get("num_workers", "?")
        self.lbl_status.setText(
            f"节点数: {nodes}  |  分区: {workers}  |  全图三角: {tri}"
        )
        self.lbl_conn.setText(f"在线  {workers} Worker  |  {nodes} 节点  |  {tri} 三角")
        self.lbl_conn.setStyleSheet(f"color: {COLOR_SUCCESS};")

    def _query(self, method, *args):
        """在后台线程中执行查询"""
        def _on_result(data):
            self._display_result(method, args, data)
            self._enable_buttons(True)

        def _on_error(msg):
            self._append_error(f"[错误] {msg}")
            self._enable_buttons(True)

        self._enable_buttons(False)
        thread = QueryThread(self.client, method, *args)
        thread.finished.connect(_on_result)
        thread.error.connect(_on_error)
        thread.start()
        self._query_threads.append(thread)

    def _display_result(self, method, args, data):
        if "error" in data and data["error"]:
            self._append_error(f"[错误] {data['error']}")
            return

        lines = []
        if method == "get_node_info":
            lines = self._format_node_info(data)
        elif method == "get_neighbors":
            lines = self._format_neighbors(data)
        elif method == "get_common":
            lines = self._format_common(data)
        elif method == "get_triangles":
            lines = self._format_triangles(data)
        elif method == "get_stats":
            lines = self._format_stats(data)
        else:
            lines = [json.dumps(data, indent=2, ensure_ascii=False)]

        text = "\n".join(lines)
        self.result_area.append(text)
        self.result_area.append("")
        # 滚动到底部
        cursor = self.result_area.textCursor()
        cursor.movePosition(QTextCursor.End)
        self.result_area.setTextCursor(cursor)

    def _format_node_info(self, data):
        lines = []
        node = data.get("node", "?")
        attrs = data.get("attrs", {})
        degree = data.get("degree", 0)
        nbrs = data.get("neighbors", [])

        label = attrs.get("label", f"节点 {node}")
        group = attrs.get("group", "?")
        lines.append(f"  {chr(9679)} 节点 {node}")
        lines.append(f"  {'  '} 标签: {label}")
        lines.append(f"  {'  '} 分组: {group}")
        lines.append(f"  {'  '} 度数: {degree}")
        if nbrs:
            lines.append(f"  {'  '} 邻居 ({len(nbrs)} 个):")
            # 每行 12 个
            for i in range(0, len(nbrs), 12):
                chunk = nbrs[i:i+12]
                lines.append(f"  {'  '}   {'  '.join(str(n) for n in chunk)}")
        return lines

    def _format_neighbors(self, data):
        lines = []
        node = data.get("node", "?")
        degree = data.get("degree", 0)
        nbrs = data.get("neighbors", [])
        lines.append(f"  {chr(9679)} 节点 {node} 的邻居  (度数: {degree})")
        if nbrs:
            for i in range(0, len(nbrs), 12):
                chunk = nbrs[i:i+12]
                lines.append(f"  {'  '}{'  '.join(str(n) for n in chunk)}")
        else:
            lines.append(f"  {'  '}(无邻居)")
        return lines

    def _format_common(self, data):
        lines = []
        a = data.get("a", data.get("node_a", "?"))
        b = data.get("b", data.get("node_b", "?"))
        common = data.get("common_neighbors", data.get("neighbors", []))
        count = data.get("count", len(common))
        lines.append(f"  {chr(9679)} 节点 {a} 和 {b} 的共同邻居  ({count} 个)")
        if common:
            for i in range(0, len(common), 12):
                chunk = common[i:i+12]
                lines.append(f"  {'  '}{'  '.join(str(n) for n in chunk)}")
        else:
            lines.append(f"  {'  '}(无共同邻居)")
        return lines

    def _format_triangles(self, data):
        lines = []
        if "node" in data:
            node = data["node"]
            count = data.get("count", 0)
            lines.append(f"  {chr(9679)} 节点 {node} 的三角形数: {count}")
        else:
            count = data.get("count", 0)
            lines.append(f"  {chr(9679)} 全图三角计数")
            lines.append(f"  {'  '}总计: {count} 个三角形")
        return lines

    def _format_stats(self, data):
        lines = []
        lines.append(f"  {chr(9679)} 全图统计")
        lines.append(f"  {'  '}节点数: {data.get('num_nodes', '?')}")
        lines.append(f"  {'  '}分区数: {data.get('num_parts', '?')}")
        lines.append(f"  {'  '}Worker数: {data.get('num_workers', '?')}")
        lines.append(f"  {'  '}三角计数: {data.get('num_triangles', '?')}")
        lines.append(f"  {'  '}状态: {data.get('status', '?')}")
        return lines

    def _append_error(self, msg):
        self.result_area.append(f"\n  [X] {msg}\n")

    def _clear_result(self):
        self.result_area.clear()
        self.lbl_result_title.setText("查询结果")

    def _enable_buttons(self, enabled):
        for btn in [self.btn_info, self.btn_neighbor, self.btn_common,
                     self.btn_tri, self.btn_stats, self.btn_search]:
            btn.setEnabled(enabled)

    def _on_search(self):
        keyword = self.inp_search.text().strip()
        if not keyword:
            return
        stats = self.client.get_stats()
        # 搜索只是全图统计的展示，更细粒度的搜索需要遍历所有 worker
        lines = [f"  搜索关键词: '{keyword}'"]
        for wid in range(5):
            lines.append(f"  Worker w{wid}: 需要遍历分区数据（高级功能）")
        lines.append(f"  (提示: 使用 '查看节点' 直接输入已知 ID 查询)")
        self.result_area.append("\n".join(lines))
        self.result_area.append("")

    def closeEvent(self, event):
        if self._poller:
            self._poller.stop()
        event.accept()


# ── 启动 ──
def main():
    import argparse
    ap = argparse.ArgumentParser(description="test6 PyQt5 图查询 GUI")
    ap.add_argument("--coord-host", default="127.0.0.1")
    ap.add_argument("--coord-port", type=int, default=9000)
    args = ap.parse_args()

    app = QApplication(sys.argv)
    app.setStyle("Fusion")

    # 暗色主题 palette
    palette = QPalette()
    palette.setColor(QPalette.Window, QColor(COLOR_BG))
    palette.setColor(QPalette.Base, QColor(COLOR_SURFACE))
    palette.setColor(QPalette.Text, QColor(COLOR_TEXT))
    palette.setColor(QPalette.WindowText, QColor(COLOR_TEXT))
    palette.setColor(QPalette.ButtonText, QColor(COLOR_TEXT))
    palette.setColor(QPalette.Highlight, QColor(COLOR_PRIMARY))
    palette.setColor(QPalette.ToolTipBase, QColor(COLOR_SURFACE))
    palette.setColor(QPalette.ToolTipText, QColor(COLOR_TEXT))
    app.setPalette(palette)

    w = MainWindow()
    w.client.host = args.coord_host
    w.client.port = args.coord_port
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
