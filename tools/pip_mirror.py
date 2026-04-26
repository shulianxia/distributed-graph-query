#!/usr/bin/env python3
"""
pip 镜像源切换工具 — 安装失败时自动切换镜像源重试

功能：
  1. pip_install(package) — 自动尝试所有镜像源安装
  2. list_mirrors() — 列出可用镜像源
  3. set_default_mirror(url) — 设置默认镜像源
  4. test_speed() — 测试各镜像源延迟

用法：
  python3 pip_mirror.py install numpy
  python3 pip_mirror.py list
  python3 pip_mirror.py speed
  python3 pip_mirror.py set https://pypi.tuna.tsinghua.edu.cn/simple

也可以在 Python 中作为模块导入使用。
"""

import subprocess
import sys
import time
import urllib.request
import urllib.error

# ============================================================
# 可用镜像源列表（中国大陆可用）
# ============================================================

DEFAULT_MIRRORS = [
    ("清华大学", "https://pypi.tuna.tsinghua.edu.cn/simple"),
    ("阿里云",    "https://mirrors.aliyun.com/pypi/simple"),
    ("中科大",    "https://pypi.mirrors.ustc.edu.cn/simple"),
    ("豆瓣",      "https://pypi.douban.com/simple"),
    ("华为云",    "https://repo.huaweicloud.com/repository/pypi/simple"),
    ("腾讯云",    "https://mirrors.cloud.tencent.com/pypi/simple"),
    ("网易",      "https://mirrors.163.com/pypi/simple"),
    ("PyPI 官方", "https://pypi.org/simple"),
]

# 当前选中的镜像（默认尝试全部）
_custom_mirror = None


def list_mirrors():
    """列出所有可用镜像源"""
    print(f"\n{'=' * 60}")
    print(f"  pip 镜像源列表")
    print(f"{'=' * 60}")
    for i, (name, url) in enumerate(DEFAULT_MIRRORS, 1):
        print(f"  {i:2d}. {name:<8s}  {url}")
    print(f"{'=' * 60}")
    return DEFAULT_MIRRORS


def test_speed(mirrors=None, timeout=5):
    """测试各镜像源的连接延迟（秒）"""
    if mirrors is None:
        mirrors = DEFAULT_MIRRORS

    print(f"\n{'=' * 60}")
    print(f"  测试镜像源延迟（超时 {timeout}s）")
    print(f"{'=' * 60}")

    results = []
    for name, url in mirrors:
        test_url = url.rstrip("/") + "/"
        start = time.time()
        try:
            req = urllib.request.Request(test_url, method="HEAD")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                elapsed = time.time() - start
                status = resp.status
                results.append((elapsed, name, url, status))
                print(f"  {name:<8s}  {elapsed:>6.3f}s  HTTP {status}   ✅")
        except Exception as e:
            elapsed = time.time() - start
            results.append((float("inf"), name, url, str(e)))
            print(f"  {name:<8s}  {elapsed:>6.3f}s  ❌ {e}")

    print(f"{'=' * 60}")
    fastest = sorted([r for r in results if r[0] != float("inf")], key=lambda x: x[0])
    if fastest:
        print(f"  最快: {fastest[0][1]} ({fastest[0][0]:.3f}s)")
    print(f"{'=' * 60}")
    return results


def set_default_mirror(url):
    """设置默认镜像源（全局变量）"""
    global _custom_mirror
    _custom_mirror = url
    print(f"  默认镜像源已设为: {url}")


def pip_install(package, mirrors=None, upgrade=False, quiet=False):
    """
    使用多个镜像源尝试安装 pip 包。

    参数：
      package : str  — 包名（支持 ==version, >=version 等 pip 格式）
      mirrors : list — 镜像源列表，None 表示全部尝试
      upgrade : bool — 是否 --upgrade
      quiet   : bool — 是否静默模式

    返回：
      (success: bool, mirror_used: str or None)
    """
    if mirrors is None:
        mirrors = DEFAULT_MIRRORS

    # 如果设置了默认镜像，优先只用它
    if _custom_mirror:
        mirrors = [("自定义", _custom_mirror)]

    cmd = [sys.executable, "-m", "pip", "install"]
    if upgrade:
        cmd.append("--upgrade")
    if quiet:
        cmd.append("-q")
    cmd.append(package)

    print(f"\n{'=' * 60}")
    print(f"  pip install {package}")
    print(f"{'=' * 60}")

    for name, url in mirrors:
        full_cmd = cmd + ["-i", url, "--trusted-host", _extract_host(url)]
        print(f"  [{name}] 尝试 {url} ...")
        start = time.time()
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
            elapsed = time.time() - start
            if result.returncode == 0:
                print(f"  ✅ [{name}] 安装成功! ({elapsed:.2f}s)")
                if not quiet:
                    print(result.stdout[-500:] if len(result.stdout) > 500 else result.stdout)
                return True, url
            else:
                # 检查是否是网络错误还是包不存在
                stderr_lower = result.stderr.lower()
                if "no matching distribution" in stderr_lower or "could not find" in stderr_lower:
                    print(f"  ❌ 包 '{package}' 在 {name} 上不存在，跳过其他镜像")
                    print(f"     {result.stderr.strip()[-200:]}")
                    return False, None
                print(f"  ❌ [{name}] 失败 ({elapsed:.2f}s)")
                if not quiet:
                    print(f"     {result.stderr.strip()[-200:]}")
        except subprocess.TimeoutExpired:
            print(f"  ❌ [{name}] 超时 (120s)")
        except Exception as e:
            print(f"  ❌ [{name}] 异常: {e}")

    print(f"\n  ❌ 所有镜像源均安装失败: {package}")
    return False, None


def _extract_host(url):
    """从 URL 中提取 host，用于 --trusted-host"""
    from urllib.parse import urlparse
    return urlparse(url).hostname


def install_requirements(requirements_file, mirrors=None, upgrade=False):
    """
    批量安装 requirements.txt 中的包，自动切换镜像源。

    参数：
      requirements_file : str — requirements.txt 路径
      mirrors           : list — 镜像源列表
      upgrade           : bool — 是否 --upgrade
    """
    with open(requirements_file, "r", encoding="utf-8") as f:
        packages = [line.strip() for line in f
                    if line.strip() and not line.startswith("#") and not line.startswith("-")]

    print(f"\n{'=' * 60}")
    print(f"  批量安装 {len(packages)} 个包 (requirements: {requirements_file})")
    print(f"{'=' * 60}")

    success_count = 0
    fail_count = 0
    for pkg in packages:
        ok, _ = pip_install(pkg, mirrors=mirrors, upgrade=upgrade, quiet=True)
        if ok:
            success_count += 1
        else:
            fail_count += 1

    print(f"\n{'=' * 60}")
    print(f"  批量安装完成: ✅ {success_count} 成功, ❌ {fail_count} 失败")
    print(f"{'=' * 60}")
    return success_count, fail_count


# ============================================================
# CLI 入口
# ============================================================

def print_usage():
    print("""用法:
  python3 pip_mirror.py install <包名>       安装包（自动切换镜像源）
  python3 pip_mirror.py install -r <文件>    批量安装 requirements.txt
  python3 pip_mirror.py list                 列出可用镜像源
  python3 pip_mirror.py speed                测试镜像源延迟
  python3 pip_mirror.py set <URL>            设置默认镜像源""")
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print_usage()

    command = sys.argv[1]

    if command == "list":
        list_mirrors()

    elif command == "speed":
        test_speed()

    elif command == "set":
        if len(sys.argv) < 3:
            print("用法: python3 pip_mirror.py set <镜像源URL>")
            sys.exit(1)
        set_default_mirror(sys.argv[2])

    elif command == "install":
        if len(sys.argv) < 3:
            print("用法: python3 pip_mirror.py install <包名>")
            sys.exit(1)
        if sys.argv[2] == "-r":
            if len(sys.argv) < 4:
                print("用法: python3 pip_mirror.py install -r requirements.txt")
                sys.exit(1)
            install_requirements(sys.argv[3])
        else:
            package = " ".join(sys.argv[2:])
            pip_install(package)

    else:
        print_usage()
