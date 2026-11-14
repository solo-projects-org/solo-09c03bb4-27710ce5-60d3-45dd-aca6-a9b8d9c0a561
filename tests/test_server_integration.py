"""真实 uvicorn 进程集成测试。

TestClient 的 BlockingPortal 各自持有独立事件循环，无法模拟单进程 uvicorn
单事件循环下「取消协程等待上传协程停止」的并发语义，因此这里拉起真实服务器，
用标准库 urllib + 线程驱动，验证端到端行为。
"""

from __future__ import annotations

import gzip
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server():
    data_dir = Path(tempfile.mkdtemp(prefix="fastq-itg-")) / "data"
    port = _free_port()
    env = {
        **os.environ,
        "FASTQ_DATA_DIR": str(data_dir),
        "FASTQ_BATCH_BYTE_LIMIT": str(64 * 1024 * 1024),
        "PYTHONPATH": str(REPO_ROOT),
    }
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=1) as r:
                if r.status == 200:
                    break
        except OSError:
            time.sleep(0.2)
    else:
        proc.terminate()
        out = proc.stdout.read() if proc.stdout else ""
        pytest.fail(f"server did not start:\n{out}")

    yield {"base": base, "data_dir": data_dir, "proc": proc}

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def _req(server, method: str, path: str, body=None, headers=None):
    req = urllib.request.Request(
        server["base"] + path,
        data=body,
        headers=headers or {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _make_gz(records) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
        for name, seq, qual in records:
            fh.write(b"@" + name + b"\n" + seq + b"\n+\n" + qual + b"\n")
    return buf.getvalue()


def _pair(n: int, length: int = 50):
    r1, r2 = [], []
    for i in range(n):
        seq = bytes(65 + (i * 3 + j) % 26 for j in range(length))
        qual = b"J" * length
        base = f"itg-{i}".encode()
        r1.append((base + b"/1", seq, qual))
        r2.append((base + b"/2", seq, qual))
    return r1, r2


def test_full_lifecycle_normal_mismatch_oversized(server):
    # 正常批
    r1, r2 = _pair(5)
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-ok"}).encode(),
                   {"Content-Type": "application/json"})
    bid = body["id"]
    assert _req(server, "POST", f"/batches/{bid}/files/R1", _make_gz(r1),
                {"Content-Type": "application/gzip"})[0] == 200
    status, body = _req(server, "POST", f"/batches/{bid}/files/R2", _make_gz(r2),
                        {"Content-Type": "application/gzip"})
    assert status == 200 and body["status"] == "ready"
    with urllib.request.urlopen(server["base"] + f"/batches/{bid}/files/R1/download") as dl:
        assert dl.read().startswith(b"@itg-0/1\n")

    # 错配批
    r1b, r2b = _pair(3)
    r2b[1] = (b"itg-WRONG/2", r2b[1][1], r2b[1][2])
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-bad"}).encode(),
                   {"Content-Type": "application/json"})
    bad = body["id"]
    _req(server, "POST", f"/batches/{bad}/files/R1", _make_gz(r1b),
         {"Content-Type": "application/gzip"})
    code, body = _req(server, "POST", f"/batches/{bad}/files/R2", _make_gz(r2b),
                      {"Content-Type": "application/gzip"})
    assert code == 422 and "mismatch" in body["detail"]
    _, detail = _req(server, "GET", f"/batches/{bad}")
    assert detail["status"] == "failed"
    assert list((server["data_dir"] / "batches" / bad).iterdir()) == []

    # 超限批：序列映射为 4 种碱基但条数足够多，展开后约 40 万 * 328 ≈ 128 MiB
    big = []
    for i in range(400_000):
        seq = os.urandom(80)
        big.append((f"big-{i}/1".encode(), bytes(65 + b % 4 for b in seq), b"J" * 80))
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-big"}).encode(),
                   {"Content-Type": "application/json"})
    big_id = body["id"]
    code, body = _req(server, "POST", f"/batches/{big_id}/files/R1", _make_gz(big),
                      {"Content-Type": "application/gzip"})
    assert code == 413
    assert list((server["data_dir"] / "batches" / big_id).iterdir()) == []


def test_trim_lifecycle_and_failure_retention(server):
    """真实服务器上的裁剪全流程：放行→裁剪→下载→失败保留旧包→重算成功。"""
    # 8 对记录：尾部 6 个碱基低质量（'!'=Q0），主体高质量（'J'=Q41）
    r1, r2 = [], []
    for i in range(8):
        seq = bytes(65 + (i * 5 + j) % 26 for j in range(40))
        qual = b"J" * 34 + b"!" * 6
        r1.append((f"trim-itg-{i}/1".encode(), seq, qual))
        r2.append((f"trim-itg-{i}/2".encode(), seq, qual))

    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-trim"}).encode(),
                   {"Content-Type": "application/json"})
    bid = body["id"]
    assert _req(server, "POST", f"/batches/{bid}/files/R1", _make_gz(r1),
                {"Content-Type": "application/gzip"})[0] == 200
    code, body = _req(server, "POST", f"/batches/{bid}/files/R2", _make_gz(r2),
                      {"Content-Type": "application/gzip"})
    assert code == 200 and body["status"] == "ready"

    with urllib.request.urlopen(server["base"] + f"/batches/{bid}/files/R1/download") as dl:
        original_r1 = dl.read()

    def post_trim(min_quality, min_length):
        return _req(server, "POST", f"/batches/{bid}/trim",
                    json.dumps({"min_quality": min_quality, "min_length": min_length}).encode(),
                    {"Content-Type": "application/json"})

    def wait_done():
        for _ in range(100):
            _, st = _req(server, "GET", f"/batches/{bid}/trim")
            if st["job"] and st["job"]["status"] != "running":
                return st
            time.sleep(0.1)
        raise AssertionError("trim job did not finish")

    def download_trimmed(role="R1"):
        with urllib.request.urlopen(
            server["base"] + f"/batches/{bid}/trim/files/{role}/download"
        ) as dl:
            return dl.read()

    # 1) 正常裁剪：尾部 6 个低质量碱基被裁去
    code, body = post_trim(30, 20)
    assert code == 202 and body["job"]["status"] == "running"
    st = wait_done()
    assert st["job"]["status"] == "published" and st["job"]["pairs_kept"] == 8
    trimmed = download_trimmed()
    first_rec = trimmed.split(b"\n")[:4]
    assert first_rec[0] == b"@trim-itg-0/1"
    assert len(first_rec[1]) == 34 and set(first_rec[3]) == {ord("J")}
    # 原始文件与批次状态不变
    with urllib.request.urlopen(server["base"] + f"/batches/{bid}/files/R1/download") as dl:
        assert dl.read() == original_r1
    _, detail = _req(server, "GET", f"/batches/{bid}")
    assert detail["status"] == "ready"

    # 2) 失败保留：最短读长超过原读长 → 无读对存活 → 任务失败，旧包仍可下载
    code, _ = post_trim(30, 100)
    assert code == 202
    st = wait_done()
    assert st["job"]["status"] == "failed" and "no read pairs survived" in st["job"]["error"]
    assert download_trimmed() == trimmed
    # 中间文件已清理，只留已发布包
    trim_dir = server["data_dir"] / "batches" / bid / "trim"
    survivors = [p.name for p in trim_dir.iterdir()]
    assert survivors == [f"pkg-{st['package']['job_id']}"]

    # 3) 调整阈值重算成功，新包替换旧包
    code, _ = post_trim(0, 20)
    assert code == 202
    st = wait_done()
    assert st["job"]["status"] == "published"
    republished = download_trimmed()
    assert len(republished) > len(trimmed)  # 阈值为 0 时低质量尾保留，读长恢复 40
    survivors = [p.name for p in trim_dir.iterdir()]
    assert survivors == [f"pkg-{st['package']['job_id']}"]


def test_slow_upload_does_not_block_other_batch(server):
    r1, r2 = _pair(4)
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "slow"}).encode(),
                   {"Content-Type": "application/json"})
    slow_id = body["id"]
    blob = _make_gz(r1)

    def slow_upload():
        def gen():
            for i in range(0, len(blob), 32):
                time.sleep(0.1)
                yield blob[i : i + 32]

        req = urllib.request.Request(
            server["base"] + f"/batches/{slow_id}/files/R1",
            data=gen(),
            headers={"Content-Type": "application/gzip"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status

    import threading

    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.3)

    # 慢上传进行中，另一个批次完整走通
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "fast"}).encode(),
                   {"Content-Type": "application/json"})
    fast_id = body["id"]
    f1, f2 = _pair(4)
    _req(server, "POST", f"/batches/{fast_id}/files/R1", _make_gz(f1),
         {"Content-Type": "application/gzip"})
    code, body = _req(server, "POST", f"/batches/{fast_id}/files/R2", _make_gz(f2),
                      {"Content-Type": "application/gzip"})
    assert code == 200 and body["status"] == "ready"
    t.join(timeout=30)


def test_cancel_during_slow_upload_waits_and_cleans(server):
    # 单成员、不可压缩的大 gzip：随机质量串让压缩后仍有足够体积，慢速传输确保取消时在途
    records = []
    for i in range(2000):
        seq = bytes(65 + b % 4 for b in os.urandom(120))
        qual = bytes(33 + b % 60 for b in os.urandom(120))
        records.append((f"cancel-{i}/1".encode(), seq, qual))
    blob = _make_gz(records)
    assert len(blob) > 100_000

    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "cancel"}).encode(),
                   {"Content-Type": "application/json"})
    bid = body["id"]
    bdir = server["data_dir"] / "batches" / bid
    result = {}

    def slow_upload():
        def gen():
            for i in range(0, len(blob), 64):
                time.sleep(0.02)
                yield blob[i : i + 64]

        req = urllib.request.Request(
            server["base"] + f"/batches/{bid}/files/R1",
            data=gen(),
            headers={"Content-Type": "application/gzip"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                result["status"] = r.status
        except urllib.error.HTTPError as e:
            result["status"] = e.code
        except urllib.error.URLError:
            # 取消后服务端关闭连接，发送端收到 broken pipe / connection reset，属预期
            result["status"] = "connection-closed"

    import threading

    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.4)
    # 此时已有落盘
    assert any(bdir.iterdir())

    code, body = _req(server, "POST", f"/batches/{bid}/cancel")
    assert code == 200 and body["status"] == "cancelled", body

    # 取消确认时：写入已停止、文件已删、空间已释放
    assert list(bdir.iterdir()) == []
    t.join(timeout=30)
    assert not t.is_alive()
    assert result["status"] in (409, 422, "connection-closed")
    # 上传彻底结束后仍无任何文件落盘
    assert list(bdir.iterdir()) == []

    # 取消后不能再上传
    code, _ = _req(server, "POST", f"/batches/{bid}/files/R1", blob,
                   {"Content-Type": "application/gzip"})
    assert code == 409
    assert list(bdir.iterdir()) == []
