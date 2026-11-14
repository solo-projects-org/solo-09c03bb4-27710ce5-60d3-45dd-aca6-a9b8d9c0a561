"""质量裁剪 API 测试：裁剪、重算、并发互斥、失败保留旧包、重启清理。"""

from __future__ import annotations

import asyncio
import threading
import time

from app.config import Settings
from app.database import get_conn
from app.service import BatchService
from app.storage import directory_size
from tests.helpers import create_batch as _create_batch
from tests.helpers import gz_bytes as _gz
from tests.helpers import upload_bytes as _upload_bytes

HIGH = b"J"  # Phred+33 Q41
LOW = b"!"   # Phred+33 Q0


def tail_pair(n: int = 4, length: int = 12, tail: int = 4):
    """n 对记录：前段高质量、尾部 tail 个碱基低质量。"""
    r1, r2 = [], []
    for i in range(n):
        seq = bytes(65 + (i + j) % 26 for j in range(length))
        qual = HIGH * (length - tail) + LOW * tail
        base = f"trim-{i}".encode()
        r1.append((base + b"/1", seq, qual))
        r2.append((base + b"/2", seq, qual))
    return r1, r2


def make_ready_batch(client, r1=None, r2=None, sample_id: str = "trim-sample") -> str:
    if r1 is None or r2 is None:
        r1, r2 = tail_pair()
    bid = _create_batch(client, sample_id)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    resp = _upload_bytes(client, bid, "R2", _gz(r2))
    assert resp.status_code == 200 and resp.json()["status"] == "ready", resp.text
    return bid


def start_trim(client, bid: str, min_quality: int = 30, min_length: int = 8):
    return client.post(
        f"/batches/{bid}/trim", json={"min_quality": min_quality, "min_length": min_length}
    )


def wait_job(client, bid: str, timeout: float = 10.0) -> dict:
    """轮询直到最近一次裁剪任务离开 running 状态，返回整个 trim 状态体。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = client.get(f"/batches/{bid}/trim").json()
        job = st["job"]
        if job is not None and job["status"] != "running":
            return st
        time.sleep(0.02)
    raise AssertionError("trim job did not finish in time")


def parse_fastq(content: bytes) -> list[tuple[bytes, bytes, bytes]]:
    lines = content.split(b"\n")
    assert lines[-1] == b""
    lines = lines[:-1]
    assert len(lines) % 4 == 0
    return [
        (lines[i], lines[i + 1], lines[i + 3]) for i in range(0, len(lines), 4)
    ]


# --------------------------------------------------------------------- #
# 基本流程
# --------------------------------------------------------------------- #


def test_trim_happy_path_trims_tails_and_keeps_originals(client, settings):
    r1, r2 = tail_pair(n=4, length=12, tail=4)
    bid = make_ready_batch(client, r1, r2)
    original_r1 = client.get(f"/batches/{bid}/files/R1/download").content

    resp = start_trim(client, bid, min_quality=30, min_length=8)
    assert resp.status_code == 202, resp.text
    assert resp.json()["job"]["status"] == "running"

    st = wait_job(client, bid)
    job, pkg = st["job"], st["package"]
    assert job["status"] == "published", job
    assert job["pairs_in"] == 4 and job["pairs_kept"] == 4
    assert pkg is not None and pkg["min_quality"] == 30 and pkg["min_length"] == 8
    assert pkg["pairs_kept"] == 4 and pkg["size"] > 0

    # 裁剪结果：尾部 4 个低质量碱基被裁去，读段名保留
    trimmed1 = parse_fastq(client.get(f"/batches/{bid}/trim/files/R1/download").content)
    trimmed2 = parse_fastq(client.get(f"/batches/{bid}/trim/files/R2/download").content)
    assert len(trimmed1) == len(trimmed2) == 4
    for i, ((h1, s1, q1), (h2, s2, q2)) in enumerate(zip(trimmed1, trimmed2)):
        assert h1 == f"@trim-{i}/1".encode() and h2 == f"@trim-{i}/2".encode()
        assert len(s1) == len(q1) == len(s2) == len(q2) == 8
        assert set(q1) == set(HIGH) and set(q2) == set(HIGH)

    # 原始文件与批次状态不变
    assert client.get(f"/batches/{bid}/files/R1/download").content == original_r1
    detail = client.get(f"/batches/{bid}").json()
    assert detail["status"] == "ready"
    # 结果包与原文件共用批次字节上限：bytes_used 含原文件 + 结果包
    assert detail["bytes_used"] == directory_size(settings.batch_dir / bid)


def test_trim_drops_pair_when_either_end_too_short(client):
    # 第 0 对 R1 尾部 9 个低质量碱基：裁后只剩 3 < min_length=8 → 整对舍弃
    r1, r2 = tail_pair(n=3, length=12, tail=4)
    name, seq, _ = r1[0]
    r1[0] = (name, seq, HIGH * 3 + LOW * 9)
    bid = make_ready_batch(client, r1, r2)

    assert start_trim(client, bid, min_quality=30, min_length=8).status_code == 202
    st = wait_job(client, bid)
    assert st["job"]["status"] == "published"
    assert st["job"]["pairs_in"] == 3 and st["job"]["pairs_kept"] == 2

    trimmed1 = parse_fastq(client.get(f"/batches/{bid}/trim/files/R1/download").content)
    trimmed2 = parse_fastq(client.get(f"/batches/{bid}/trim/files/R2/download").content)
    # 被舍弃的整对两端都不出现，保留对仍逐对对应
    assert [h for h, _, _ in trimmed1] == [b"@trim-1/1", b"@trim-2/1"]
    assert [h for h, _, _ in trimmed2] == [b"@trim-1/2", b"@trim-2/2"]


def test_trim_recompute_replaces_package(client, settings):
    bid = make_ready_batch(client)
    assert start_trim(client, bid, min_quality=30, min_length=8).status_code == 202
    st1 = wait_job(client, bid)
    pkg1 = st1["package"]
    first = client.get(f"/batches/{bid}/trim/files/R1/download").content

    # 调整阈值重算：阈值为 0 时低质量尾也保留，新包完整发布后替换旧包
    assert start_trim(client, bid, min_quality=0, min_length=5).status_code == 202
    st2 = wait_job(client, bid)
    pkg2 = st2["package"]
    assert st2["job"]["status"] == "published"
    assert pkg2["job_id"] != pkg1["job_id"]
    assert pkg2["min_length"] == 5
    second = client.get(f"/batches/{bid}/trim/files/R1/download").content
    assert second != first
    # 旧包目录已删除，盘上只剩新结果包
    trim_dir = settings.batch_dir / bid / "trim"
    assert [p.name for p in trim_dir.iterdir()] == [f"pkg-{pkg2['job_id']}"]


def test_trim_requires_ready_batch(client):
    bid = _create_batch(client, "trim-not-ready")
    assert start_trim(client, bid).status_code == 409
    assert client.get(f"/batches/{bid}/trim").status_code == 200
    body = client.get(f"/batches/{bid}/trim").json()
    assert body["job"] is None and body["package"] is None
    assert client.get(f"/batches/{bid}/trim/files/R1/download").status_code == 409

    assert start_trim(client, "no-such-batch").status_code == 404
    assert client.get("/batches/no-such-batch/trim").status_code == 404


def test_trim_request_validation(client):
    bid = make_ready_batch(client)
    assert client.post(f"/batches/{bid}/trim", json={"min_quality": -1, "min_length": 8}).status_code == 422
    assert client.post(f"/batches/{bid}/trim", json={"min_quality": 30, "min_length": 0}).status_code == 422
    assert client.post(f"/batches/{bid}/trim", json={"min_quality": 30}).status_code == 422


def test_trim_concurrent_conflict(client, monkeypatch):
    import app.service as service_mod
    from app import trimming

    orig = trimming.trim_fastq_pair
    started = threading.Event()
    release = threading.Event()

    async def slow_trim(**kwargs):
        started.set()
        await asyncio.to_thread(release.wait, 10)
        return await orig(**kwargs)

    monkeypatch.setattr(service_mod, "trim_fastq_pair", slow_trim)
    bid = make_ready_batch(client)

    assert start_trim(client, bid).status_code == 202
    assert started.wait(timeout=5)
    # 同批第二项裁剪在途时被拒
    resp = start_trim(client, bid)
    assert resp.status_code == 409
    assert "already running" in resp.json()["detail"]

    release.set()
    st = wait_job(client, bid)
    assert st["job"]["status"] == "published"


# --------------------------------------------------------------------- #
# 失败语义：超限 / 无读对存活 → 保留旧包、清理中间文件
# --------------------------------------------------------------------- #


def test_trim_quota_exceeded_keeps_old_package_and_other_batches(client, settings):
    from app.main import service

    bid = make_ready_batch(client)
    assert start_trim(client, bid).status_code == 202
    st = wait_job(client, bid)
    assert st["job"]["status"] == "published"
    old_pkg_dir = settings.batch_dir / bid / "trim" / f"pkg-{st['package']['job_id']}"
    assert old_pkg_dir.is_dir()
    old_content = client.get(f"/batches/{bid}/trim/files/R1/download").content

    # 把上限压到当前占用：任何新写入都超限
    original_limit = service.space._limit
    service.space._limit = directory_size(settings.batch_dir / bid)
    try:
        assert start_trim(client, bid).status_code == 202
        st = wait_job(client, bid)
        assert st["job"]["status"] == "failed"
        assert "quota" in st["job"]["error"].lower()

        # 旧包仍可下载且内容不变；staging 已清理，只留旧包目录
        assert client.get(f"/batches/{bid}/trim/files/R1/download").content == old_content
        assert client.get(f"/batches/{bid}").json()["status"] == "ready"
        trim_dir = settings.batch_dir / bid / "trim"
        assert [p.name for p in trim_dir.iterdir()] == [old_pkg_dir.name]

        # 其他批次在同一上限下仍能正常接收（配额按批记账）
        r1, r2 = tail_pair(n=2, length=12, tail=4)
        other = _create_batch(client, "trim-other-batch")
        assert _upload_bytes(client, other, "R1", _gz(r1)).status_code == 200
        resp = _upload_bytes(client, other, "R2", _gz(r2))
        assert resp.status_code == 200 and resp.json()["status"] == "ready"
    finally:
        service.space._limit = original_limit

    # 上限恢复后可重算成功
    assert start_trim(client, bid).status_code == 202
    st = wait_job(client, bid)
    assert st["job"]["status"] == "published"


def test_trim_no_pairs_survived_keeps_old_package(client, settings):
    bid = make_ready_batch(client)
    assert start_trim(client, bid, min_quality=30, min_length=8).status_code == 202
    assert wait_job(client, bid)["job"]["status"] == "published"
    old_content = client.get(f"/batches/{bid}/trim/files/R2/download").content

    # 最短读长超过原读长 → 所有读对被舍弃 → 任务失败，旧包保留
    assert start_trim(client, bid, min_quality=30, min_length=100).status_code == 202
    st = wait_job(client, bid)
    assert st["job"]["status"] == "failed"
    assert "no read pairs survived" in st["job"]["error"]
    assert client.get(f"/batches/{bid}/trim/files/R2/download").content == old_content
    # 中间文件已清理，结果包仍是旧包
    trim_dir = settings.batch_dir / bid / "trim"
    survivors = [p.name for p in trim_dir.iterdir()]
    assert survivors == [f"pkg-{st['package']['job_id']}"]
    assert not any(name.startswith("staging") for name in survivors)


# --------------------------------------------------------------------- #
# 重启恢复
# --------------------------------------------------------------------- #


def test_restart_recovers_trim_state(client, settings):
    bid = make_ready_batch(client)
    assert start_trim(client, bid).status_code == 202
    st = wait_job(client, bid)
    assert st["job"]["status"] == "published"
    pkg_dir = settings.batch_dir / bid / "trim" / f"pkg-{st['package']['job_id']}"
    assert pkg_dir.is_dir()

    # 模拟崩溃现场：在途任务记录、staging 中间文件、未发布的包目录
    now = "2026-01-01T00:00:00.000000Z"
    with get_conn(settings.db_path) as conn:
        conn.execute(
            "INSERT INTO trim_jobs (id, batch_id, min_quality, min_length, status,"
            " created_at, updated_at) VALUES ('deadjob', ?, 30, 8, 'running', ?, ?)",
            (bid, now, now),
        )
    staging = settings.batch_dir / bid / "trim" / "staging-deadjob"
    staging.mkdir(parents=True)
    (staging / "R1.fastq").write_bytes(b"partial")
    orphan_pkg = settings.batch_dir / bid / "trim" / "pkg-deadjob"
    orphan_pkg.mkdir()
    (orphan_pkg / "R1.fastq").write_bytes(b"never published")

    new_service = BatchService(Settings())
    summary = asyncio.run(new_service.recover_on_startup())
    assert summary["trim_cleaned"] >= 2

    # 在途任务判失败；已发布包保留；staging 与未发布包被清理
    with get_conn(settings.db_path) as conn:
        dead = conn.execute(
            "SELECT status, error FROM trim_jobs WHERE id = 'deadjob'"
        ).fetchone()
    assert dead["status"] == "failed" and "restart" in dead["error"]
    trim_dir = settings.batch_dir / bid / "trim"
    assert [p.name for p in trim_dir.iterdir()] == [pkg_dir.name]

    # 新 service 的配额账与磁盘一致（含保留的结果包）
    usage = asyncio.run(new_service.space.usage())
    assert usage[bid] == directory_size(settings.batch_dir / bid)

    # 已发布包仍可下载
    dl = client.get(f"/batches/{bid}/trim/files/R1/download")
    assert dl.status_code == 200 and dl.content.startswith(b"@trim-0/1\n")
