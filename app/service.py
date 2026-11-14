"""批次生命周期服务：创建、上传校验、取消、回收、重启恢复、质量裁剪。"""

from __future__ import annotations

import asyncio
import os
import shutil
import uuid
from pathlib import Path

from fastapi import HTTPException

from .database import get_conn, init_db
from .models import BatchStatus, FileRole, TrimJobStatus, utcnow_iso
from .storage import (
    BatchRegistry,
    QuotaExceededError,
    SpaceManager,
    directory_size,
    remove_batch_files,
)
from .trimming import TrimError, trim_fastq_pair
from .validation import StreamError, stream_gunzip, validate_fastq_pair, validate_fastq_structure

TERMINAL_STATUSES = {BatchStatus.READY.value, BatchStatus.FAILED.value, BatchStatus.CANCELLED.value}


class BatchService:
    def __init__(self, settings) -> None:
        self.settings = settings
        init_db(settings.db_path)
        self.space = SpaceManager(settings.batch_byte_limit)
        self.registry = BatchRegistry()
        self._recovered = False

    # ------------------------------------------------------------------ #
    # 路径
    # ------------------------------------------------------------------ #
    def _dir(self, batch_id: str) -> Path:
        return self.settings.batch_dir / batch_id

    def _gz_path(self, batch_id: str, role: FileRole) -> Path:
        return self._dir(batch_id) / f"{role.value}.fastq.gz.uploading"

    def _fastq_path(self, batch_id: str, role: FileRole) -> Path:
        return self._dir(batch_id) / f"{role.value}.fastq"

    def _trim_dir(self, batch_id: str) -> Path:
        return self._dir(batch_id) / "trim"

    # ------------------------------------------------------------------ #
    # DB 小工具
    # ------------------------------------------------------------------ #
    def _row(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
            return dict(row) if row else None

    def _files(self, batch_id: str) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute(
                "SELECT role, size, uploaded_at FROM files WHERE batch_id = ? ORDER BY role",
                (batch_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 创建 / 查询
    # ------------------------------------------------------------------ #
    async def create_batch(self, sample_id: str) -> dict:
        batch_id = uuid.uuid4().hex
        now = utcnow_iso()
        self._dir(batch_id).mkdir(parents=True, exist_ok=True)
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "INSERT INTO batches (id, sample_id, status, byte_limit, error, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, NULL, ?, ?)",
                (batch_id, sample_id, BatchStatus.RECEIVING.value, self.settings.batch_byte_limit, now, now),
            )
        await self.registry.create(batch_id)
        return await self.get_batch_detail(batch_id)

    async def get_batch_detail(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        row["files"] = self._files(batch_id)
        row["bytes_used"] = directory_size(self._dir(batch_id))
        return row

    async def list_batches(self) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute("SELECT id FROM batches ORDER BY created_at").fetchall()
        return [await self.get_batch_detail(r["id"]) for r in rows]

    # ------------------------------------------------------------------ #
    # 终态处理
    # ------------------------------------------------------------------ #
    def _mark_failed(self, batch_id: str, reason: str) -> None:
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE batches SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (BatchStatus.FAILED.value, reason[:2000], now, batch_id),
            )
            conn.execute("DELETE FROM files WHERE batch_id = ?", (batch_id,))

    async def _cleanup_files_only(self, batch_id: str) -> int:
        """删除本批全部文件并按磁盘真实占用回收配额，但不改批次状态。"""
        freed = await asyncio.to_thread(remove_batch_files, self._dir(batch_id))
        await self.space.release(batch_id, freed)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        return freed

    async def fail_and_cleanup(self, batch_id: str, reason: str) -> int:
        """标记失败，删除本批全部文件并按磁盘真实占用回收配额。"""
        await self._cleanup_files_only(batch_id)
        self._mark_failed(batch_id, reason)
        return 0

    # ------------------------------------------------------------------ #
    # 上传
    # ------------------------------------------------------------------ #
    async def upload(self, batch_id: str, role: FileRole, request) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.RECEIVING.value:
            raise HTTPException(status_code=409, detail=f"batch is {row['status']}, upload rejected")

        with get_conn(self.settings.db_path) as conn:
            dup = conn.execute(
                "SELECT 1 FROM files WHERE batch_id = ? AND role = ?", (batch_id, role.value)
            ).fetchone()
        if dup:
            raise HTTPException(status_code=409, detail=f"{role.value} already uploaded")

        runtime = await self.registry.create(batch_id)
        if runtime.cancel_event.is_set():
            raise HTTPException(status_code=409, detail="batch is cancelling")

        current_task = asyncio.current_task()
        if current_task is not None:
            runtime.uploads[role] = current_task

        gz_path = self._gz_path(batch_id, role)
        out_path = self._fastq_path(batch_id, role)
        gz_bytes = 0
        try:
            try:
                out_bytes = await stream_gunzip(
                    request=request,
                    gz_path=gz_path,
                    out_path=out_path,
                    space=self.space,
                    batch_id=batch_id,
                    cancel_event=runtime.cancel_event,
                    idle_timeout=self.settings.upload_idle_timeout,
                )
                gz_bytes = gz_path.stat().st_size

                # 单端四行制结构校验（线程内做，避免阻塞事件循环）
                await asyncio.to_thread(validate_fastq_structure, out_path)

                # 压缩临时文件已完成使命，立刻删除并回收其占用配额
                gz_path.unlink()
                await self.space.release(batch_id, gz_bytes)

                now = utcnow_iso()
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "INSERT INTO files (batch_id, role, path, size, uploaded_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (batch_id, role.value, str(out_path), out_bytes, now),
                    )
                    conn.execute(
                        "UPDATE batches SET updated_at = ? WHERE id = ?", (now, batch_id)
                    )
            except QuotaExceededError as exc:
                await self.fail_and_cleanup(batch_id, str(exc))
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except (StreamError, ValueError, OSError) as exc:
                if runtime.cancel_event.is_set():
                    # 取消导致的中断：只清理磁盘，终态由 cancel() 落库，避免抢占为 failed
                    await self._cleanup_files_only(batch_id)
                    raise HTTPException(status_code=409, detail="batch cancelled") from exc
                await self.fail_and_cleanup(batch_id, f"{role.value} rejected: {exc}")
                raise HTTPException(
                    status_code=422, detail=f"{role.value} rejected: {exc}"
                ) from exc
            except HTTPException:
                raise
            except Exception as exc:  # 客户端断连等兜底
                if runtime.cancel_event.is_set():
                    await self._cleanup_files_only(batch_id)
                    raise HTTPException(status_code=409, detail="batch cancelled") from exc
                await self.fail_and_cleanup(batch_id, f"{role.value} upload failed: {exc}")
                raise HTTPException(
                    status_code=500, detail=f"{role.value} upload failed: {exc}"
                ) from exc

            # 两端都到齐则做配对校验
            detail = await self.get_batch_detail(batch_id)
            roles = {f["role"] for f in detail["files"]}
            if {FileRole.R1, FileRole.R2}.issubset(roles):
                try:
                    await asyncio.to_thread(
                        validate_fastq_pair,
                        self._fastq_path(batch_id, FileRole.R1),
                        self._fastq_path(batch_id, FileRole.R2),
                    )
                except ValueError as exc:
                    await self.fail_and_cleanup(batch_id, f"pair validation failed: {exc}")
                    raise HTTPException(
                        status_code=422, detail=f"pair validation failed: {exc}"
                    ) from exc
                now = utcnow_iso()
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "UPDATE batches SET status = ?, error = NULL, updated_at = ? WHERE id = ?",
                        (BatchStatus.READY.value, now, batch_id),
                    )
            return await self.get_batch_detail(batch_id)
        finally:
            runtime.uploads.pop(role, None)

    # ------------------------------------------------------------------ #
    # 取消
    # ------------------------------------------------------------------ #
    async def cancel(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] in TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"batch already {row['status']}")

        runtime = await self.registry.create(batch_id)
        runtime.cancel_event.set()

        # 等待本批正在进行的写入停止（上传任务退出后不会再落盘）
        if runtime.uploads:
            await asyncio.gather(*runtime.uploads.values(), return_exceptions=True)

        # 上传任务可能在取消信号到达前恰好完成（ready）或自行失败，重新确认终态
        refreshed = self._row(batch_id)
        if refreshed is not None and refreshed["status"] in TERMINAL_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"batch already {refreshed['status']} before cancel took effect",
            )

        # 文件删除、空间释放、状态落库全部完成后才确认
        freed = await asyncio.to_thread(remove_batch_files, self._dir(batch_id))
        await self.space.release(batch_id, freed)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE batches SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (BatchStatus.CANCELLED.value, "cancelled by operator", now, batch_id),
            )
            conn.execute("DELETE FROM files WHERE batch_id = ?", (batch_id,))
        await self.registry.remove(batch_id)
        return {
            "id": batch_id,
            "status": BatchStatus.CANCELLED,
            "freed_bytes": freed,
            "message": "all writes stopped, files deleted and space released",
        }

    # ------------------------------------------------------------------ #
    # 下载
    # ------------------------------------------------------------------ #
    def download_path(self, batch_id: str, role: FileRole) -> tuple[Path, str]:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(status_code=409, detail=f"batch is {row['status']}, not downloadable")
        path = self._fastq_path(batch_id, role)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"{role.value} file missing on disk")
        return path, f"{row['sample_id']}_{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # 质量裁剪
    # ------------------------------------------------------------------ #
    def _latest_trim_job(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM trim_jobs WHERE batch_id = ?"
                " ORDER BY created_at DESC, rowid DESC LIMIT 1",
                (batch_id,),
            ).fetchone()
            return dict(row) if row else None

    def _trim_package(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM trim_packages WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            return dict(row) if row else None

    async def start_trim(self, batch_id: str, min_quality: int, min_length: int) -> dict:
        """为 ready 批启动后台裁剪任务；同批只允许一项在途。"""
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(
                status_code=409,
                detail=f"batch is {row['status']}, trim requires a ready batch",
            )
        r1 = self._fastq_path(batch_id, FileRole.R1)
        r2 = self._fastq_path(batch_id, FileRole.R2)
        if not r1.exists() or not r2.exists():
            raise HTTPException(status_code=409, detail="original FASTQ files missing on disk")

        runtime = await self.registry.create(batch_id)
        # 同批只能有一项裁剪在途；检查与登记之间不 await，保证并发下原子
        task = runtime.trim_task
        if task is not None and not task.done():
            raise HTTPException(
                status_code=409, detail="a trim job is already running for this batch"
            )
        with get_conn(self.settings.db_path) as conn:
            running = conn.execute(
                "SELECT 1 FROM trim_jobs WHERE batch_id = ? AND status = ?",
                (batch_id, TrimJobStatus.RUNNING.value),
            ).fetchone()
        if running:
            raise HTTPException(
                status_code=409, detail="a trim job is already running for this batch"
            )

        job_id = uuid.uuid4().hex
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "INSERT INTO trim_jobs (id, batch_id, min_quality, min_length, status,"
                " error, pairs_in, pairs_kept, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)",
                (
                    job_id, batch_id, min_quality, min_length,
                    TrimJobStatus.RUNNING.value, now, now,
                ),
            )
        runtime.trim_task = asyncio.create_task(
            self._run_trim(batch_id, job_id, min_quality, min_length)
        )
        return self._latest_trim_job(batch_id)

    async def _run_trim(self, batch_id: str, job_id: str, min_quality: int, min_length: int) -> None:
        """后台裁剪：写 staging → 原子发布 → 失败/超限时保留旧包并清理中间文件。"""
        bdir = self._dir(batch_id)
        staging = self._trim_dir(batch_id) / f"staging-{job_id}"
        pkg_dir = self._trim_dir(batch_id) / f"pkg-{job_id}"
        old_pkg = self._trim_package(batch_id)
        try:
            pairs_in, pairs_kept = await trim_fastq_pair(
                r1_path=self._fastq_path(batch_id, FileRole.R1),
                r2_path=self._fastq_path(batch_id, FileRole.R2),
                out_r1=staging / "R1.fastq",
                out_r2=staging / "R2.fastq",
                min_quality=min_quality,
                min_length=min_length,
                space=self.space,
                batch_id=batch_id,
            )
            if pairs_kept == 0:
                raise TrimError(
                    f"no read pairs survived trimming (pairs_in={pairs_in}); loosen thresholds"
                )
            # 发布：staging 目录整体原子改名，随后落库切换「当前包」指针
            await asyncio.to_thread(os.replace, staging, pkg_dir)
            now = utcnow_iso()
            size = await asyncio.to_thread(directory_size, pkg_dir)
            with get_conn(self.settings.db_path) as conn:
                conn.execute(
                    "INSERT INTO trim_packages (batch_id, job_id, path, min_quality, min_length,"
                    " pairs_in, pairs_kept, size, published_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    " ON CONFLICT(batch_id) DO UPDATE SET job_id=excluded.job_id,"
                    " path=excluded.path, min_quality=excluded.min_quality,"
                    " min_length=excluded.min_length, pairs_in=excluded.pairs_in,"
                    " pairs_kept=excluded.pairs_kept, size=excluded.size,"
                    " published_at=excluded.published_at",
                    (batch_id, job_id, str(pkg_dir), min_quality, min_length,
                     pairs_in, pairs_kept, size, now),
                )
                conn.execute(
                    "UPDATE trim_jobs SET status = ?, pairs_in = ?, pairs_kept = ?,"
                    " updated_at = ? WHERE id = ?",
                    (TrimJobStatus.PUBLISHED.value, pairs_in, pairs_kept, now, job_id),
                )
            # 新包完整发布后旧包才删除；发布前旧包一直可下载
            if old_pkg is not None and Path(old_pkg["path"]) != pkg_dir:
                await asyncio.to_thread(shutil.rmtree, Path(old_pkg["path"]), True)
        except QuotaExceededError as exc:
            self._fail_trim_job(job_id, f"byte quota exceeded: {exc}")
        except (TrimError, OSError) as exc:
            self._fail_trim_job(job_id, str(exc))
        except Exception as exc:  # 兜底：任何失败都不得弄丢旧包
            self._fail_trim_job(job_id, f"trim failed: {exc}")
        finally:
            # 清理中间文件，按磁盘真实占用对账（保留的旧包/新包字节继续计入）
            if staging.exists():
                await asyncio.to_thread(shutil.rmtree, staging, True)
            await self.space.set_batch_usage(batch_id, directory_size(bdir))
            runtime = await self.registry.get(batch_id)
            if runtime is not None and runtime.trim_task is asyncio.current_task():
                runtime.trim_task = None

    def _fail_trim_job(self, job_id: str, reason: str) -> None:
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE trim_jobs SET status = ?, error = ?, updated_at = ?"
                " WHERE id = ? AND status = ?",
                (
                    TrimJobStatus.FAILED.value, reason[:2000], now, job_id,
                    TrimJobStatus.RUNNING.value,
                ),
            )

    async def get_trim_status(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        return {
            "batch_id": batch_id,
            "job": self._latest_trim_job(batch_id),
            "package": self._trim_package(batch_id),
        }

    def trim_download_path(self, batch_id: str, role: FileRole) -> tuple[Path, str]:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(
                status_code=409, detail=f"batch is {row['status']}, not downloadable"
            )
        pkg = self._trim_package(batch_id)
        if pkg is None:
            raise HTTPException(status_code=404, detail="no published trim package for this batch")
        path = Path(pkg["path"]) / f"{role.value}.fastq"
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"trimmed {role.value} file missing on disk")
        return path, f"{row['sample_id']}_trimmed_{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # 重启恢复
    # ------------------------------------------------------------------ #
    async def _clean_trim_intermediates(self, batch_id: str, keep_pkg: str | None) -> int:
        """删除批次 trim/ 下除当前已发布包之外的一切（staging 与未发布包）。"""
        tdir = self._trim_dir(batch_id)
        if not tdir.is_dir():
            return 0
        cleaned = 0
        for child in tdir.iterdir():
            if child.is_dir() and keep_pkg is not None and str(child) == keep_pkg:
                continue
            if child.is_dir():
                await asyncio.to_thread(shutil.rmtree, child, True)
            else:
                try:
                    child.unlink()
                except FileNotFoundError:
                    pass
            cleaned += 1
        return cleaned

    async def recover_on_startup(self) -> dict:
        """通过批保留；未完成批（receiving）一律判失败并回收磁盘后再对外服务。

        裁剪产物：重启时在途任务判失败，staging 与未发布包清理，已发布包保留。
        """
        # 同一 service 实例只恢复一次，避免测试中多个 ASGI 客户端重跑 lifespan 误判在途批
        if self._recovered:
            return {"ready": 0, "aborted": 0, "orphaned": 0, "skipped": "already-recovered"}
        self._recovered = True
        self.settings.batch_dir.mkdir(parents=True, exist_ok=True)
        recovered = {"ready": 0, "aborted": 0, "orphaned": 0, "trim_cleaned": 0}

        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute("SELECT id, status FROM batches").fetchall()
            known = {r["id"]: r["status"] for r in rows}
            # 重启时在途的裁剪任务一律判失败；已发布结果包保留
            now = utcnow_iso()
            conn.execute(
                "UPDATE trim_jobs SET status = ?, error = ?, updated_at = ? WHERE status = ?",
                (
                    TrimJobStatus.FAILED.value, "interrupted by server restart", now,
                    TrimJobStatus.RUNNING.value,
                ),
            )
            pkg_rows = conn.execute("SELECT batch_id, path FROM trim_packages").fetchall()
            referenced = {r["batch_id"]: r["path"] for r in pkg_rows}

        for batch_id, status in known.items():
            bdir = self._dir(batch_id)
            if status == BatchStatus.READY.value:
                # 先清理裁剪中间文件，再以磁盘真实文件为准重建配额账
                recovered["trim_cleaned"] += await self._clean_trim_intermediates(
                    batch_id, referenced.get(batch_id)
                )
                await self.space.set_batch_usage(batch_id, directory_size(bdir))
                recovered["ready"] += 1
                continue
            if status == BatchStatus.RECEIVING.value:
                await asyncio.to_thread(remove_batch_files, bdir)
                await self.space.remove_batch(batch_id)
                self._mark_failed(batch_id, "interrupted by server restart")
                recovered["aborted"] += 1
            # failed / cancelled：清理可能残留的文件
            else:
                await asyncio.to_thread(remove_batch_files, bdir)
                await self.space.remove_batch(batch_id)
            # 非 ready 批不可能持有裁剪产物，防御性清掉关联行
            with get_conn(self.settings.db_path) as conn:
                conn.execute("DELETE FROM trim_jobs WHERE batch_id = ?", (batch_id,))
                conn.execute("DELETE FROM trim_packages WHERE batch_id = ?", (batch_id,))

        # 磁盘上有目录但 DB 无记录的孤儿数据，直接回收
        for child in self.settings.batch_dir.iterdir():
            if child.is_dir() and child.name not in known:
                await asyncio.to_thread(remove_batch_files, child)
                recovered["orphaned"] += 1
        return recovered
