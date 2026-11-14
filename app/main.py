"""FastAPI 入口：双端 FASTQ.gz 单进程接收服务。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

from .config import get_settings
from .models import FileRole
from .schemas import (
    BatchCreateRequest,
    BatchListResponse,
    BatchResponse,
    CancelResponse,
    TrimAcceptedResponse,
    TrimRequest,
    TrimStatusResponse,
)
from .service import BatchService

logger = logging.getLogger("fastq_receiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

settings = get_settings()
service = BatchService(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("data dir: %s, per-batch byte limit: %d", settings.data_dir, settings.batch_byte_limit)
    summary = await service.recover_on_startup()
    logger.info("startup recovery: %s", summary)
    yield


app = FastAPI(
    title="双端 FASTQ.gz 接收 API",
    description=(
        "测序双端数据按样本建批接收。R1、R2 各以请求体流式上传一次，"
        "单成员 gzip、严格四行制 FASTQ，两端条数与读段名逐对校验。"
        "坏包或校验失败整批拒收；单批文件字节上限接收过程中实时守限。"
        "ready 批次可提交质量裁剪任务：按 Phred+33 阈值裁去两端尾部低质量碱基，"
        "任一端不足最短读长则舍弃整对，结果以原子发布的结果包提供下载，"
        "原始文件与批次状态不变，可调整阈值重算。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.post("/batches", response_model=BatchResponse, status_code=201, tags=["batches"],
          summary="按样本创建接收批次")
async def create_batch(payload: BatchCreateRequest):
    return await service.create_batch(payload.sample_id)


@app.get("/batches", response_model=BatchListResponse, tags=["batches"], summary="列出全部批次")
async def list_batches():
    return {"batches": await service.list_batches()}


@app.get("/batches/{batch_id}", response_model=BatchResponse, tags=["batches"], summary="查询批次状态")
async def get_batch(batch_id: str):
    return await service.get_batch_detail(batch_id)


@app.post(
    "/batches/{batch_id}/files/{role}",
    response_model=BatchResponse,
    tags=["upload"],
    summary="流式上传 R1 或 R2（请求体即 gzip 字节，不用 multipart）",
    responses={
        409: {"description": "批次非 receiving / 该端已上传 / 批次取消中"},
        413: {"description": "超过单批文件字节上限，本批已失败并清理"},
        422: {"description": "坏 gzip 或 FASTQ 校验失败，本批已拒收并清理"},
    },
)
async def upload_file(batch_id: str, role: FileRole, request: Request):
    return await service.upload(batch_id, role, request)


@app.post("/batches/{batch_id}/cancel", response_model=CancelResponse, tags=["batches"],
          summary="取消批次（等待写入停止、文件删除、空间释放后确认）")
async def cancel_batch(batch_id: str):
    return await service.cancel(batch_id)


@app.get("/batches/{batch_id}/files/{role}/download", tags=["download"],
         summary="下载展开后的 R1 / R2 FASTQ（仅 ready 批次）")
async def download_file(batch_id: str, role: FileRole):
    path, filename = service.download_path(batch_id, role)
    return FileResponse(path, media_type="text/plain", filename=filename)


@app.post(
    "/batches/{batch_id}/trim",
    response_model=TrimAcceptedResponse,
    status_code=202,
    tags=["trim"],
    summary="为 ready 批次启动质量裁剪任务（后台执行，同批仅一项在途）",
    responses={
        409: {"description": "批次非 ready / 已有裁剪任务在途 / 原始文件缺失"},
    },
)
async def start_trim(batch_id: str, payload: TrimRequest):
    job = await service.start_trim(batch_id, payload.min_quality, payload.min_length)
    return {"job": job, "message": "trim job accepted; poll GET /batches/{id}/trim for progress"}


@app.get(
    "/batches/{batch_id}/trim",
    response_model=TrimStatusResponse,
    tags=["trim"],
    summary="查询最近一次裁剪任务状态与当前已发布结果包",
    responses={404: {"description": "批次不存在"}},
)
async def get_trim(batch_id: str):
    return await service.get_trim_status(batch_id)


@app.get(
    "/batches/{batch_id}/trim/files/{role}/download",
    tags=["trim"],
    summary="下载当前已发布结果包中的裁剪后 R1 / R2 FASTQ",
    responses={
        404: {"description": "批次不存在 / 尚无已发布结果包"},
        409: {"description": "批次非 ready"},
    },
)
async def download_trimmed_file(batch_id: str, role: FileRole):
    path, filename = service.trim_download_path(batch_id, role)
    return FileResponse(path, media_type="text/plain", filename=filename)


@app.get("/health", tags=["system"], summary="健康检查")
async def health():
    usage = await service.space.usage()
    return {
        "status": "ok",
        "batch_byte_limit": settings.batch_byte_limit,
        "active_batches": len(usage),
        "data_dir": str(settings.data_dir),
    }
