"""ready 批次的双端质量裁剪（Phred+33 尾部裁剪）。

从已放行的 R1 / R2 逐对读取记录，裁去两端尾部连续低于阈值的碱基与质量字符；
任一端裁剪后不足最短读长则舍弃整对（两端同步丢弃，配对关系不变）。

输出写入批次目录下的 staging 目录，所有落盘字节在写入前都向 SpaceManager
预留，因此结果包与原始文件共用同一批次字节上限，超限在写入途中即失败，
不会在落盘后才发现。staging 目录由调用方在发布或清理时处理。
"""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

import aiofiles

from .storage import SpaceManager

_READ_CHUNK = 1 << 20  # 输入每次读 1 MiB
_FLUSH_SIZE = 256 * 1024  # 输出缓冲超过 256 KiB 即预留配额并落盘


class TrimError(Exception):
    """裁剪过程中的可预期失败（输入损坏 / 无读对存活等）。"""


def trim_tail(seq: bytes, qual: bytes, threshold: int) -> tuple[bytes, bytes]:
    """裁去尾部连续 Phred 值低于阈值的碱基；threshold = min_quality + 33。"""
    end = len(qual)
    while end > 0 and qual[end - 1] < threshold:
        end -= 1
    return seq[:end], qual[:end]


async def _iter_lines(fh) -> AsyncIterator[bytes]:
    """按行产出（去行尾换行，兼容 CRLF）；文件必须以换行结尾。"""
    buf = b""
    while True:
        chunk = await fh.read(_READ_CHUNK)
        if not chunk:
            break
        buf += chunk
        parts = buf.split(b"\n")
        buf = parts.pop()  # 末尾是不完整行，留给下一轮
        for part in parts:
            yield part[:-1] if part.endswith(b"\r") else part
    if buf:
        raise TrimError("input file does not end with newline")


async def _iter_records(fh, side: str) -> AsyncIterator[tuple[bytes, bytes, bytes]]:
    """把行流按四行制分组成 (header, seq, qual)。

    输入是上传时已通过严格校验的原始文件，这里只做防御性复查。
    """
    lines = _iter_lines(fh)
    index = 0
    while True:
        header = await anext(lines, None)
        if header is None:
            return
        seq = await anext(lines, None)
        plus = await anext(lines, None)
        qual = await anext(lines, None)
        if seq is None or plus is None or qual is None:
            raise TrimError(f"{side} truncated at record {index + 1}")
        if not header.startswith(b"@") or plus != b"+" or len(seq) != len(qual):
            raise TrimError(f"{side} record {index + 1}: invalid FASTQ structure")
        yield header, seq, qual
        index += 1


class _QuotaWriter:
    """带配额的缓冲写出：每次落盘前先向 SpaceManager 预留对应字节。"""

    def __init__(self, fh, space: SpaceManager, batch_id: str) -> None:
        self._fh = fh
        self._space = space
        self._batch_id = batch_id
        self._buf = bytearray()
        self.written = 0

    async def write(self, data: bytes) -> None:
        self._buf += data
        if len(self._buf) >= _FLUSH_SIZE:
            await self.flush()

    async def flush(self) -> None:
        if not self._buf:
            return
        n = len(self._buf)
        await self._space.reserve(self._batch_id, n)
        try:
            await self._fh.write(bytes(self._buf))
        except BaseException:
            await self._space.release(self._batch_id, n)
            raise
        self.written += n
        self._buf.clear()


async def trim_fastq_pair(
    *,
    r1_path: Path,
    r2_path: Path,
    out_r1: Path,
    out_r2: Path,
    min_quality: int,
    min_length: int,
    space: SpaceManager,
    batch_id: str,
) -> tuple[int, int]:
    """逐对裁剪 R1 / R2，结果写入 out_r1 / out_r2。

    返回 (输入读对数, 保留读对数)。配额超限抛 QuotaExceededError，
    输入损坏抛 TrimError；已写入的 staging 字节由调用方负责清理与对账。
    """
    threshold = min_quality + 33
    pairs_in = 0
    pairs_kept = 0

    out_r1.parent.mkdir(parents=True, exist_ok=True)
    out_r2.parent.mkdir(parents=True, exist_ok=True)

    async with (
        aiofiles.open(r1_path, "rb") as f1,
        aiofiles.open(r2_path, "rb") as f2,
        aiofiles.open(out_r1, "wb") as o1,
        aiofiles.open(out_r2, "wb") as o2,
    ):
        w1 = _QuotaWriter(o1, space, batch_id)
        w2 = _QuotaWriter(o2, space, batch_id)
        recs1 = _iter_records(f1, "R1")
        recs2 = _iter_records(f2, "R2")
        while True:
            r1 = await anext(recs1, None)
            r2 = await anext(recs2, None)
            if r1 is None and r2 is None:
                break
            if r1 is None or r2 is None:
                raise TrimError("R1 and R2 have different record counts")
            pairs_in += 1
            h1, s1, q1 = r1
            h2, s2, q2 = r2
            s1t, q1t = trim_tail(s1, q1, threshold)
            s2t, q2t = trim_tail(s2, q2, threshold)
            if len(s1t) < min_length or len(s2t) < min_length:
                continue  # 任一端不足最短读长：舍弃整对
            pairs_kept += 1
            await w1.write(h1 + b"\n" + s1t + b"\n+\n" + q1t + b"\n")
            await w2.write(h2 + b"\n" + s2t + b"\n+\n" + q2t + b"\n")
        await w1.flush()
        await w2.flush()

    return pairs_in, pairs_kept
