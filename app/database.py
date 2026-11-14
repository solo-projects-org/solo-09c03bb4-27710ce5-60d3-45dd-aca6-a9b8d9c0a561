"""SQLite 持久化层。

单进程应用，每次操作开一个连接，WAL 模式保证读写不互斥。
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id          TEXT PRIMARY KEY,
    sample_id   TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('receiving','ready','failed','cancelled')),
    byte_limit  INTEGER NOT NULL,
    error       TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id    TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('R1','R2')),
    path        TEXT NOT NULL,
    size        INTEGER NOT NULL,
    uploaded_at TEXT NOT NULL,
    UNIQUE(batch_id, role)
);

CREATE TABLE IF NOT EXISTS trim_jobs (
    id           TEXT PRIMARY KEY,
    batch_id     TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    min_quality  INTEGER NOT NULL,
    min_length   INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('running','published','failed')),
    error        TEXT,
    pairs_in     INTEGER,
    pairs_kept   INTEGER,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

-- 每批至多一行的「当前已发布裁剪结果包」指针；新包完整落库后才切换
CREATE TABLE IF NOT EXISTS trim_packages (
    batch_id     TEXT PRIMARY KEY REFERENCES batches(id) ON DELETE CASCADE,
    job_id       TEXT NOT NULL,
    path         TEXT NOT NULL,
    min_quality  INTEGER NOT NULL,
    min_length   INTEGER NOT NULL,
    pairs_in     INTEGER NOT NULL,
    pairs_kept   INTEGER NOT NULL,
    size         INTEGER NOT NULL,
    published_at TEXT NOT NULL
);
"""

_INIT_LOCK = threading.Lock()
_initialized: set[Path] = set()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with _INIT_LOCK:
        if db_path in _initialized:
            return
        conn = _connect(db_path)
        try:
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()
        _initialized.add(db_path)


@contextmanager
def get_conn(db_path: Path) -> Iterator[sqlite3.Connection]:
    conn = _connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
