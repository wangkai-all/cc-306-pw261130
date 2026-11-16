"""SQLite 存储层。

双时态建模：
- event_time: 业务事件真实发生的时间（迟到事件该字段仍在过去）
- ingest_version: 系统「知道」该事实时的版本号（单调递增）
- 身份边用 from_version / to_version 表达生效区间：合并打开边，拆关闭边，边本身不删除，
  因此任意历史版本的身份图都可以被重新构造（报告版本可复算）。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Iterable

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS versions (
    version        INTEGER PRIMARY KEY,
    created_at     TEXT NOT NULL,
    kind           TEXT NOT NULL,           -- node_birth|edge_open|edge_close|event_ingest|recompute_note
    summary        TEXT NOT NULL,
    detail_json    TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS id_nodes (
    id                 TEXT PRIMARY KEY,     -- acc_* / dev_* / anon_*
    kind               TEXT NOT NULL,        -- account|device|anonymous
    born_version       INTEGER NOT NULL,     -- 节点首次出现（被系统知道）的版本
    born_event_time    TEXT NOT NULL,        -- 首个事件的业务时间，可能早于 born_version
    UNIQUE(id),
    FOREIGN KEY(born_version) REFERENCES versions(version)
);

CREATE TABLE IF NOT EXISTS id_edges (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    a              TEXT NOT NULL,
    b              TEXT NOT NULL,
    from_version   INTEGER NOT NULL,         -- 合并生效版本（含）
    to_version     INTEGER,                  -- 拆分版本（含起该边失效）；NULL = 仍生效
    reason         TEXT NOT NULL,
    FOREIGN KEY(a) REFERENCES id_nodes(id),
    FOREIGN KEY(b) REFERENCES id_nodes(id)
);
CREATE INDEX IF NOT EXISTS idx_edges_ver ON id_edges(from_version, to_version);

CREATE TABLE IF NOT EXISTS events (
    id               TEXT PRIMARY KEY,       -- 幂等键
    event_time       TEXT NOT NULL,
    ingest_version   INTEGER NOT NULL,       -- 到达（被知道）的版本
    emitter_id       TEXT,                   -- 发射事件的节点；空表示无法归属的孤立回补
    attached_ids     TEXT NOT NULL DEFAULT '[]',  -- 到达时挂载的节点 id 列表 JSON
    event_type       TEXT NOT NULL,
    payload_json     TEXT NOT NULL DEFAULT '{}',
    FOREIGN KEY(ingest_version) REFERENCES versions(version),
    FOREIGN KEY(emitter_id) REFERENCES id_nodes(id)
);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_ingest ON events(ingest_version);

CREATE TABLE IF NOT EXISTS report_snapshots (
    snapshot_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT NOT NULL,
    pinned_version   INTEGER NOT NULL,
    basis            TEXT NOT NULL,          -- entity|account|visit
    label            TEXT NOT NULL,
    payload_json     TEXT NOT NULL,
    job_id           INTEGER,
    FOREIGN KEY(pinned_version) REFERENCES versions(version)
);

CREATE TABLE IF NOT EXISTS recompute_jobs (
    job_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at       TEXT NOT NULL,
    finished_at      TEXT,
    pinned_version   INTEGER NOT NULL,       -- 启动瞬间钉住的版本：队列与分母必须同版
    latest_at_end    INTEGER,                -- 完成时的最新版本
    status           TEXT NOT NULL,          -- running|done|stale
    basis            TEXT NOT NULL,
    note             TEXT NOT NULL DEFAULT ''
);
"""


class Database:
    """封装 SQLite 连接。

    写入侧（ingest 模块）持有全局写锁并使用 write_conn；
    读取侧（report 模块、HTTP 只读请求）每次新建 read 连接，WAL 下读写互不阻塞。
    """

    def __init__(self, path: str):
        self.path = path
        # check_same_thread=False：写锁保证同一时刻只有一个线程使用 write_conn
        self.write_conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.write_conn.row_factory = sqlite3.Row
        self.write_lock = threading.RLock()
        self._apply_pragmas(self.write_conn)

    @staticmethod
    def _apply_pragmas(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")

    def read_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        return conn

    @contextmanager
    def transaction(self):
        """串行化写入事务：版本号递增必须与业务写入原子提交。"""
        with self.write_lock:
            self.write_conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.write_conn
                self.write_conn.execute("COMMIT")
            except Exception:
                self.write_conn.execute("ROLLBACK")
                raise

    def current_version(self, conn: sqlite3.Connection | None = None) -> int:
        conn = conn or self.write_conn
        row = conn.execute("SELECT COALESCE(MAX(version), 0) AS v FROM versions").fetchone()
        return int(row["v"])

    def initialize(self) -> None:
        with self.write_lock:
            self.write_conn.executescript(SCHEMA)

    def fetchall_dicts(self, sql: str, params: Iterable[Any] = ()) -> list[dict]:
        conn = self.read_connection()
        try:
            return [dict(r) for r in conn.execute(sql, list(params)).fetchall()]
        finally:
            conn.close()
