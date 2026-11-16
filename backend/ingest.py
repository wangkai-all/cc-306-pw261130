"""写入侧：版本推进、事件回补、身份合并/拆分、报告快照与后台重算。

所有写操作都在 Database.transaction()（BEGIN IMMEDIATE + 进程内锁）中推进版本号，
保证「身份裁决、事件回补、后台重算」并行时，每份报告的队列与分母取自同一版本。
"""
from __future__ import annotations

import json
import threading
import time

from .db import Database
from .report import BASES, build_report
from .timeutil import now_iso, parse_iso


class IngestError(ValueError):
    """可直接返回给前端的 400 类业务错误。"""


def _bump_version(conn, kind: str, summary: str, detail: dict | None = None) -> int:
    v = conn.execute("SELECT COALESCE(MAX(version), 0) + 1 AS v FROM versions").fetchone()["v"]
    conn.execute(
        "INSERT INTO versions(version, created_at, kind, summary, detail_json) VALUES (?,?,?,?,?)",
        (v, now_iso(), kind, summary, json.dumps(detail or {}, ensure_ascii=False)),
    )
    return v


def _ensure_node(conn, node_id: str, kind: str, born_version: int, event_time: str) -> bool:
    existed = conn.execute("SELECT 1 FROM id_nodes WHERE id = ?", (node_id,)).fetchone()
    if not existed:
        conn.execute(
            "INSERT INTO id_nodes(id, kind, born_version, born_event_time) VALUES (?,?,?,?)",
            (node_id, kind, born_version, event_time),
        )
    return not bool(existed)


def ingest_event(db: Database, event_id: str, event_time: str, emitter_id: str | None,
                 emitter_kind: str | None, attached_ids: list[str],
                 event_type: str, payload: dict | None = None,
                 summary: str | None = None) -> dict:
    """回补一个活动事件。

    - event_time 可以远早于当前版本（迟到事件）；它在「当前版本」才对报告可见。
    - emitter 节点若尚未出生，随本次事件出生（born_version 即本版本）。
    - attached_ids 是事件到达时挂载到的全部身份节点，拆分后用于判定可归属/不确定。
    """
    # 时间格式先在锁外校验，尽早失败
    parse_iso(event_time)
    with db.transaction() as conn:
        if conn.execute("SELECT 1 FROM events WHERE id = ?", (event_id,)).fetchone():
            raise IngestError(f"事件 {event_id} 已存在（幂等键冲突）")
        is_late = bool(conn.execute("SELECT 1 FROM versions LIMIT 1").fetchone())
        v = _bump_version(
            conn, "event_ingest",
            summary or f"回补事件 {event_id}（{event_type}，发生于 {event_time}）",
            {"event_id": event_id, "event_time": event_time, "late": is_late},
        )
        born = []
        if emitter_id:
            existing = conn.execute("SELECT kind FROM id_nodes WHERE id = ?",
                                    (emitter_id,)).fetchone()
            if not existing:
                if emitter_kind is None:
                    raise IngestError("新建 emitter 节点必须提供 emitter_kind")
                if _ensure_node(conn, emitter_id, emitter_kind, v, event_time):
                    born.append(emitter_id)
        for nid in attached_ids:
            row = conn.execute("SELECT kind FROM id_nodes WHERE id = ?", (nid,)).fetchone()
            if not row:
                raise IngestError(f"挂载节点 {nid} 不存在，请先回补带该 emitter 的事件")
        conn.execute(
            """INSERT INTO events(id, event_time, ingest_version, emitter_id,
                                  attached_ids, event_type, payload_json)
               VALUES (?,?,?,?,?,?,?)""",
            (event_id, event_time, v, emitter_id, json.dumps(attached_ids),
             event_type, json.dumps(payload or {}, ensure_ascii=False)),
        )
        return {"version": v, "nodes_born": born, "event_id": event_id}


def merge_identities(db: Database, a: str, b: str, reason: str) -> dict:
    """合并两个身份节点（打开一条身份边）。

    约束：
    - 两个节点都必须存在；
    - 禁止自环与重复合并（已在同一实体中）；
    - 一个实体至多一个账号节点，避免账号口径出现无法选择成员键的歧义。
    """
    if a == b:
        raise IngestError("不能合并节点自身")
    with db.transaction() as conn:
        na = conn.execute("SELECT * FROM id_nodes WHERE id = ?", (a,)).fetchone()
        nb = conn.execute("SELECT * FROM id_nodes WHERE id = ?", (b,)).fetchone()
        if not na or not nb:
            raise IngestError("待合并的节点不存在")
        # 在当前图上求分量
        from .report import build_graph
        graph = build_graph(conn, db.current_version(conn))
        ra, rb = graph.find(a), graph.find(b)
        if ra == rb:
            raise IngestError(f"{a} 与 {b} 已属于同一稳定实体，无需合并")
        accounts = [
            nid for nid in (graph.root_members[ra] | graph.root_members[rb])
            if graph.nodes[nid]["kind"] == "account"
        ]
        if len(accounts) > 1:
            raise IngestError(
                f"合并将使同一实体包含多个账号 {accounts}；账号口径要求一实体至多一账号"
            )
        v = _bump_version(conn, "edge_open", f"身份合并：{a} ⇔ {b}",
                          {"a": a, "b": b, "reason": reason})
        conn.execute(
            "INSERT INTO id_edges(a, b, from_version, to_version, reason) VALUES (?,?,?,NULL,?)",
            (a, b, v, reason),
        )
        return {"version": v, "edge": {"a": a, "b": b, "from_version": v}, "action": "merged"}


def split_identities(db: Database, a: str, b: str, reason: str) -> dict:
    """撤销合并：关闭 a、b 之间当前生效的边。

    边记录保留（to_version 被赋值），历史版本的图仍可重建。
    拆分后，挂载在多个如今已分开的节点上且无 emitter 的历史事件将变成「不确定」。
    """
    with db.transaction() as conn:
        edge = conn.execute(
            """SELECT * FROM id_edges
               WHERE to_version IS NULL
                 AND ((a = ? AND b = ?) OR (a = ? AND b = ?))
               ORDER BY id DESC LIMIT 1""",
            (a, b, b, a),
        ).fetchone()
        if not edge:
            raise IngestError(f"{a} 与 {b} 之间没有生效中的合并边，无法撤销")
        v = _bump_version(conn, "edge_close", f"撤销合并：{a} ⊘ {b}",
                          {"a": a, "b": b, "edge_id": edge["id"], "reason": reason})
        conn.execute("UPDATE id_edges SET to_version = ? WHERE id = ?", (v, edge["id"]))
        return {"version": v, "edge_id": edge["id"], "action": "split"}


# --------------------------------------------------------------------------------------
# 报告快照与后台重算
# --------------------------------------------------------------------------------------

def save_snapshot(db: Database, version: int, basis: str, label: str,
                  job_id: int | None = None) -> int:
    conn = db.read_connection()
    try:
        payload = build_report(conn, version, basis)
    finally:
        conn.close()
    with db.transaction() as wconn:
        cur = wconn.execute(
            "INSERT INTO report_snapshots(created_at, pinned_version, basis, label, payload_json, job_id)"
            " VALUES (?,?,?,?,?,?)",
            (now_iso(), version, basis, label,
             json.dumps(payload, ensure_ascii=False), job_id),
        )
        return int(cur.lastrowid)


def list_snapshots(db: Database) -> list[dict]:
    rows = db.fetchall_dicts(
        "SELECT snapshot_id, created_at, pinned_version, basis, label, job_id "
        "FROM report_snapshots ORDER BY snapshot_id DESC"
    )
    latest = _latest_version(db)
    for r in rows:
        r["is_current"] = r["pinned_version"] == latest
    return rows


def get_snapshot(db: Database, snapshot_id: int) -> dict:
    rows = db.fetchall_dicts(
        "SELECT * FROM report_snapshots WHERE snapshot_id = ?", (snapshot_id,))
    if not rows:
        raise IngestError("快照不存在")
    row = rows[0]
    row["payload"] = json.loads(row.pop("payload_json"))
    return row


def _latest_version(db: Database) -> int:
    return db.fetchall_dicts("SELECT COALESCE(MAX(version),0) AS v FROM versions")[0]["v"]


def start_recompute(db: Database, basis: str, simulated_seconds: float = 2.0) -> dict:
    """启动一个后台重算任务。

    任务在启动瞬间钉住 pinned_version；完成时再读一次最新版本，若期间有写入则标记
    stale（队列与分母仍严格使用钉住版本，绝不混入新版本）。
    """
    pinned = _latest_version(db)
    with db.transaction() as conn:
        cur = conn.execute(
            "INSERT INTO recompute_jobs(started_at, pinned_version, status, basis, note)"
            " VALUES (?,?, 'running', ?, ?)",
            (now_iso(), pinned, basis,
             f"后台重算已钉住版本 {pinned}（{basis} 口径）"),
        )
        job_id = int(cur.lastrowid)

    def worker() -> None:
        time.sleep(max(0.0, simulated_seconds))  # 模拟重算耗时，期间允许写入推进版本
        try:
            snapshot_id = save_snapshot(db, pinned, basis,
                                        f"重算任务 #{job_id} 产出（钉版 {pinned}）", job_id)
        except Exception as exc:  # pragma: no cover - 防御性
            snapshot_id = None
            note = f"重算失败：{exc}"
        else:
            note = f"重算完成，快照 #{snapshot_id}"
        latest = _latest_version(db)
        status = "done" if latest == pinned else "stale"
        with db.transaction() as conn:
            conn.execute(
                "UPDATE recompute_jobs SET finished_at = ?, latest_at_end = ?, status = ?,"
                " note = ? WHERE job_id = ?",
                (now_iso(), latest, status,
                 note + ("" if status == "done" else
                         f"；期间版本推进至 {latest}，本任务结果仍只反映版本 {pinned}，需重跑"),
                 job_id),
            )

    threading.Thread(target=worker, daemon=True).start()
    return {"job_id": job_id, "pinned_version": pinned, "status": "running"}


def list_jobs(db: Database) -> list[dict]:
    return db.fetchall_dicts(
        "SELECT * FROM recompute_jobs ORDER BY job_id DESC"
    )
