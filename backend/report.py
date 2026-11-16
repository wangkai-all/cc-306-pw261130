"""纯计算层：在任意版本快照上构造身份图并计算留存报告。

关键约定
========
1. 「截至版本 v 的已知世界」：
   - 节点：born_version <= v
   - 身份边：from_version <= v 且 (to_version IS NULL 或 to_version > v)
   - 事件：ingest_version <= v（迟到事件在它真正到达的版本之前不可见）
2. 三种队列口径（basis）：
   - entity: 稳定实体 = 身份图连通分量（合并后的 DSU 根）
   - account: 注册账号 = 含账号节点的分量，以账号 id 为成员键；无账号分量不计入
   - visit:  访问身份 = 单个身份节点（设备/匿名/账号节点各自独立），最贴近「当时看到的访问」
3. 不确定事件：拆分（边关闭）后，到达时挂载在多个如今已分开的节点上、且没有 emitter 的
   历史事件无法再唯一归属，进入 uncertain 桶；留存率因此给出下界/上界区间而非单值。
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field

from .timeutil import add_weeks, parse_iso, week_key, week_range, week_start

BASES = ("entity", "account", "visit")

BASIS_NOTES = {
    "entity": "稳定实体口径：按当前身份图的连通分量合并计数，跨端覆盖最完整；"
              "错误合并会低估人数，拆分后部分历史事件可能无法归属。",
    "account": "注册账号口径：只统计含账号的实体，分母明确、可复算；"
               "未注册访问被排除，一个实体多账号的合并被写入侧禁止。",
    "visit": "访问身份口径：每个设备/匿名/账号节点各自计数，最贴近系统当时所见，"
             "无回溯改写；同一人多端会重复计数，跨端覆盖为零。",
}


# --------------------------------------------------------------------------------------
# 版本 v 上的身份图（DSU）
# --------------------------------------------------------------------------------------

@dataclass
class IdentityGraph:
    version: int
    parent: dict[str, str]
    nodes: dict[str, dict]                       # id -> {id, kind, born_version, born_event_time}
    root_members: dict[str, set[str]] = field(default_factory=dict)   # root -> 节点集合
    root_account: dict[str, str | None] = field(default_factory=dict)  # root -> 账号 id 或 None

    def find(self, x: str) -> str:
        parent = self.parent
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def account_of(self, root: str) -> str | None:
        return self.root_account.get(root)


def build_graph(conn: sqlite3.Connection, version: int) -> IdentityGraph:
    nodes = {
        r["id"]: dict(r)
        for r in conn.execute(
            "SELECT id, kind, born_version, born_event_time FROM id_nodes WHERE born_version <= ?",
            (version,),
        ).fetchall()
    }
    parent = {nid: nid for nid in nodes}

    def union(a: str, b: str) -> None:
        ra, rb = parent[a], parent[b]
        # 路径较短者挂到较长者，保持确定性
        if ra == rb:
            return
        if ra > rb:
            ra, rb = rb, ra
        parent[rb] = ra
        for k in list(parent):
            if parent[k] == rb:
                parent[k] = ra

    edges = conn.execute(
        """SELECT a, b, from_version, to_version, reason FROM id_edges
           WHERE from_version <= ? AND (to_version IS NULL OR to_version > ?)
           ORDER BY id""",
        (version, version),
    ).fetchall()
    for e in edges:
        if e["a"] in parent and e["b"] in parent:
            union(e["a"], e["b"])

    # 完全路径压缩后再聚合
    root_members: dict[str, set[str]] = {}
    root_account: dict[str, str | None] = {}
    for nid in nodes:
        r = parent[nid] = _root(parent, nid)
        root_members.setdefault(r, set()).add(nid)
        if nodes[nid]["kind"] == "account":
            # 写入侧保证一个分量至多一个账号
            root_account[r] = nid
        root_account.setdefault(r, None)
    return IdentityGraph(version, parent, nodes, root_members, root_account)


def _root(parent: dict[str, str], x: str) -> str:
    while parent[x] != x:
        parent[x] = parent[parent[x]]
        x = parent[x]
    return x


# --------------------------------------------------------------------------------------
# 事件归属
# --------------------------------------------------------------------------------------

@dataclass
class Allocation:
    event: dict
    root: str | None                 # 可归属时的实体根；不确定为 None
    allocatable: bool
    candidate_roots: set[str]        # 不确定事件可能涉及的实体根（拆分后的候选分量）
    reason: str


def allocate_event(event: dict, graph: IdentityGraph) -> Allocation:
    emitter = event["emitter_id"]
    attached = json.loads(event["attached_ids"])
    if emitter:
        # 有明确发射节点：拆分也不影响归属（事件本来就是这个节点产生的）
        return Allocation(event, graph.find(emitter), True, set(), "emitter")
    # 无 emitter 的回补事件：靠到达时的挂载集合归属
    roots = {graph.find(nid) for nid in attached if nid in graph.parent}
    if len(roots) == 1:
        return Allocation(event, next(iter(roots)), True, roots, "attached_singleton")
    if len(roots) == 0:
        return Allocation(event, None, False, set(), "no_live_attachment")
    return Allocation(event, None, False, roots, f"split_over_{len(roots)}_entities")


def _member_key(basis: str, alloc: Allocation, graph: IdentityGraph) -> str | None:
    """按口径把一次已归属事件映射到队列成员键。"""
    if not alloc.allocatable or alloc.root is None:
        return None
    if basis == "entity":
        return alloc.root
    if basis == "account":
        return graph.account_of(alloc.root)       # 无账号分量 -> None（该口径排除）
    if basis == "visit":
        emitter = alloc.event["emitter_id"]
        if emitter:
            return emitter
        # 挂载到单节点分量的无 emitter 事件，归该唯一节点
        members = graph.root_members.get(alloc.root, set())
        return next(iter(members)) if len(members) == 1 else None
    raise ValueError(f"unknown basis {basis}")


# --------------------------------------------------------------------------------------
# 留存矩阵
# --------------------------------------------------------------------------------------

@dataclass
class Computed:
    graph: IdentityGraph
    allocations: list[Allocation]
    # basis -> member_key -> 首次活动周（周一 date）
    first_week: dict[str, dict[str, date]]
    # basis -> 周一起 -> 活跃成员键集合
    active_weeks: dict[str, dict[str, set[str]]]
    # 周一起 -> 不确定事件列表
    uncertain_week: dict[str, list[dict]]
    # 周一起 -> 不确定事件候选根集合
    candidate_roots_week: dict[str, set[str]]


def _load_events(conn: sqlite3.Connection, version: int) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            """SELECT id, event_time, ingest_version, emitter_id, attached_ids,
                      event_type, payload_json
               FROM events WHERE ingest_version <= ? ORDER BY event_time, id""",
            (version,),
        ).fetchall()
    ]


def compute_state(conn: sqlite3.Connection, version: int) -> Computed:
    graph = build_graph(conn, version)
    events = _load_events(conn, version)
    allocations = [allocate_event(ev, graph) for ev in events]

    first_week = {b: {} for b in BASES}
    active_weeks: dict[str, dict[str, set[str]]] = {b: {} for b in BASES}
    uncertain_week: dict[str, list[dict]] = {}
    candidate_roots_week: dict[str, set[str]] = {}

    for alloc in allocations:
        wk_start = week_start(parse_iso(alloc.event["event_time"]).date())
        if not alloc.allocatable:
            bucket = uncertain_week.setdefault(wk_start, [])
            bucket.append({
                "event_id": alloc.event["id"],
                "event_time": alloc.event["event_time"],
                "event_type": alloc.event["event_type"],
                "ingest_version": alloc.event["ingest_version"],
                "attached_ids": json.loads(alloc.event["attached_ids"]),
                "reason": alloc.reason,
            })
            candidate_roots_week.setdefault(wk_start, set()).update(alloc.candidate_roots)
            continue
        for basis in BASES:
            key = _member_key(basis, alloc, graph)
            if key is None:
                continue
            fw = first_week[basis].get(key)
            if fw is None or wk_start < fw:
                first_week[basis][key] = wk_start
            active_weeks[basis].setdefault(wk_start, set()).add(key)

    return Computed(graph, allocations, first_week, active_weeks,
                    uncertain_week, candidate_roots_week)


def build_report(conn: sqlite3.Connection, version: int, basis: str,
                 horizon: int = 6) -> dict:
    if basis not in BASES:
        raise ValueError(f"unknown basis {basis}")
    latest = conn.execute("SELECT COALESCE(MAX(version),0) AS v FROM versions").fetchone()["v"]
    version = min(version, latest)
    state = compute_state(conn, version)
    fw = state.first_week[basis]
    active = state.active_weeks[basis]

    if fw:
        span = week_range(min(fw.values()), max(fw.values()))
    else:
        span = []

    # member_key -> 实体根，用于把不确定事件的候选根映射到队列成员
    key_to_root: dict[str, str] = {}
    for root, members in state.graph.root_members.items():
        if basis == "entity":
            key_to_root[root] = root
        elif basis == "account":
            acc = state.graph.account_of(root)
            if acc:
                key_to_root[acc] = root
        else:
            for m in members:
                key_to_root[m] = root

    cohorts = []
    for cw in span:
        members = sorted(k for k, w in fw.items() if w == cw)
        cohort_set = set(members)
        cells = []
        for offset in range(horizon + 1):
            aw = add_weeks(cw, offset)
            aw_key = week_key(aw)
            active_here = active.get(aw, set()) & cohort_set
            candidate_roots = state.candidate_roots_week.get(aw, set())
            # 候选成员：所在实体是该周不确定事件的候选分量、且本周尚未确认活跃的队列成员
            candidate_members = sorted(
                k for k in cohort_set
                if k not in active_here and key_to_root.get(k) in candidate_roots
            )
            uncertain_events = state.uncertain_week.get(aw, [])
            size = len(cohort_set)
            lower = len(active_here)
            upper = min(size, lower + len(candidate_members)) if size else 0
            cells.append({
                "offset": offset,
                "week_key": aw_key,
                "cohort_size": size,
                "active": lower,
                "candidate_members": candidate_members,
                "uncertain_event_count": len(uncertain_events),
                "lower_rate": round(lower / size, 4) if size else None,
                "upper_rate": round(upper / size, 4) if size else None,
            })
        cohorts.append({
            "week_key": week_key(cw),
            "week_start": cw.isoformat(),
            "cohort_size": len(members),
            "members": members,
            "cells": cells,
        })

    uncertain_summary = [
        {
            "week_key": week_key(w),
            "week_start": w.isoformat(),
            "count": len(evs),
            "events": evs,
        }
        for w, evs in sorted(state.uncertain_week.items())
    ]

    return {
        "version": version,
        "latest_version": latest,
        "basis": basis,
        "basis_note": BASIS_NOTES[basis],
        "horizon": horizon,
        "weeks": [{"week_key": week_key(w), "week_start": w.isoformat()} for w in span],
        "cohorts": cohorts,
        "uncertain_by_week": uncertain_summary,
        "totals": {
            "nodes": len(state.graph.nodes),
            "entities": len(state.graph.root_members),
            "events_visible": len(state.allocations),
            "uncertain_events": sum(len(v) for v in state.uncertain_week.values()),
            "cohort_members": len(fw),
        },
    }


# --------------------------------------------------------------------------------------
# 单元格钻取
# --------------------------------------------------------------------------------------

def drill_cell(conn: sqlite3.Connection, version: int, basis: str,
               cohort_week_start: str, offset: int) -> dict:
    report = build_report(conn, version, basis)
    cw = week_start(parse_iso(cohort_week_start + "T00:00:00Z").date())
    target_week = add_weeks(cw, offset)
    state = compute_state(conn, version)
    fw = state.first_week[basis]
    cohort_members = sorted(k for k, w in fw.items() if w == cw)
    cohort_set = set(cohort_members)

    active_rows, inactive_rows = [], []
    for key in cohort_members:
        member_events = []
        for alloc in state.allocations:
            if not alloc.allocatable:
                continue
            if _member_key(basis, alloc, state.graph) != key:
                continue
            ew = week_start(parse_iso(alloc.event["event_time"]).date())
            if ew == target_week:
                member_events.append({
                    "event_id": alloc.event["id"],
                    "event_time": alloc.event["event_time"],
                    "event_type": alloc.event["event_type"],
                    "emitter_id": alloc.event["emitter_id"],
                    "ingest_version": alloc.event["ingest_version"],
                    "allocation_reason": alloc.reason,
                })
        row = {
            "member": key,
            "entity_root": _root_of_key(state, basis, key),
            "active": bool(member_events),
            "events": sorted(member_events, key=lambda e: e["event_time"]),
        }
        (active_rows if row["active"] else inactive_rows).append(row)

    uncertain_here = []
    for ev in state.uncertain_week.get(target_week, []):
        candidate_keys = sorted(
            k for k in cohort_members
            if _root_of_key(state, basis, k) in {state.graph.find(n) for n in ev["attached_ids"]
                                                 if n in state.graph.parent}
        )
        uncertain_here.append({**ev, "candidate_members": candidate_keys})

    return {
        "version": version,
        "basis": basis,
        "cohort_week_key": week_key(cw),
        "target_week_key": week_key(target_week),
        "cohort_size": len(cohort_members),
        "active_members": active_rows,
        "inactive_members": inactive_rows,
        "uncertain_events": uncertain_here,
    }


def _root_of_key(state: Computed, basis: str, key: str) -> str | None:
    g = state.graph
    if basis == "entity":
        return key if key in g.root_members else None
    if basis == "account":
        return g.find(key) if key in g.parent else None
    return g.find(key) if key in g.parent else None


# --------------------------------------------------------------------------------------
# 版本差异：v_old -> v_new（默认 old = new-1）
# --------------------------------------------------------------------------------------

def version_diff(conn: sqlite3.Connection, basis: str, v_new: int,
                 v_old: int | None = None) -> dict:
    latest = conn.execute("SELECT COALESCE(MAX(version),0) AS v FROM versions").fetchone()["v"]
    v_new = min(v_new, latest)
    v_old = latest - 1 if v_old is None else v_old
    if v_old > v_new:
        v_old, v_new = v_new, v_old

    old_r = build_report(conn, v_old, basis)
    new_r = build_report(conn, v_new, basis)
    old_state = compute_state(conn, v_old)
    new_state = compute_state(conn, v_new)

    version_logs = [
        dict(r) for r in conn.execute(
            "SELECT version, kind, summary, detail_json FROM versions "
            "WHERE version > ? AND version <= ? ORDER BY version", (v_old, v_new))
    ]
    for log in version_logs:
        log["detail"] = json.loads(log.pop("detail_json"))

    # 队列成员首次活动周的变化（迟到事件 / 归属改写都可能改动）
    old_fw, new_fw = old_state.first_week[basis], new_state.first_week[basis]
    cohort_changes = []
    for key in sorted(set(old_fw) | set(new_fw)):
        ow = old_fw.get(key)
        nw = new_fw.get(key)
        if ow != nw:
            cohort_changes.append({
                "member": key,
                "old_first_week": week_key(ow) if ow else None,
                "new_first_week": week_key(nw) if nw else None,
                "change": "new_member" if ow is None else
                          ("removed" if nw is None else "reassigned"),
            })

    # 矩阵单元格变化
    cell_changes = []
    old_cells = {(c["week_key"], cell["offset"]): cell
                 for c in old_r["cohorts"] for cell in c["cells"]}
    for c in new_r["cohorts"]:
        for cell in c["cells"]:
            prev = old_cells.get((c["week_key"], cell["offset"]))
            if not prev:
                continue
            if prev["active"] != cell["active"] or prev["cohort_size"] != cell["cohort_size"] \
                    or prev["uncertain_event_count"] != cell["uncertain_event_count"]:
                cell_changes.append({
                    "cohort_week": c["week_key"],
                    "offset": cell["offset"],
                    "target_week": cell["week_key"],
                    "old_cohort_size": prev["cohort_size"],
                    "new_cohort_size": cell["cohort_size"],
                    "old_active": prev["active"],
                    "new_active": cell["active"],
                    "old_uncertain": prev["uncertain_event_count"],
                    "new_uncertain": cell["uncertain_event_count"],
                })

    # 身份图结构变化
    graph_change_set = _graph_changes(conn, v_old, v_new)

    return {
        "basis": basis,
        "v_old": v_old,
        "v_new": v_new,
        "version_logs": version_logs,
        "graph_changes": graph_change_set,
        "cohort_changes": cohort_changes,
        "cell_changes": cell_changes,
        "uncertain_total_old": old_r["totals"]["uncertain_events"],
        "uncertain_total_new": new_r["totals"]["uncertain_events"],
    }


def _graph_changes(conn: sqlite3.Connection, v_old: int, v_new: int) -> dict:
    opened, closed = [], []
    for r in conn.execute(
        """SELECT a, b, from_version, to_version, reason FROM id_edges
           WHERE from_version > ? AND from_version <= ?
              OR to_version > ? AND to_version <= ?""",
        (v_old, v_new, v_old, v_new),
    ).fetchall():
        if v_old < r["from_version"] <= v_new:
            opened.append({"a": r["a"], "b": r["b"], "version": r["from_version"],
                           "reason": r["reason"]})
        if r["to_version"] and v_old < r["to_version"] <= v_new:
            closed.append({"a": r["a"], "b": r["b"], "version": r["to_version"],
                           "reason": r["reason"]})
    born = [
        dict(r) for r in conn.execute(
            "SELECT id, kind, born_version, born_event_time FROM id_nodes "
            "WHERE born_version > ? AND born_version <= ? ORDER BY id", (v_old, v_new))
    ]
    return {"nodes_born": born, "edges_opened": opened, "edges_closed": closed}
