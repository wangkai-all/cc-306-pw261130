"""后端核心逻辑测试：双时态版本、三口径队列、拆分不确定、版本一致性。

运行：python -m unittest tests.test_backend -v
"""
from __future__ import annotations

import os
import tempfile
import unittest

from backend import ingest
from backend.db import Database
from backend.report import BASES, build_report, compute_state, drill_cell, version_diff
from backend.seed import seed
from backend.timeutil import parse_iso, week_key, week_start


class SeededFixture:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "test.db")
        self.db = Database(self.path)
        self.db.initialize()
        seed(self.db)

    def close(self):
        self.tmp.cleanup()


class SeededReportTests(unittest.TestCase):
    def setUp(self):
        self.fx = SeededFixture()
        self.db = self.fx.db
        self.conn = self.db.read_connection()

    def tearDown(self):
        self.conn.close()
        self.fx.close()

    def test_versions_monotonic_and_late_events_have_old_event_time(self):
        rows = self.db.fetchall_dicts("SELECT version, kind FROM versions ORDER BY version")
        versions = [r["version"] for r in rows]
        self.assertEqual(versions, list(range(1, len(versions) + 1)))
        late = self.db.fetchall_dicts(
            "SELECT event_time, ingest_version FROM events WHERE id LIKE 'evt_late_%'")
        self.assertGreaterEqual(len(late), 2)
        for ev in late:
            self.assertLess(parse_iso(ev["event_time"]),
                            parse_iso("2026-10-05T00:00:00Z"))
            self.assertGreater(ev["ingest_version"], 5)

    def test_three_bases_have_expected_ordering_and_visit_double_counts(self):
        reports = {b: build_report(self.conn, self.db.current_version(), b) for b in BASES}
        total = {b: r["totals"]["cohort_members"] for b, r in reports.items()}
        # 访问身份（节点级）人数 >= 实体数；账号口径排除无账号实体，通常最小
        self.assertGreaterEqual(total["visit"], total["entity"])
        self.assertGreater(total["visit"], total["account"])
        # 三口径事件可见数一致（差别只在归属/计数，不在可见性）
        self.assertEqual(len({r["totals"]["events_visible"] for r in reports.values()}), 1)

    def test_split_produces_uncertain_events(self):
        report = build_report(self.conn, self.db.current_version(), "entity")
        self.assertGreaterEqual(report["totals"]["uncertain_events"], 2)
        reasons = {ev["reason"] for item in report["uncertain_by_week"] for ev in item["events"]}
        self.assertTrue(any("split" in r for r in reasons))

    def test_lower_upper_bounds_when_uncertainty_exists(self):
        report = build_report(self.conn, self.db.current_version(), "entity")
        found = False
        for cohort in report["cohorts"]:
            for cell in cohort["cells"]:
                if cell["uncertain_event_count"] and cell["candidate_members"]:
                    found = True
                    self.assertIsNotNone(cell["lower_rate"])
                    self.assertGreaterEqual(cell["upper_rate"], cell["lower_rate"])
        self.assertTrue(found, "应至少有一个单元格因不确定事件产生区间")

    def test_asof_graph_reconstructable_and_merges_invisible_before_version(self):
        # 在最早的合并版本之前，anon_7 与 dev_1 必须是两个独立实体
        edge = self.db.fetchall_dicts(
            "SELECT MIN(from_version) AS v FROM id_edges WHERE reason LIKE '%指纹%'")[0]
        before = edge["v"] - 1
        state = compute_state(self.conn, before)
        self.assertNotEqual(state.graph.find("anon_7"), state.graph.find("dev_1"))
        after = compute_state(self.conn, self.db.current_version())
        self.assertEqual(after.graph.find("anon_7"), after.graph.find("dev_1"))
        # 拆分边仍可追溯：dev_4 / anon_9 在拆分版本后重新分开
        cur = compute_state(self.conn, self.db.current_version())
        self.assertNotEqual(cur.graph.find("dev_4"), cur.graph.find("anon_9"))

    def test_past_report_is_immutable_under_later_writes(self):
        v = 10
        before = build_report(self.conn, v, "entity")
        # 后续写入新事件
        ingest.ingest_event(self.db, "evt_immut_check", "2026-11-02T10:00:00Z",
                            "dev_6", None, ["dev_6"], "page_view")
        after = build_report(self.conn, v, "entity")
        self.assertEqual(before["cohorts"], after["cohorts"])
        self.assertEqual(before["totals"], after["totals"])

    def test_version_diff_detects_split_and_cohort_changes(self):
        latest = self.db.current_version()
        d = version_diff(self.conn, "entity", latest, latest - 1)
        self.assertIn(d["version_logs"][0]["kind"],
                      {"edge_close", "edge_open", "event_ingest", "node_birth"})
        # 全量 diff（v1 -> latest）必须能观察到 S3 的边关闭与 S1 的边打开
        full = version_diff(self.conn, "entity", latest, 1)
        self.assertTrue(full["graph_changes"]["edges_closed"])
        self.assertTrue(full["graph_changes"]["edges_opened"])
        self.assertTrue(full["cell_changes"] or full["cohort_changes"])

    def test_drill_cell_members_and_uncertain_candidates(self):
        report = build_report(self.conn, self.db.current_version(), "entity")
        cohort = report["cohorts"][0]
        data = drill_cell(self.conn, self.db.current_version(), "entity",
                          cohort["week_start"], cohort["cells"][0]["offset"])
        self.assertEqual(data["cohort_size"], cohort["cohort_size"])
        self.assertEqual(len(data["active_members"]) + len(data["inactive_members"]),
                         data["cohort_size"])

    def test_recompute_pins_version_and_detects_staleness(self):
        pinned = self.db.current_version()
        job = ingest.start_recompute(self.db, "entity", simulated_seconds=0.05)
        # 重算进行中推进版本
        ingest.ingest_event(self.db, "evt_during_recompute", "2026-11-03T08:00:00Z",
                            "dev_7", None, ["dev_7"], "click")
        import time
        time.sleep(0.25)
        jobs = {j["job_id"]: j for j in ingest.list_jobs(self.db)}
        finished = jobs[job["job_id"]]
        self.assertEqual(finished["pinned_version"], pinned)
        self.assertEqual(finished["status"], "stale")
        self.assertEqual(finished["latest_at_end"], self.db.current_version())
        # 快照内容必须停留在钉住版本
        snap = ingest.get_snapshot(self.db, finished["job_id"])
        self.assertEqual(snap["payload"]["version"], pinned)

    def test_account_basis_excludes_anonymous_only_entities(self):
        report = build_report(self.conn, self.db.current_version(), "account")
        for cohort in report["cohorts"]:
            for member in cohort["members"]:
                self.assertTrue(member.startswith("acc_"))

    def test_visit_basis_splits_same_person_across_devices(self):
        cur = compute_state(self.conn, self.db.current_version())
        # acc_1 实体在访问口径下应有多个成员（acc_1 / dev_1 / dev_5 / anon_7）
        root = cur.graph.find("acc_1")
        visit_members = {k for fw in (cur.first_week["visit"],) for k in fw}
        for nid in ("acc_1", "dev_1", "dev_5", "anon_7"):
            self.assertIn(nid, visit_members)
        entity_members = set(cur.first_week["entity"])
        self.assertIn(root, entity_members)


class IngestRuleTests(unittest.TestCase):
    def setUp(self):
        self.fx = SeededFixture()
        self.db = self.fx.db

    def tearDown(self):
        self.fx.close()

    def test_merge_guard_two_accounts(self):
        with self.assertRaises(ingest.IngestError):
            ingest.merge_identities(self.db, "acc_2", "acc_3", "应当被拒绝")

    def test_merge_guard_already_same_entity(self):
        with self.assertRaises(ingest.IngestError):
            ingest.merge_identities(self.db, "acc_1", "dev_1", "已经合并过")

    def test_split_without_edge_rejected(self):
        with self.assertRaises(ingest.IngestError):
            ingest.split_identities(self.db, "dev_2", "dev_3", "没有边可拆")

    def test_duplicate_event_id_rejected(self):
        with self.assertRaises(ingest.IngestError):
            ingest.ingest_event(self.db, "evt_late_dev1_w0", "2026-09-08T00:00:00Z",
                                "dev_1", None, ["dev_1"], "page_view")

    def test_resplit_creates_more_uncertainty_and_remerge_resolves(self):
        conn = self.db.read_connection()
        before = build_report(conn, self.db.current_version(), "entity")["totals"]["uncertain_events"]
        conn.close()
        # 新合并一对节点，制造无 emitter 事件，再拆
        ingest.merge_identities(self.db, "dev_6", "anon_12", "测试用临时合并")
        ingest.ingest_event(self.db, "evt_orphan_test", "2026-11-04T10:00:00Z",
                            None, None, ["dev_6", "anon_12"], "server_side_touch")
        ingest.split_identities(self.db, "dev_6", "anon_12", "测试用撤销")
        conn = self.db.read_connection()
        after = build_report(conn, self.db.current_version(), "entity")
        conn.close()
        self.assertGreater(after["totals"]["uncertain_events"], before)
        # 重新合并后该事件重新可归属
        ingest.merge_identities(self.db, "dev_6", "anon_12", "复核后重新合并")
        conn = self.db.read_connection()
        remerged = build_report(conn, self.db.current_version(), "entity")
        conn.close()
        ids = {ev["event_id"] for item in remerged["uncertain_by_week"] for ev in item["events"]}
        self.assertNotIn("evt_orphan_test", ids)


if __name__ == "__main__":
    unittest.main()
