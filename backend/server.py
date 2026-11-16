"""标准库 HTTP 服务：JSON API + 前端静态文件。

只读接口每次开新的读连接（WAL，无锁）；写接口走 ingest 模块的串行写事务。
"""
from __future__ import annotations

import csv
import io
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import ingest
from .db import Database
from .ingest import IngestError
from .report import BASES, BASIS_NOTES, build_report, drill_cell, version_diff
from .timeutil import week_key

FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "frontend")

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
}


def make_handler(db: Database) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "PointwiseRetention/1.0"

        def log_message(self, fmt, *args):  # 安静一点
            pass

        # ------------------------------------------------------------------ helpers
        def _send_json(self, obj, status: int = 200) -> None:
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_csv(self, text: str, filename: str) -> None:
            body = text.encode("utf-8-sig")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _handle_error(self, exc: Exception) -> None:
            status = 400 if isinstance(exc, (IngestError, KeyError, ValueError)) else 500
            if status >= 500:
                import traceback
                traceback.print_exc()
            self._send_json({"error": str(exc)}, status)

        def _read_conn(self):
            return db.read_connection()

        # ------------------------------------------------------------------ GET
        def do_GET(self) -> None:
            try:
                parsed = urlparse(self.path)
                path, qs = parsed.path, parse_qs(parsed.query)
                if path.startswith("/api/"):
                    self._route_get(path, qs)
                else:
                    self._serve_static(path)
            except Exception as exc:  # noqa: BLE001
                self._handle_error(exc)

        def _route_get(self, path: str, qs: dict) -> None:
            def q(name: str, default=None):
                return qs.get(name, [default])[0]

            if path == "/api/meta":
                self._send_json({
                    "bases": [{"id": b, "note": BASIS_NOTES[b]} for b in BASES],
                    "current_version": db.current_version(),
                })
            elif path == "/api/versions":
                rows = db.fetchall_dicts(
                    "SELECT version, created_at, kind, summary, detail_json "
                    "FROM versions ORDER BY version DESC LIMIT 200"
                )
                for r in rows:
                    r["detail"] = json.loads(r.pop("detail_json"))
                self._send_json({"versions": rows, "current_version": db.current_version()})
            elif path == "/api/report":
                basis = q("basis", "entity")
                version = int(q("version", db.current_version()))
                horizon = int(q("horizon", 6))
                conn = self._read_conn()
                try:
                    self._send_json(build_report(conn, version, basis, horizon))
                finally:
                    conn.close()
            elif path == "/api/diff":
                basis = q("basis", "entity")
                v_new = int(q("v_new", db.current_version()))
                v_old = int(q("v_old")) if "v_old" in qs else None
                conn = self._read_conn()
                try:
                    self._send_json(version_diff(conn, basis, v_new, v_old))
                finally:
                    conn.close()
            elif path == "/api/drill":
                conn = self._read_conn()
                try:
                    self._send_json(drill_cell(
                        conn, int(q("version", db.current_version())),
                        q("basis", "entity"), q("cohort_week"), int(q("offset"))))
                finally:
                    conn.close()
            elif path == "/api/export/matrix":
                self._export_matrix(q)
            elif path == "/api/export/drill":
                self._export_drill_csv(q)
            elif path == "/api/snapshots":
                self._send_json({"snapshots": ingest.list_snapshots(db)})
            elif path == "/api/snapshot":
                self._send_json(ingest.get_snapshot(db, int(q("id"))))
            elif path == "/api/jobs":
                self._send_json({"jobs": ingest.list_jobs(db)})
            else:
                self._send_json({"error": "未知接口"}, 404)

        def _export_matrix(self, q) -> None:
            basis = q("basis", "entity")
            version = int(q("version", db.current_version()))
            conn = self._read_conn()
            try:
                report = build_report(conn, version, basis)
            finally:
                conn.close()
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["# 点态留存矩阵", f"版本={version}", f"口径={basis}",
                        f"导出时间={ingest.now_iso()}"])
            w.writerow(["队列周", "队列人数"] +
                       [f"W{c['offset']}下界留存" for c in report["cohorts"][0]["cells"]] +
                       [f"W{c['offset']}上界留存" for c in report["cohorts"][0]["cells"]])
            for cohort in report["cohorts"]:
                lowers = [("" if c["lower_rate"] is None else c["lower_rate"])
                          for c in cohort["cells"]]
                uppers = [("" if c["upper_rate"] is None else c["upper_rate"])
                          for c in cohort["cells"]]
                w.writerow([cohort["week_key"], cohort["cohort_size"]] + lowers + uppers)
            w.writerow([])
            w.writerow(["# 不确定事件（拆分后无法唯一归属）"])
            w.writerow(["周", "事件数", "事件ID", "事件时间", "类型", "到达版本", "原因"])
            for item in report["uncertain_by_week"]:
                for ev in item["events"]:
                    w.writerow([item["week_key"], item["count"], ev["event_id"],
                                ev["event_time"], ev["event_type"],
                                ev["ingest_version"], ev["reason"]])
            self._send_csv(buf.getvalue(), f"retention_matrix_{basis}_v{version}.csv")

        def _export_drill_csv(self, q) -> None:
            conn = self._read_conn()
            try:
                data = drill_cell(conn, int(q("version", db.current_version())),
                                  q("basis", "entity"), q("cohort_week"), int(q("offset")))
            finally:
                conn.close()
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["# 单元格钻取", f"队列周={data['cohort_week_key']}",
                        f"目标周={data['target_week_key']}", f"版本={data['version']}"])
            w.writerow(["成员", "实体根", "是否活跃", "事件ID", "事件时间", "事件类型",
                        "发射节点", "到达版本", "归属依据"])
            for group in (data["active_members"], data["inactive_members"]):
                for m in group:
                    if not m["events"]:
                        w.writerow([m["member"], m["entity_root"], "是" if m["active"] else "否"])
                    for ev in m["events"]:
                        w.writerow([m["member"], m["entity_root"], "是" if m["active"] else "否",
                                    ev["event_id"], ev["event_time"], ev["event_type"],
                                    ev["emitter_id"], ev["ingest_version"],
                                    ev["allocation_reason"]])
            w.writerow([])
            w.writerow(["# 不确定事件候选成员"])
            for ev in data["uncertain_events"]:
                w.writerow([ev["event_id"], ev["event_time"], ev["event_type"],
                            ev["reason"], "候选:" + "|".join(ev["candidate_members"])])
            self._send_csv(buf.getvalue(),
                           f"drill_{data['cohort_week_key']}_{data['target_week_key']}.csv")

        # ------------------------------------------------------------------ POST
        def do_POST(self) -> None:
            try:
                path = urlparse(self.path).path
                body = self._read_json()
                if path == "/api/ingest/event":
                    self._send_json(ingest.ingest_event(
                        db, body["event_id"], body["event_time"],
                        body.get("emitter_id"), body.get("emitter_kind"),
                        body.get("attached_ids", []),
                        body.get("event_type", "page_view"),
                        body.get("payload"), body.get("summary")))
                elif path == "/api/identity/merge":
                    self._send_json(ingest.merge_identities(
                        db, body["a"], body["b"], body.get("reason", "人工合并")))
                elif path == "/api/identity/split":
                    self._send_json(ingest.split_identities(
                        db, body["a"], body["b"], body.get("reason", "人工撤销")))
                elif path == "/api/snapshot":
                    sid = ingest.save_snapshot(
                        db, int(body.get("version", db.current_version())),
                        body.get("basis", "entity"), body.get("label", "手动快照"))
                    self._send_json({"snapshot_id": sid})
                elif path == "/api/recompute":
                    self._send_json(ingest.start_recompute(
                        db, body.get("basis", "entity"),
                        float(body.get("simulated_seconds", 2.0))))
                else:
                    self._send_json({"error": "未知接口"}, 404)
            except Exception as exc:  # noqa: BLE001
                self._handle_error(exc)

        # ------------------------------------------------------------------ static
        def _serve_static(self, path: str) -> None:
            if path in ("/", ""):
                path = "/index.html"
            # 防目录穿越
            target = os.path.normpath(os.path.join(FRONTEND_DIR, path.lstrip("/")))
            if not target.startswith(FRONTEND_DIR) or not os.path.isfile(target):
                self.send_error(404)
                return
            ext = os.path.splitext(target)[1]
            with open(target, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPES.get(ext, "application/octet-stream"))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def serve(db: Database, host: str, port: int) -> None:
    httpd = ThreadingHTTPServer((host, port), make_handler(db))
    print(f"点态留存分析服务已启动: http://{host}:{port}")
    httpd.serve_forever()
