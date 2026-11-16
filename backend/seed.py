"""确定性合成数据（全部虚构，不进行任何真实用户识别）。

脚本化推进版本，刻意覆盖五类场景：
  S1 匿名访客 → 设备 → 注册账号 的跨端合并（实体/账号/访问三口径差异最大）
  S2 普通单设备账号的完整留存生命周期
  S3 错误合并被撤销（split），挂载在两端的无 emitter 历史事件变成「不确定」
  S4 迟到事件：event_time 在很久以前，ingest_version 在很后面
  S5 永不注册的独立设备/匿名访问（访问口径独有）

重跑方式：删除数据库文件后执行 `python -m backend.seed`。
"""
from __future__ import annotations

import json
import os
import random
from datetime import timedelta

from . import ingest
from .db import Database
from .timeutil import add_weeks, format_iso, now_iso, parse_iso

RNG = random.Random(20261116)
EVENT_TYPES = ["page_view", "click", "add_to_cart", "checkout", "search"]

# 业务时间起点（周一，UTC）
T0 = "2026-09-07T09:00:00Z"


def _ts(base: str, week: int, day: int, hour: int, minute: int = 0) -> str:
    dt = parse_iso(base) + timedelta(weeks=week, days=day, hours=hour, minutes=minute)
    return format_iso(dt)


def seed(db: Database) -> dict:
    counter = {"events": 0}

    def ev(prefix: str) -> str:
        counter["events"] += 1
        return f"evt_{prefix}_{counter['events']:04d}"

    def emit(emitter: str | None, kind: str | None, week: int, day: int, hour: int,
             etype: str | None = None, attached: list[str] | None = None,
             late_note: str = "") -> str:
        eid = ev(late_note or "w" + str(week))
        ingest.ingest_event(
            db, eid, _ts(T0, week, day % 7, hour),
            emitter, kind, attached or ([emitter] if emitter else []),
            etype or RNG.choice(EVENT_TYPES),
            {"synthetic": True},
        )
        return eid

    def activity_stream(nodes: list[tuple[str, str]], weeks: range, p_active: float,
                        attached_factory=None) -> None:
        """为一组节点生成按周活动；attached_factory 可让事件带历史挂载身份。"""
        for w in weeks:
            for nid, kind in nodes:
                if RNG.random() > p_active:
                    continue
                n_ev = RNG.randint(1, 3)
                for _ in range(n_ev):
                    attached = attached_factory(nid) if attached_factory else [nid]
                    emit(nid, kind, w, RNG.randint(0, 6), RNG.randint(8, 21),
                         attached=attached)

    # ---------------------------------------------------------------- S1 跨端合并主线
    # v1..v3：anon_7 与 dev_1 各自的早期访问（此时系统不知道二者同人）
    emit("anon_7", "anonymous", 0, 1, 10)
    emit("anon_7", "anonymous", 0, 3, 15)
    emit("dev_1", "device", 0, 2, 11)
    emit("dev_1", "device", 0, 5, 20)
    # v5：同设备登录，匿名身份与设备合并
    ingest.merge_identities(db, "anon_7", "dev_1", "同设备指纹与登录态匹配")
    # v6：注册账号 acc_1 出生，挂到同一实体
    emit("acc_1", "account", 1, 0, 9, etype="account_signup",
         attached=["acc_1", "dev_1", "anon_7"])
    ingest.merge_identities(db, "acc_1", "dev_1", "注册后账号与设备绑定")
    # 合并后跨端活动持续 8 周
    activity_stream([("acc_1", "account"), ("dev_1", "device")],
                    range(1, 8), 0.75,
                    attached_factory=lambda nid: [nid, "dev_1", "anon_7"])

    # ---------------------------------------------------------------- S4 迟到事件
    # 一个发生在 W0 的设备事件，直到第 4 周才被回补（SDK 离线缓存上传）
    ingest.ingest_event(
        db, "evt_late_dev1_w0", _ts(T0, 0, 6, 23), "dev_1", None,
        ["dev_1", "anon_7", "acc_1"], "offline_buffer_flush",
        {"synthetic": True, "late": True},
        summary="迟到回补：dev_1 在 W0 的离线事件（第 4 周才到达）",
    )

    # ---------------------------------------------------------------- S2 普通单设备账号
    emit("dev_2", "device", 1, 1, 12)
    emit("acc_2", "account", 1, 2, 10, etype="account_signup", attached=["acc_2", "dev_2"])
    ingest.merge_identities(db, "acc_2", "dev_2", "注册绑定")
    activity_stream([("acc_2", "account")], range(2, 8), 0.6,
                    attached_factory=lambda nid: [nid, "dev_2"])
    emit("dev_3", "device", 2, 3, 14)
    emit("acc_3", "account", 2, 4, 9, etype="account_signup", attached=["acc_3", "dev_3"])
    ingest.merge_identities(db, "acc_3", "dev_3", "注册绑定")
    activity_stream([("acc_3", "account")], range(3, 8), 0.55,
                    attached_factory=lambda nid: [nid, "dev_3"])

    # ---------------------------------------------------------------- S3 错误合并 -> 撤销
    # dev_4 与 anon_9 在 W2 被错误地判为同一人
    emit("dev_4", "device", 2, 0, 13)
    emit("anon_9", "anonymous", 2, 2, 17)
    ingest.merge_identities(db, "dev_4", "anon_9", "机房 IP 段误判（待复核）")
    # 合并期间产生的活动同时挂载两个身份（无 emitter 的服务端回补事件，拆分后无法归属）
    ingest.ingest_event(
        db, "evt_orphan_1", _ts(T0, 2, 4, 18), None, None,
        ["dev_4", "anon_9"], "server_side_touch",
        {"synthetic": True}, summary="无 emitter 服务端触达，挂载 dev_4+anon_9",
    )
    emit("dev_4", "device", 2, 5, 19, attached=["dev_4", "anon_9"])
    emit("anon_9", "anonymous", 3, 1, 16, attached=["dev_4", "anon_9"])
    ingest.ingest_event(
        db, "evt_orphan_2", _ts(T0, 3, 3, 12), None, None,
        ["dev_4", "anon_9"], "server_side_touch",
        {"synthetic": True}, summary="无 emitter 服务端触达，挂载 dev_4+anon_9",
    )
    # W4：复核确认误判，撤销合并 -> 上面的无 emitter 事件进入不确定桶
    ingest.split_identities(db, "dev_4", "anon_9", "人工复核：IP 误判，撤销合并")
    # 拆分后两端各自活动
    activity_stream([("dev_4", "device")], range(4, 8), 0.5)
    activity_stream([("anon_9", "anonymous")], range(4, 8), 0.45)

    # 更多迟到：anon_9 的 W1 事件在拆分后（W5）才到达
    ingest.ingest_event(
        db, "evt_late_anon9_w1", _ts(T0, 1, 5, 8), "anon_9", None,
        ["anon_9"], "page_view", {"synthetic": True, "late": True},
        summary="迟到回补：anon_9 在 W1 的事件（拆分后才到达）",
    )

    # ---------------------------------------------------------------- S1 续：第二台设备并入 acc_1
    emit("dev_5", "device", 3, 2, 11)
    ingest.merge_identities(db, "dev_5", "acc_1", "第二台设备登录同一账号（跨端覆盖）")
    activity_stream([("dev_5", "device")], range(3, 8), 0.5,
                    attached_factory=lambda nid: [nid, "acc_1"])

    # ---------------------------------------------------------------- S5 永不注册的独立访问
    activity_stream([("dev_6", "device")], range(0, 6), 0.5)
    activity_stream([("anon_12", "anonymous")], range(0, 5), 0.4)
    emit("dev_7", "device", 4, 1, 10)
    emit("dev_7", "device", 5, 2, 11)
    emit("anon_15", "anonymous", 3, 4, 15)
    emit("anon_15", "anonymous", 5, 0, 9)

    # 另一组正常注册账号，丰富队列行数
    for idx, first_week in enumerate((4, 5, 6), start=4):
        dev, acc = f"dev_{idx + 4}", f"acc_{idx}"
        emit(dev, "device", first_week, 1, 11)
        emit(acc, "account", first_week, 2, 10, etype="account_signup",
             attached=[acc, dev])
        ingest.merge_identities(db, acc, dev, "注册绑定")
        activity_stream([(acc, "account")], range(first_week + 1, 8), 0.5,
                        attached_factory=lambda nid, d=dev: [nid, d])

    return {"events": counter["events"], "final_version": db.current_version()}


def main(db_path: str) -> None:
    if os.path.exists(db_path):
        raise SystemExit(f"数据库已存在：{db_path}，请先删除再 seed")
    db = Database(db_path)
    db.initialize()
    result = seed(db)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.getcwd(), "data", "retention.db"))
