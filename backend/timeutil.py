"""周历与时间工具。

系统统一用「业务周」（ISO 周，周一为起点）作为队列与留存的时间粒度。
所有时间戳都是 UTC ISO-8601 字符串，合成数据，不涉及真实用户。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime(ISO_FMT)


def parse_iso(s: str) -> datetime:
    return datetime.strptime(s, ISO_FMT).replace(tzinfo=timezone.utc)


def format_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(ISO_FMT)


def week_start(d: date) -> date:
    """返回 d 所在 ISO 周的周一。"""
    return d - timedelta(days=d.weekday())


def week_key(d: date) -> str:
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def week_key_of_ts(ts: str) -> str:
    return week_key(parse_iso(ts).date())


def add_weeks(d: date, n: int) -> date:
    return d + timedelta(weeks=n)


def week_diff(later_week: date, earlier_week: date) -> int:
    """两个周一间的整数周数差（later - earlier）。"""
    return (week_start(later_week) - week_start(earlier_week)).days // 7


def week_range(first: date, last: date) -> list[date]:
    """从 first 周到 last 周（含）的每周周一列表。"""
    first, last = week_start(first), week_start(last)
    out = []
    cur = first
    while cur <= last:
        out.append(cur)
        cur = add_weeks(cur, 1)
    return out
