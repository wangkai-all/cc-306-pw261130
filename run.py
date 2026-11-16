"""项目入口：初始化（如需）+ 灌入合成数据（如需）+ 启动 Web 服务。

用法：
  python run.py                 # 默认 data/retention.db，自动 seed
  python run.py --reseed        # 删除旧库重新生成
  python run.py --port 8080
"""
from __future__ import annotations

import argparse
import os

from backend.db import Database
from backend.seed import seed
from backend.server import serve


def main() -> None:
    parser = argparse.ArgumentParser(description="点态留存分析（虚构数据，本地运行）")
    parser.add_argument("--db", default=os.path.join("data", "retention.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reseed", action="store_true", help="删除旧库并重新生成合成数据")
    args = parser.parse_args()

    if args.reseed and os.path.exists(args.db):
        for suffix in ("", "-wal", "-shm"):
            p = args.db + suffix
            if os.path.exists(p):
                os.remove(p)

    os.makedirs(os.path.dirname(args.db) or ".", exist_ok=True)
    db = Database(args.db)
    db.initialize()
    if db.current_version() == 0:
        result = seed(db)
        print(f"合成数据就绪：{result['events']} 个事件，版本推进至 {result['final_version']}")
    else:
        print(f"使用现有数据库（当前版本 {db.current_version()}）")
    serve(db, args.host, args.port)


if __name__ == "__main__":
    main()
