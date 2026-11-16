"""点态留存分析（pointwise retention）后端包。

模块划分：
- timeutil: 周历与 ISO 时间工具
- db:       SQLite 连接与版本表结构（双时态：event_time + ingest_version）
- report:   纯计算：任意版本快照下的身份图、队列、留存矩阵、版本 diff
- ingest:   写入侧：事件回补、身份合并/拆分、快照、后台重算
- server:   标准库 HTTP 服务，提供 JSON API 与静态页面
"""
