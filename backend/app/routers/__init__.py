"""业务路由汇总：重放数据包为专用路由，需排在通用资源路由之前避免动态路径抢先匹配。"""
from __future__ import annotations

from app.routers import replay, resource

ROUTERS = [replay, resource]
