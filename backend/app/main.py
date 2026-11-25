"""多维指标分析与实验评估平台 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8012
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.routers.chains import router as chains_router
from app.routers.domain import router as domain_router
from app.routers.reconciliation import aliases_router as reconciliation_aliases_router
from app.routers.reconciliation import router as reconciliation_router
from app.store import store

app = FastAPI(title="多维指标分析与实验评估平台", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# 业务链路路由
app.include_router(chains_router)

# 领域业务操作路由
app.include_router(domain_router)

# 空分区凭据、补算幂等和报告范围核对。同一服务暴露多个语义前缀。
app.include_router(reconciliation_aliases_router)
for prefix in (
    "/api/reconciliation",
    "/api/partition-audit",
    "/api/partition-reconciliation",
    "/api/domain/reconciliation",
    "/api/domain/partition-audit",
):
    app.include_router(reconciliation_router, prefix=prefix)

# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)
