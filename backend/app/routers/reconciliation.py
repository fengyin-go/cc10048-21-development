"""分区凭据、补算幂等和报告范围核对的 HTTP 接口。

同一组相对路由会挂载到多个语义前缀下，兼容不同调用习惯：
``/api/reconciliation``、``/api/partition-audit``、
``/api/domain/reconciliation`` 等。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.services.reconciliation import reconciliation

router = APIRouter(tags=["分区凭据与补算核对"])
aliases_router = APIRouter(tags=["分区凭据领域别名"])


async def _payload(request: Request) -> dict[str, Any]:
    """合并 query/form/json 参数；GET 链接也能驱动演示流程。"""
    data: dict[str, Any] = dict(request.query_params)
    regions = request.query_params.getlist("regions") or request.query_params.getlist("地区")
    if regions:
        data["regions"] = regions
    if request.method in {"POST", "PUT", "PATCH"}:
        content_type = request.headers.get("content-type", "")
        try:
            if "application/json" in content_type:
                body = await request.json()
                if isinstance(body, dict):
                    data.update(body)
            elif "form" in content_type:
                form = await request.form()
                data.update(dict(form))
            else:
                # 测试客户端有时以 JSON 提交但省略 content-type。
                body = await request.json()
                if isinstance(body, dict):
                    data.update(body)
        except Exception:
            pass
    return data


def _response(result: dict[str, Any]) -> JSONResponse | dict[str, Any]:
    status = int(result.get("http_status", 200)) if isinstance(result, dict) else 200
    if status >= 400:
        return JSONResponse(status_code=status, content=result)
    return result


# ----------------------------------------------------------------------
# 场景准备/一键演练
# ----------------------------------------------------------------------
@router.api_route("/scenario/prepare", methods=["GET", "POST"])
async def prepare_scenario(request: Request):
    payload = await _payload(request)
    return reconciliation.prepare_scenario(payload)


@router.api_route("/scenario/run", methods=["GET", "POST"])
@router.api_route("/scenario/execute", methods=["GET", "POST"])
async def run_scenario(request: Request):
    payload = await _payload(request)
    return reconciliation.run_scenario_script(payload)


@router.get("/scenario")
@router.get("/state")
@router.get("/")
async def scenario_state():
    return reconciliation.get_state()


# ----------------------------------------------------------------------
# 分区记录、来源凭据和校验
# ----------------------------------------------------------------------
@router.get("/partition-records")
@router.get("/records")
async def partition_records():
    return {"items": reconciliation.all_records(), "total": len(reconciliation.all_records())}


@router.get("/empty-partitions")
async def empty_partitions(include_unconfirmed: bool = True):
    items = reconciliation.empty_partition_records(include_unconfirmed=include_unconfirmed)
    return {"items": items, "total": len(items)}


@router.get("/source-reads")
@router.get("/sources")
@router.get("/credentials")
async def source_reads():
    items = reconciliation.source_reads()
    return {"items": items, "total": len(items)}


@router.api_route("/source-reads/record", methods=["GET", "POST"])
async def record_source_read(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.record_source_read(payload))


@router.api_route("/aggregate-jobs/{job_id}/validate", methods=["GET", "POST"])
@router.api_route("/aggregate-jobs/{job_id}/validation", methods=["GET", "POST"])
@router.api_route("/jobs/{job_id}/validate", methods=["GET", "POST"])
async def validate_job(job_id: int, request: Request):
    payload = await _payload(request)
    return reconciliation.validate_partitions(job_id=job_id, period=payload.get("period") or payload.get("date"))


# ----------------------------------------------------------------------
# 补算
# ----------------------------------------------------------------------
@router.api_route("/backfills/submit", methods=["GET", "POST"])
async def submit_backfill_alias(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.submit_backfill(payload))


@router.post("/backfills")
async def create_backfill(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.submit_backfill(payload))


@router.get("/backfills")
async def list_backfills():
    return reconciliation.list_backfills()


@router.get("/backfills/{backfill_id}")
async def get_backfill(backfill_id: str):
    item = reconciliation.get_backfill(backfill_id)
    if item is None:
        return JSONResponse(status_code=404, content={"ok": False, "error": "补算单不存在"})
    return item


@router.api_route("/backfills/{backfill_id}/complete", methods=["GET", "POST"])
@router.api_route("/backfills/{backfill_id}/execute", methods=["GET", "POST"])
async def complete_backfill(backfill_id: str, request: Request):
    payload = await _payload(request)
    return _response(reconciliation.complete_backfill(backfill_id, payload))


@router.api_route("/backfills/complete-by-region", methods=["GET", "POST"])
async def complete_backfill_by_region(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.complete_backfill(payload.get("backfill_id"), payload))


# ----------------------------------------------------------------------
# 指标报告与对照
# ----------------------------------------------------------------------
@router.api_route("/reports/query", methods=["GET", "POST"])
@router.api_route("/metric-report", methods=["GET", "POST"])
@router.api_route("/daily-report", methods=["GET", "POST"])
async def query_report(request: Request):
    payload = await _payload(request)
    query_id = payload.get("query_id", 1)
    try:
        query_id = int(query_id)
    except (TypeError, ValueError):
        query_id = 1
    return _response(reconciliation.create_report(query_id, payload))


@router.post("/reports")
async def create_report(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.create_report(payload.get("query_id", 1), payload))


@router.get("/reports")
async def list_reports():
    return reconciliation.list_reports()


@router.api_route("/reports/compare", methods=["GET", "POST"])
async def compare_reports(request: Request):
    payload = await _payload(request)
    first = payload.get("first_report_id") or payload.get("first") or request.query_params.get("first")
    second = payload.get("second_report_id") or payload.get("second") or request.query_params.get("second")
    result = reconciliation.compare_reports(first, second)
    return _response(result)


@router.get("/reports/{report_id}")
async def get_report(report_id: str):
    report = reconciliation.get_report(report_id)
    if report is None:
        return JSONResponse(status_code=404, content={"ok": False, "error": "报告不存在"})
    return report


@router.get("/reports/{first_id}/compare/{second_id}")
async def compare_by_path(first_id: str, second_id: str):
    return _response(reconciliation.compare_reports(first_id, second_id))


@router.api_route("/reports/{report_id}/publish", methods=["GET", "POST"])
async def publish_report(report_id: str, request: Request):
    payload = await _payload(request)
    return _response(reconciliation.publish_report(report_id, payload))


@router.api_route("/publish", methods=["GET", "POST"])
async def publish_from_payload(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.publish_report(payload.get("report_id"), payload))


@router.get("/publications")
async def publications():
    return reconciliation.list_publications()


# ----------------------------------------------------------------------
# 现有领域 URL 风格的显式别名
# ----------------------------------------------------------------------
@aliases_router.api_route(
    "/api/domain/aggregate_job/{job_id}/validate-partitions",
    methods=["GET", "POST"],
)
@aliases_router.api_route(
    "/api/domain/aggregate_job/{job_id}/partition-coverage",
    methods=["GET", "POST"],
)
async def alias_validate_job(job_id: int, request: Request):
    payload = await _payload(request)
    return reconciliation.validate_partitions(job_id, payload.get("period") or payload.get("date"))


@aliases_router.get("/api/domain/aggregate_job/{job_id}/partition-records")
async def alias_job_records(job_id: int):
    validation = reconciliation.validate_partitions(job_id)
    return validation


@aliases_router.api_route(
    "/api/domain/dim_query/{query_id}/daily-report",
    methods=["GET", "POST"],
)
@aliases_router.api_route(
    "/api/domain/dim_query/{query_id}/current-report",
    methods=["GET", "POST"],
)
async def alias_daily_report(query_id: int, request: Request):
    payload = await _payload(request)
    return _response(reconciliation.create_report(query_id, payload))


@aliases_router.get("/api/domain/dim_query/{query_id}/reports")
async def alias_query_reports(query_id: int):
    return reconciliation.list_reports()


@aliases_router.api_route(
    "/api/domain/dim_query/{query_id}/backfills",
    methods=["GET", "POST"],
)
async def alias_query_backfills(query_id: int, request: Request):
    payload = await _payload(request)
    payload.setdefault("query_id", query_id)
    if request.method == "GET" and "region" in payload:
        return _response(reconciliation.submit_backfill(payload))
    return reconciliation.list_backfills()


@aliases_router.api_route("/api/domain/backfills", methods=["GET", "POST"])
@aliases_router.api_route("/api/domain/backfill", methods=["GET", "POST"])
async def alias_backfills(request: Request):
    payload = await _payload(request)
    if request.method == "GET" and not payload:
        return reconciliation.list_backfills()
    if payload.get("backfill_id") and (payload.get("value") is not None or payload.get("complete")):
        return _response(reconciliation.complete_backfill(str(payload["backfill_id"]), payload))
    if payload.get("action") == "complete" or payload.get("completion_value") is not None:
        return _response(reconciliation.complete_backfill(payload.get("backfill_id"), payload))
    return _response(reconciliation.submit_backfill(payload))


@aliases_router.api_route("/api/domain/backfills/{backfill_id}/complete", methods=["GET", "POST"])
async def alias_complete(backfill_id: str, request: Request):
    payload = await _payload(request)
    return _response(reconciliation.complete_backfill(backfill_id, payload))


@aliases_router.api_route("/api/domain/reports/compare", methods=["GET", "POST"])
async def alias_compare(request: Request):
    payload = await _payload(request)
    first = payload.get("first_report_id") or payload.get("first") or request.query_params.get("first")
    second = payload.get("second_report_id") or payload.get("second") or request.query_params.get("second")
    return _response(reconciliation.compare_reports(first, second))


@aliases_router.api_route("/api/domain/report_schedule/{task_id}/publish-report", methods=["GET", "POST"])
async def alias_schedule_publish(task_id: int, request: Request):
    payload = await _payload(request)
    result = reconciliation.publish_report(payload.get("report_id"), payload)
    result["report_task_id"] = task_id
    return _response(result)


# 常见 REST 资源名的顶层别名，避免调用方必须猜测 reconciliation 前缀。
@aliases_router.get("/api/partition-execution-records")
@aliases_router.get("/api/partition_execution_records")
@aliases_router.get("/api/empty-partition-records")
@aliases_router.get("/api/empty_partition_records")
async def alias_top_level_records():
    return {
        "items": reconciliation.all_records(),
        "total": len(reconciliation.all_records()),
        "empty_partition_records": reconciliation.empty_partition_records(),
    }


@aliases_router.api_route("/api/backfill-orders", methods=["GET", "POST"])
@aliases_router.api_route("/api/backfill_orders", methods=["GET", "POST"])
async def alias_top_level_backfills(request: Request):
    payload = await _payload(request)
    if request.method == "GET" and not payload:
        return reconciliation.list_backfills()
    return _response(reconciliation.submit_backfill(payload))


@aliases_router.get("/api/metric-reports")
@aliases_router.get("/api/metric_reports")
async def alias_top_level_reports():
    return reconciliation.list_reports()


@aliases_router.api_route("/api/daily-report", methods=["GET", "POST"])
@aliases_router.api_route("/api/current-report", methods=["GET", "POST"])
async def alias_top_level_report(request: Request):
    payload = await _payload(request)
    return _response(reconciliation.create_report(payload.get("query_id", 1), payload))
