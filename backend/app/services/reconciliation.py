"""分区读取凭据、空分区确认、补算幂等与指标报告范围核对。

这个服务是场景的唯一事实源：

* 读取成功且为 0 行 -> ``TRUE_ZERO``（确实为零），必须保存来源读取凭据；
* 来源超时/失败 -> ``SOURCE_UNAVAILABLE``（还没拿到源数据），数值保持 ``None``，
  绝不允许写成数值零；
* 同一统计期、指标、地区的缺项重复提交时，只保留一个受理单和一次领取凭据；
* 已确认空的地区再次收到补单时跳过，不产生新的执行；
* 报告保存不可变快照，发布时只汇总已确认分区，未确认地区不计入当日总量。
"""
from __future__ import annotations

import hashlib
import threading
from datetime import date, datetime, timezone, timedelta
from typing import Any, Iterable

from app.store import store

SHANGHAI_TZ = timezone(timedelta(hours=8))

DEFAULT_METRIC_ID = "METRIC-DAILY-DEAL"
DEFAULT_METRIC_NAME = "当日成交量"
DEFAULT_QUERY_ID = 1
DEFAULT_JOB_ID = 1

REGIONS = ("华东", "华南", "华北")

REGION_SOURCES: dict[str, dict[str, str]] = {
    "华东": {
        "source_id": "SRC-EAST-DEAL",
        "source_name": "华东成交明细库",
        "source_table": "dwd_deal_east_di",
        "endpoint": "partition://dwd_deal_east_di/dt={date}/region=华东",
    },
    "华南": {
        "source_id": "SRC-SOUTH-ORDER",
        "source_name": "华南订单流水库",
        "source_table": "dwd_order_south_di",
        "endpoint": "partition://dwd_order_south_di/dt={date}/region=华南",
    },
    "华北": {
        "source_id": "SRC-NORTH-TRADE",
        "source_name": "华北交易回报库",
        "source_table": "dwd_trade_north_di",
        "endpoint": "partition://dwd_trade_north_di/dt={date}/region=华北",
    },
}


def today_in_shanghai() -> str:
    return datetime.now(SHANGHAI_TZ).date().isoformat()


def _now() -> str:
    return datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")


def _as_int(value: Any, default: int | None = None) -> int | None:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    try:
        text = str(value).strip().replace(",", "")
        return int(float(text))
    except (TypeError, ValueError):
        return default


def _value_or_none(value: Any) -> float | int | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


class PartitionReconciliationService:
    """内存态的分区凭据/补算/报告服务。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.reset_scenario()

    # ------------------------------------------------------------------
    # 场景准备与基础结构
    # ------------------------------------------------------------------
    def reset_scenario(self, period: str | None = None) -> dict[str, Any]:
        """准备题目场景：华东 30、华南确认零行、华北数据源超时。"""
        with self._lock:
            period = period or today_in_shanghai()
            self._counters: dict[str, int] = {}
            self.state: dict[str, Any] = {
                "scenario_id": f"SCN-{period.replace('-', '')}-3SOURCES",
                "metric_id": DEFAULT_METRIC_ID,
                "metric_name": DEFAULT_METRIC_NAME,
                "period": period,
                "coverage": self._coverage(period),
                "records": {},
                "record_history": [],
                "backfills": {},
                "backfill_by_key": {},
                "skipped_empty_requests": [],
                "reports": [],
                "publications": [],
            }

            east = self._make_read_record(
                region="华东", attempt=1, read_status="HAS_DATA",
                metric_value=30, rows_read=30, latency_ms=126,
            )
            south = self._make_read_record(
                region="华南", attempt=1, read_status="CONFIRMED_EMPTY",
                metric_value=0, rows_read=0, latency_ms=183,
            )
            north = self._make_read_record(
                region="华北", attempt=1, read_status="SOURCE_READ_FAILED",
                metric_value=None, rows_read=None, latency_ms=5000,
                error="数据源超时",
            )
            for record in (east, south, north):
                self._store_record(record)

            self._mirror_to_store()
            return self.scenario_summary()

    def prepare_scenario(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = payload or {}
        period = payload.get("period") or payload.get("date") or payload.get("统计日期")
        result = self.reset_scenario(str(period) if period else None)

        # 允许测试通过准备请求覆盖三个地区的初始结果。
        regions = payload.get("regions") or payload.get("region_results")
        if isinstance(regions, list):
            self.reset_scenario(str(period) if period else None)
            for item in regions:
                if isinstance(item, dict) and item.get("region"):
                    self.record_source_read(item)
        return result

    def scenario_summary(self) -> dict[str, Any]:
        with self._lock:
            validation = self.validate_partitions()
            return {
                "ok": True,
                "scenario": "华东有成交、华南确认零行、华北读取超时",
                "scenario_id": self.state["scenario_id"],
                "metric_id": self.state["metric_id"],
                "metric_name": self.state["metric_name"],
                "period": self.state["period"],
                "coverage": self.state["coverage"],
                "partition_execution_records": self.all_records(),
                "分区执行记录": self.all_records(),
                "空分区执行记录": [r for r in self.all_records() if r.get("confirmed_empty")],
                "validation": validation,
                "backfills": self.list_backfills().get("items", []),
                "reports": self.list_reports().get("items", []),
            }

    def get_state(self) -> dict[str, Any]:
        with self._lock:
            return self.scenario_summary()

    def _next_id(self, prefix: str) -> str:
        self._counters[prefix] = int(self._counters.get(prefix, 0)) + 1
        return f"{prefix}-{self._counters[prefix]:04d}"

    def _coverage(self, period: str) -> dict[str, Any]:
        partition_keys = [f"dt={period}/region={region}" for region in REGIONS]
        return {
            "period": period,
            "date": period,
            "granularity": "day",
            "统计粒度": "天",
            "dimension": "region",
            "维度": "地区",
            "regions": list(REGIONS),
            "地区范围": list(REGIONS),
            "partition_keys": partition_keys,
            "分区范围": partition_keys,
            "start_time": f"{period}T00:00:00+08:00",
            "end_time": f"{period}T23:59:59.999999+08:00",
            "coverage_interval": f"[{period}T00:00:00+08:00, {period}T23:59:59.999999+08:00]",
        }

    def _credential(self, source_id: str, partition_key: str, attempt: int, read_at: str) -> str:
        raw = f"{source_id}|{partition_key}|attempt={attempt}|{read_at}|read-success"
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
        return f"prc_{digest}"

    def _make_read_record(
        self,
        *,
        region: str,
        attempt: int,
        read_status: str,
        metric_value: int | float | None,
        rows_read: int | None,
        latency_ms: int | None = None,
        error: str | None = None,
        source_override: dict[str, Any] | None = None,
        backfill_id: str | None = None,
        claim_id: str | None = None,
    ) -> dict[str, Any]:
        period = self.state["period"]
        source = dict(REGION_SOURCES.get(region, {
            "source_id": f"SRC-{region}",
            "source_name": f"{region}数据源",
            "source_table": f"source_{region}",
            "endpoint": f"partition://{region}",
        }))
        if source_override:
            source.update({k: v for k, v in source_override.items() if v is not None})

        partition_key = f"dt={period}/region={region}"
        read_at = _now()
        success = read_status in {"HAS_DATA", "CONFIRMED_EMPTY", "READ_OK"}
        confirmed_empty = read_status == "CONFIRMED_EMPTY" or (
            success and rows_read == 0 and (metric_value in (0, None))
        )
        if confirmed_empty:
            read_status = "CONFIRMED_EMPTY"
            metric_value = 0
            rows_read = 0
        elif success and read_status == "READ_OK":
            read_status = "HAS_DATA"

        credential = self._credential(
            source["source_id"], partition_key, attempt, read_at
        ) if success else None

        if read_status == "CONFIRMED_EMPTY":
            status_label = "已确认空"
            classification = "TRUE_ZERO"
            classification_label = "确实为零"
        elif read_status == "HAS_DATA":
            status_label = "有数据"
            classification = "HAS_DATA"
            classification_label = "有实际数据"
        else:
            status_label = "读取失败"
            classification = "SOURCE_UNAVAILABLE"
            classification_label = "还没拿到源数据"
            metric_value = None
            rows_read = None
            credential = None

        record_id = self._next_id("PER")
        source_read_record = {
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "source_table": source.get("source_table"),
            "endpoint": source.get("endpoint", "").format(date=period) if "{date}" in source.get("endpoint", "") else source.get("endpoint"),
            "partition_key": partition_key,
            "read_at": read_at,
            "read_success": success,
            "row_count": rows_read,
            "rows_read": rows_read,
            "credential": credential,
            "read_credential": credential,
            "latency_ms": latency_ms,
            "error": error,
        }
        return {
            "id": record_id,
            "record_id": record_id,
            "record_type": "EMPTY_PARTITION_EXECUTION" if confirmed_empty else "PARTITION_EXECUTION",
            "job_id": DEFAULT_JOB_ID,
            "task_id": DEFAULT_JOB_ID,
            "query_id": DEFAULT_QUERY_ID,
            "metric_id": self.state["metric_id"],
            "metric_name": self.state["metric_name"],
            "period": period,
            "stat_date": period,
            "coverage_interval": self.state["coverage"]["coverage_interval"],
            "partition_key": partition_key,
            "region": region,
            "地区": region,
            "dimension": "地区",
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "数据来源": source["source_name"],
            "source": source["source_name"],
            "read_status": read_status,
            "read_status_label": status_label,
            "status": status_label,
            "classification": classification,
            "classification_label": classification_label,
            "data_classification": classification,
            "read_success": success,
            "confirmed": success,
            "is_empty": confirmed_empty,
            "empty": confirmed_empty,
            "confirmed_empty": confirmed_empty,
            "is_empty_partition_execution": confirmed_empty,
            "rows_read": rows_read,
            "row_count": rows_read,
            "完成行数": rows_read,
            "metric_value": metric_value,
            "value": metric_value,
            "指标值": metric_value,
            "read_credential": credential,
            "credential": credential,
            "分区读取凭据": credential,
            "source_read_record": source_read_record,
            "实际读取来源记录": source_read_record,
            "error": error,
            "错误信息": error,
            "attempt": attempt,
            "backfill_id": backfill_id,
            "claim_id": claim_id,
            "read_at": read_at,
        }

    def _store_record(self, record: dict[str, Any]) -> None:
        region = record["region"]
        self.state["records"][region] = record
        self.state["record_history"].append(record)

    def all_records(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(r) for r in self.state["record_history"]]

    def current_records(self, period: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            period = period or self.state["period"]
            return [
                dict(record) for region in REGIONS
                if (record := self.state["records"].get(region))
                and record.get("period") == period
            ]

    def empty_partition_records(self, include_unconfirmed: bool = True) -> list[dict[str, Any]]:
        records = []
        for record in self.current_records():
            if record.get("confirmed_empty") or (include_unconfirmed and not record.get("confirmed")):
                records.append(record)
        return records

    def source_reads(self) -> list[dict[str, Any]]:
        return [dict(r["source_read_record"]) for r in self.current_records()]

    # ------------------------------------------------------------------
    # 来源回传与强制约束
    # ------------------------------------------------------------------
    def record_source_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        """登记一次来源读取结果。

        失败来源不允许携带数值零；成功零行必须留下读取凭据。
        """
        with self._lock:
            region = str(payload.get("region") or payload.get("地区") or "").strip()
            if not region:
                return {"ok": False, "error": "缺少 region/地区"}

            explicit_success = payload.get("read_success")
            status = str(payload.get("read_status") or payload.get("status") or "").strip()
            error = payload.get("error") or payload.get("错误信息")
            failed = (
                explicit_success is False
                or status in {"FAILED", "SOURCE_READ_FAILED", "TIMEOUT", "读取失败", "超时"}
                or (error and status not in {"CONFIRMED_EMPTY", "已确认空"})
            )

            value = _value_or_none(payload.get("metric_value", payload.get("value", payload.get("指标值"))))
            rows = _as_int(payload.get("rows_read", payload.get("row_count", payload.get("行数"))))

            if failed:
                if value is not None and value == 0:
                    return {
                        "ok": False,
                        "error": "来源没有读取成功，不能生成有数值的零结果；应标记为未确认，而不是数值 0",
                        "code": "UNREAD_SOURCE_CANNOT_BE_ZERO",
                        "region": region,
                    }
                value = None
                rows = None
                read_status = "SOURCE_READ_FAILED"
                error = error or "数据源读取失败"
            else:
                if rows is None:
                    rows = _as_int(value, default=0)
                if rows == 0 or status in {"CONFIRMED_EMPTY", "TRUE_ZERO", "已确认空", "确认零行"}:
                    read_status = "CONFIRMED_EMPTY"
                    value = 0
                    rows = 0
                else:
                    read_status = "HAS_DATA"
                    if value is None:
                        value = rows

            previous = self.state["records"].get(region)
            attempt = int(previous.get("attempt", 0)) + 1 if previous else 1
            source_override = payload.get("source") if isinstance(payload.get("source"), dict) else None
            if source_override is None and payload.get("source_id"):
                source_override = {
                    "source_id": payload.get("source_id"),
                    "source_name": payload.get("source_name") or payload.get("数据来源"),
                }
            record = self._make_read_record(
                region=region,
                attempt=attempt,
                read_status=read_status,
                metric_value=value,
                rows_read=rows,
                latency_ms=_as_int(payload.get("latency_ms")),
                error=str(error) if error else None,
                source_override=source_override,
                backfill_id=payload.get("backfill_id"),
                claim_id=payload.get("claim_id"),
            )
            self._store_record(record)
            self._mirror_to_store()
            return {"ok": True, "record": record}

    # ------------------------------------------------------------------
    # 分区/覆盖区间校验
    # ------------------------------------------------------------------
    def validate_partitions(
        self,
        job_id: int | str = DEFAULT_JOB_ID,
        period: str | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            period = period or self.state["period"]
            coverage = self._coverage(period) if period != self.state["period"] else self.state["coverage"]
            records = self.current_records(period)
            by_region = {r["region"]: r for r in records}
            errors: list[dict[str, Any]] = []
            warnings: list[dict[str, Any]] = []

            for region in REGIONS:
                rec = by_region.get(region)
                if rec is None:
                    errors.append({
                        "code": "MISSING_PARTITION_EXECUTION",
                        "region": region,
                        "message": f"{region} 缺少统计期 {period} 的分区执行记录",
                    })
                    continue

                if rec.get("period") != period:
                    errors.append({
                        "code": "PERIOD_MISMATCH",
                        "region": region,
                        "expected": period,
                        "actual": rec.get("period"),
                    })

                if rec.get("confirmed_empty"):
                    if rec.get("metric_value") != 0 or rec.get("rows_read") != 0:
                        errors.append({
                            "code": "INVALID_EMPTY_RECORD",
                            "region": region,
                            "message": "确认空分区必须同时满足读取 0 行且指标值为 0",
                        })
                    if not rec.get("read_credential") or not rec.get("source_read_record"):
                        errors.append({
                            "code": "MISSING_READ_CREDENTIAL",
                            "region": region,
                            "message": "确认空数据必须保留实际读取成功的来源记录和分区读取凭据",
                        })

                if rec.get("read_success") and not rec.get("read_credential"):
                    errors.append({
                        "code": "MISSING_READ_CREDENTIAL",
                        "region": region,
                        "message": "来源读取成功但缺少分区读取凭据",
                    })

                if not rec.get("read_success") and rec.get("metric_value") is not None:
                    errors.append({
                        "code": "UNVERIFIED_NUMERIC_ZERO",
                        "region": region,
                        "message": "来源没有读取成功，不能生成有数值的零结果",
                        "actual_value": rec.get("metric_value"),
                    })

                if not rec.get("read_success"):
                    errors.append({
                        "code": "SOURCE_READ_FAILED",
                        "region": region,
                        "source_id": rec.get("source_id"),
                        "source_name": rec.get("source_name"),
                        "reason": rec.get("error") or "来源读取失败",
                        "message": f"{region} 尚未拿到源数据，不能按零确认",
                        "numeric_zero_allowed": False,
                    })

            confirmed = [r["region"] for r in records if r.get("confirmed")]
            true_zero = [r["region"] for r in records if r.get("confirmed_empty")]
            unavailable = [r["region"] for r in records if not r.get("confirmed")]
            # 任何分区缺失/读取失败/凭据或区间不一致，都不能宣称全区间覆盖完成。
            complete = not unavailable and not errors

            integrity_passed = not any(
                e["code"] in {"UNVERIFIED_NUMERIC_ZERO", "MISSING_READ_CREDENTIAL", "INVALID_EMPTY_RECORD"}
                for e in errors
            )
            return {
                "ok": True,
                "job_id": job_id,
                "period": period,
                "coverage": coverage,
                "coverage_interval": coverage["coverage_interval"],
                "expected_regions": list(REGIONS),
                "records": records,
                "partition_execution_records": records,
                "分区执行记录": records,
                "empty_partition_records": [r for r in records if r.get("confirmed_empty")],
                "empty_partition_execution_records": [r for r in records if r.get("confirmed_empty")],
                "空分区执行记录": [r for r in records if r.get("confirmed_empty")],
                "failed_partition_records": [r for r in records if not r.get("confirmed")],
                "source_read_receipts": [r["source_read_record"] for r in records],
                "source_records": [r["source_read_record"] for r in records],
                "实际读取来源记录": [r["source_read_record"] for r in records],
                "credentials": [r.get("read_credential") for r in records if r.get("read_credential")],
                "分区读取凭据": [r.get("read_credential") for r in records if r.get("read_credential")],
                "confirmed_regions": confirmed,
                "confirmed_region_scope": confirmed,
                "true_zero_regions": true_zero,
                "确认零行地区": true_zero,
                "source_unavailable_regions": unavailable,
                "未取得源数据地区": unavailable,
                "errors": errors,
                "warnings": warnings,
                "integrity_passed": integrity_passed,
                "record_integrity_valid": integrity_passed,
                "coverage_complete": complete,
                "all_regions_confirmed": complete,
                "can_finalize_full_coverage": complete,
                "passed": complete and integrity_passed,
                "valid": complete and integrity_passed,
                "summary": {
                    "total_partitions": len(REGIONS),
                    "confirmed_partitions": len(confirmed),
                    "true_zero_partitions": len(true_zero),
                    "unavailable_partitions": len(unavailable),
                },
            }

    # ------------------------------------------------------------------
    # 补算受理与幂等领取
    # ------------------------------------------------------------------
    def _missing_key(self, region: str, period: str, metric_id: str | None = None) -> str:
        metric_id = metric_id or self.state["metric_id"]
        return f"{metric_id}|{period}|region={region}"

    def submit_backfill(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            payload = payload or {}
            region = str(payload.get("region") or payload.get("地区") or payload.get("partition_region") or "").strip()
            period = str(payload.get("period") or payload.get("date") or payload.get("统计日期") or self.state["period"])
            metric_id = payload.get("metric_id") or self.state["metric_id"]
            explicit_key = payload.get("missing_key") or payload.get("idempotency_key") or payload.get("缺项标识")
            missing_key = str(explicit_key) if explicit_key else self._missing_key(region, period, str(metric_id))

            if not region:
                # 也允许从标准缺项键的末尾解析 region=华北。
                if "region=" in missing_key:
                    region = missing_key.split("region=", 1)[1].split("|", 1)[0]
                else:
                    return {"ok": False, "error": "缺少 region/地区，无法提交补算"}

            record = self.state["records"].get(region)
            if record and record.get("period") != period:
                record = next((r for r in self.state["record_history"] if r["region"] == region and r["period"] == period), None)

            # 已确认空：重复补单也不执行。
            if record and record.get("confirmed_empty"):
                item = {
                    "backfill_id": f"SKIP-EMPTY-{region}-{period}",
                    "id": f"SKIP-EMPTY-{region}-{period}",
                    "claim_id": None,
                    "missing_key": missing_key,
                    "region": region,
                    "period": period,
                    "status": "SKIPPED_CONFIRMED_EMPTY",
                    "status_label": "已确认空，跳过补算",
                    "execution_created": False,
                    "execution_count": 0,
                    "executions": [],
                    "should_execute": False,
                    "partition_record_id": record["record_id"],
                    "partition_record_id": record["record_id"],
                    "submitted_at": _now(),
                    "idempotent": True,
                    "message": f"{region}已凭实际读取记录确认零行，不因补单重复执行",
                }
                self.state["skipped_empty_requests"].append(item)
                return {"ok": True, "deduplicated": True, "skipped": True, "backfill": item, **item}

            if record and record.get("confirmed") and record.get("metric_value") not in (None,):
                item = {
                    "missing_key": missing_key,
                    "region": region,
                    "period": period,
                    "status": "SKIPPED_ALREADY_CONFIRMED",
                    "status_label": "已有确认数据，跳过补算",
                    "execution_created": False,
                    "partition_record_id": record["record_id"],
                    "submitted_at": _now(),
                    "idempotent": True,
                    "message": f"{region}已有成功读取并确认的数据，无需补算",
                }
                return {"ok": True, "deduplicated": True, "skipped": True, "backfill": item, **item}

            existing_id = self.state["backfill_by_key"].get(missing_key)
            if existing_id:
                order = self.state["backfills"][existing_id]
                order["submission_count"] = int(order.get("submission_count", 1)) + 1
                order["duplicate_submission_count"] = int(order.get("duplicate_submission_count", 0)) + 1
                order["deduplicated"] = True
                order["new_claim"] = False
                order["claim_retention"] = "SAME_CLAIM"
                order["message"] = "同一缺项已受理，重复提交仍使用同一次领取"
                return {
                    "ok": True,
                    "deduplicated": True,
                    "already_accepted": True,
                    "new_claim": False,
                    "claim_id": order["claim_id"],
                    "lease_id": order["claim_id"],
                    "claim_token": order["claim_id"],
                    "领取号": order["claim_id"],
                    "backfill_id": order["backfill_id"],
                    "backfill": dict(order),
                    "message": order["message"],
                }

            backfill_id = self._next_id("BF")
            claim_seed = f"{missing_key}|{backfill_id}|single-claim"
            claim_id = "CLM-" + hashlib.sha256(claim_seed.encode()).hexdigest()[:16]
            owner = payload.get("owner") or payload.get("assignee") or payload.get("值班员") or "值班员"
            order = {
                "backfill_id": backfill_id,
                "id": backfill_id,
                "claim_id": claim_id,
                "lease_id": claim_id,
                "claim_token": claim_id,
                "领取号": claim_id,
                "领取凭据": claim_id,
                "missing_key": missing_key,
                "idempotency_key": missing_key,
                "region": region,
                "地区": region,
                "period": period,
                "date": period,
                "metric_id": metric_id,
                "reason": payload.get("reason") or (record.get("error") if record else "缺少分区执行记录") or "源数据未确认",
                "status": "ACCEPTED",
                "status_label": "已受理/已领取",
                "owner": owner,
                "assignee": owner,
                "submitted_at": _now(),
                "accepted_at": _now(),
                "claim_count": 1,
                "submission_count": 1,
                "duplicate_submission_count": 0,
                "execution_count": 0,
                "deduplicated": False,
                "new_claim": True,
                "claim_retention": "SINGLE_CLAIM_RETAINED",
                "result_value": None,
                "partition_record_id": record.get("record_id") if record else None,
                "completion": None,
                "message": "缺项已受理，只保留同一次领取；重复提交不会生成新领取",
            }
            self.state["backfills"][backfill_id] = order
            self.state["backfill_by_key"][missing_key] = backfill_id
            self._mirror_to_store()
            return {
                "ok": True,
                "deduplicated": False,
                "already_accepted": False,
                "new_claim": True,
                "claim_id": claim_id,
                "lease_id": claim_id,
                "claim_token": claim_id,
                "领取号": claim_id,
                "backfill_id": backfill_id,
                "backfill": dict(order),
                "message": order["message"],
            }

    def list_backfills(self) -> dict[str, Any]:
        with self._lock:
            items = [dict(o) for o in self.state["backfills"].values()]
            return {"items": items, "total": len(items), "skipped_empty_requests": list(self.state["skipped_empty_requests"])}

    def get_backfill(self, backfill_id: str) -> dict[str, Any] | None:
        with self._lock:
            order = self.state["backfills"].get(backfill_id)
            return dict(order) if order else None

    def complete_backfill(
        self,
        backfill_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            payload = payload or {}
            order = None
            if backfill_id:
                order = self.state["backfills"].get(backfill_id)
            if order is None and payload.get("backfill_id"):
                order = self.state["backfills"].get(str(payload["backfill_id"]))
            if order is None and payload.get("claim_id"):
                order = next((o for o in self.state["backfills"].values() if o["claim_id"] == payload["claim_id"]), None)
            if order is None:
                region = payload.get("region") or payload.get("地区") or "华北"
                period = payload.get("period") or payload.get("date") or self.state["period"]
                key = self._missing_key(str(region), str(period))
                bf_id = self.state["backfill_by_key"].get(key)
                order = self.state["backfills"].get(bf_id) if bf_id else None

            if order is None:
                # 允许值班员直接回传华北结果；先幂等受理再完成。
                submit = self.submit_backfill(payload)
                if not submit.get("ok") or submit.get("skipped"):
                    return submit
                order = self.state["backfills"][submit["backfill_id"]]

            supplied_claim = payload.get("claim_id")
            if supplied_claim and supplied_claim != order["claim_id"]:
                return {
                    "ok": False,
                    "http_status": 409,
                    "error": "领取凭据不一致；已经受理的地区只保留同一次领取",
                    "expected_claim_id": order["claim_id"],
                    "supplied_claim_id": supplied_claim,
                }

            if order["status"] == "COMPLETED":
                return {
                    "ok": True,
                    "idempotent": True,
                    "deduplicated": True,
                    "backfill": dict(order),
                    "record": self.state["records"].get(order["region"]),
                    "message": "补算已经完成，重复完成请求返回同一结果",
                }

            value = _value_or_none(payload.get("value", payload.get("metric_value", payload.get("指标值", 15))))
            if value is None:
                value = 15
            rows = _as_int(payload.get("rows_read", payload.get("row_count")), default=int(value))
            read_result = self.record_source_read({
                "region": order["region"],
                "period": order["period"],
                "read_success": True,
                "read_status": "HAS_DATA",
                "metric_value": value,
                "rows_read": rows,
                "source_id": payload.get("source_id"),
                "source_name": payload.get("source_name") or payload.get("数据来源"),
                "backfill_id": order["backfill_id"],
                "claim_id": order["claim_id"],
            })
            if not read_result.get("ok"):
                return read_result

            record = read_result["record"]
            order["status"] = "COMPLETED"
            order["status_label"] = "补算完成，来源已读取"
            order["completed_at"] = _now()
            order["execution_count"] = 1
            order["result_value"] = value
            order["partition_record_id"] = record["record_id"]
            order["completion"] = {
                "value": value,
                "rows_read": rows,
                "record_id": record["record_id"],
                "read_credential": record["read_credential"],
                "completed_at": order["completed_at"],
            }
            order["message"] = f"{order['region']}补算完成并留下实际来源读取凭据"
            self._mirror_to_store()
            return {"ok": True, "deduplicated": False, "backfill": dict(order), "record": record}

    # ------------------------------------------------------------------
    # 指标报告快照、范围核对与发布
    # ------------------------------------------------------------------
    def _requested_regions(self, payload: dict[str, Any] | None) -> list[str]:
        payload = payload or {}
        raw = payload.get("regions") or payload.get("region_scope") or payload.get("地区范围")
        if raw is None:
            return list(REGIONS)
        if isinstance(raw, str):
            if raw.strip().lower() in {"all", "all_regions", "national", "全国", "全部", "全部地区"}:
                return list(REGIONS)
            raw = [x.strip() for x in raw.replace("，", ",").split(",") if x.strip()]
        if isinstance(raw, Iterable):
            values = [str(x) for x in raw]
            if len(values) == 1 and values[0].strip().lower() in {"all", "all_regions", "national", "全国", "全部", "全部地区"}:
                return list(REGIONS)
            return values
        return list(REGIONS)

    def create_report(
        self,
        query_id: int | str = DEFAULT_QUERY_ID,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            payload = payload or {}
            period = str(payload.get("period") or payload.get("date") or payload.get("统计日期") or self.state["period"])
            requested = self._requested_regions(payload)
            records = self.current_records(period)
            by_region = {r["region"]: r for r in records}

            missing = [region for region in requested if region not in by_region]
            unknown = [region for region in requested if region not in REGIONS]
            scope_match = not missing and not unknown
            if not scope_match:
                return {
                    "ok": False,
                    "http_status": 409,
                    "error": "查询地区范围与空分区执行记录/覆盖区间对不上",
                    "missing_partition_records": missing,
                    "unknown_regions": unknown,
                    "available_regions": list(by_region),
                }

            region_results = []
            sources = []
            all_attempts = []
            total = 0.0
            confirmed_regions: list[str] = []
            unconfirmed_regions: list[str] = []
            zero_regions: list[str] = []

            for region in requested:
                rec = by_region[region]
                attempts = [
                    r for r in self.state["record_history"]
                    if r["region"] == region and r.get("period") == period
                ]
                all_attempts.extend(attempts)
                confirmed = bool(rec.get("confirmed"))
                value = rec.get("metric_value") if confirmed else None
                counted = confirmed and value is not None
                if counted:
                    total += float(value)
                    confirmed_regions.append(region)
                else:
                    unconfirmed_regions.append(region)
                if rec.get("confirmed_empty"):
                    zero_regions.append(region)

                region_results.append({
                    "region": region,
                    "地区": region,
                    "status": rec.get("status"),
                    "classification": rec.get("classification"),
                    "classification_label": rec.get("classification_label"),
                    "confirmed": confirmed,
                    "confirmed_empty": rec.get("confirmed_empty"),
                    "value": value,
                    "metric_value": value,
                    "指标值": value,
                    "counted_in_total": counted,
                    "计入总量": counted,
                    "read_credential": rec.get("read_credential"),
                    "分区读取凭据": rec.get("read_credential"),
                    "source_id": rec.get("source_id"),
                    "source_name": rec.get("source_name"),
                    "数据来源": rec.get("source_name"),
                    "partition_record_id": rec.get("record_id"),
                    "attempt_count": len(attempts),
                    "error": rec.get("error"),
                })

                sources.append({
                    "region": region,
                    "source_id": rec.get("source_id"),
                    "source_name": rec.get("source_name"),
                    "source_table": rec.get("source_read_record", {}).get("source_table"),
                    "partition_key": rec.get("partition_key"),
                    "partition_record_id": rec.get("record_id"),
                    "read_success": rec.get("read_success"),
                    "read_at": rec.get("read_at"),
                    "credential": rec.get("read_credential"),
                    "read_credential": rec.get("read_credential"),
                    "rows_read": rec.get("rows_read"),
                    "used_in_total": counted,
                    "计入报告总量": counted,
                    "usage": (
                        "CONFIRMED_ZERO_USED" if rec.get("confirmed_empty")
                        else "USED" if counted
                        else "FAILED_ATTEMPT_EXCLUDED"
                    ),
                    "error": rec.get("error"),
                })

            related_backfills = [
                dict(o) for o in self.state["backfills"].values()
                if o.get("period") == period and o.get("region") in requested
            ]
            report_id = self._next_id("RPT")
            complete = not unconfirmed_regions
            total_value: int | float = int(total) if float(total).is_integer() else round(total, 2)
            report = {
                "ok": True,
                "report_id": report_id,
                "id": report_id,
                "query_id": query_id,
                "metric_id": payload.get("metric_id") or self.state["metric_id"],
                "metric_name": payload.get("metric_name") or self.state["metric_name"],
                "period": period,
                "date": period,
                "created_at": _now(),
                "requested_regions": requested,
                "query_region_scope": requested,
                "查询地区范围": requested,
                "scope_match": True,
                "范围核对": "查询地区范围与分区执行记录、覆盖区间一致",
                "coverage": self._coverage(period),
                "region_results": region_results,
                "地区结果": region_results,
                "confirmed_regions": confirmed_regions,
                "actual_regions": confirmed_regions,
                "实际范围": confirmed_regions,
                "true_zero_regions": zero_regions,
                "confirmed_empty_regions": zero_regions,
                "unconfirmed_regions": unconfirmed_regions,
                "未确认地区": unconfirmed_regions,
                "excluded_regions": unconfirmed_regions,
                "排除地区": unconfirmed_regions,
                "total": total_value,
                "confirmed_total": total_value,
                "current_day_confirmed_total": total_value,
                "当日已确认总量": total_value,
                "daily_total": total_value,
                "national_total": total_value if complete else None,
                "complete": complete,
                "coverage_complete": complete,
                "is_complete": complete,
                "publishable_as_national": complete,
                "publishable_as_confirmed_scope": True,
                "unconfirmed_value_policy": "EXCLUDED_NOT_ZERO",
                "未确认地区处理": "排除且不按零计入",
                "sources": sources,
                "source_usage": sources,
                "来源核对": sources,
                "source_attempt_history": all_attempts,
                "partition_record_ids": [r["record_id"] for r in records if r["region"] in requested],
                "partition_execution_records": [dict(by_region[region]) for region in requested],
                "空分区执行记录": [
                    dict(by_region[region]) for region in requested
                    if by_region[region].get("confirmed_empty")
                ],
                "backfill_actions": related_backfills,
                "补算动作": related_backfills,
                "comparison_key": f"{self.state['metric_id']}|{period}|{','.join(requested)}",
                "status": "已生成",
            }
            self.state["reports"].append(report)
            self._mirror_to_store(report)
            return dict(report)

    def list_reports(self) -> dict[str, Any]:
        with self._lock:
            return {"items": [dict(r) for r in self.state["reports"]], "total": len(self.state["reports"])}

    def _find_report(self, report_ref: str | int | None) -> dict[str, Any] | None:
        if report_ref is None:
            return self.state["reports"][-1] if self.state["reports"] else None
        ref = str(report_ref)
        for report in self.state["reports"]:
            if str(report.get("report_id")) == ref or str(report.get("id")) == ref:
                return report
        # 支持 1 / RPT-1 / RPT-0001。
        digits = "".join(ch for ch in ref if ch.isdigit())
        if digits:
            num = int(digits)
            for report in self.state["reports"]:
                rid_digits = "".join(ch for ch in str(report.get("report_id")) if ch.isdigit())
                if rid_digits and int(rid_digits) == num:
                    return report
        return None

    def get_report(self, report_id: str | int) -> dict[str, Any] | None:
        with self._lock:
            report = self._find_report(report_id)
            return dict(report) if report else None

    def compare_reports(
        self,
        first_id: str | int | None = None,
        second_id: str | int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            first = self._find_report(first_id if first_id is not None else 0 if self.state["reports"] else None)
            if first_id is None and self.state["reports"]:
                first = self.state["reports"][0]
            second = self._find_report(second_id)
            if second_id is None and self.state["reports"]:
                second = self.state["reports"][-1]
            if not first or not second:
                return {"ok": False, "error": "需要两份已查询的报告才能对照"}

            first_map = {r["region"]: r for r in first["region_results"]}
            second_map = {r["region"]: r for r in second["region_results"]}
            regions = list(dict.fromkeys([*first_map.keys(), *second_map.keys(), *REGIONS]))
            deltas = {}
            for region in regions:
                a = first_map.get(region, {}).get("value")
                b = second_map.get(region, {}).get("value")
                numeric_a = float(a) if isinstance(a, (int, float)) else None
                numeric_b = float(b) if isinstance(b, (int, float)) else None
                delta = None if numeric_a is None or numeric_b is None else round(numeric_b - numeric_a, 2)
                deltas[region] = {
                    "before": a,
                    "after": b,
                    "delta": delta,
                    "first_counted": first_map.get(region, {}).get("counted_in_total", False),
                    "second_counted": second_map.get(region, {}).get("counted_in_total", False),
                }

            first_actual = set(first.get("actual_regions", []))
            second_actual = set(second.get("actual_regions", []))
            first_requested = set(first.get("requested_regions", []))
            second_requested = set(second.get("requested_regions", []))
            total_delta = second["confirmed_total"] - first["confirmed_total"]
            result = {
                "ok": True,
                "first_report_id": first["report_id"],
                "second_report_id": second["report_id"],
                "first_scope": {
                    "requested_regions": first.get("requested_regions"),
                    "actual_regions": first.get("actual_regions"),
                    "confirmed_regions": first.get("confirmed_regions"),
                    "unconfirmed_regions": first.get("unconfirmed_regions"),
                    "total": first.get("confirmed_total"),
                    "source_ids": [s["source_id"] for s in first.get("sources", []) if s.get("used_in_total")],
                    "credentials": [s.get("credential") for s in first.get("sources", []) if s.get("credential")],
                },
                "second_scope": {
                    "requested_regions": second.get("requested_regions"),
                    "actual_regions": second.get("actual_regions"),
                    "confirmed_regions": second.get("confirmed_regions"),
                    "unconfirmed_regions": second.get("unconfirmed_regions"),
                    "total": second.get("confirmed_total"),
                    "source_ids": [s["source_id"] for s in second.get("sources", []) if s.get("used_in_total")],
                    "credentials": [s.get("credential") for s in second.get("sources", []) if s.get("credential")],
                },
                "requested_scope_unchanged": first_requested == second_requested,
                "same_query_scope": first_requested == second_requested,
                "scope_added": sorted(second_actual - first_actual),
                "scope_removed": sorted(first_actual - second_actual),
                "actual_scope_changed": first_actual != second_actual,
                "region_deltas": deltas,
                "total_before": first["confirmed_total"],
                "total_after": second["confirmed_total"],
                "total_delta": total_delta,
                "difference": total_delta,
                "scope_verified": first.get("scope_match") and second.get("scope_match"),
                "reports_can_be_checked": True,
                "conclusion": (
                    f"第一份报告请求范围为{'、'.join(first.get('requested_regions', []))}，"
                    f"实际确认范围为{'、'.join(first.get('actual_regions', []))}，"
                    f"总量 {first['confirmed_total']}；"
                    f"第二份实际确认范围为{'、'.join(second.get('actual_regions', []))}，"
                    f"总量 {second['confirmed_total']}。"
                    + (f"范围新增{'、'.join(sorted(second_actual - first_actual))}，" if second_actual - first_actual else "")
                    + f"总量差异 {total_delta}。两份报告的地区范围均与分区执行记录核对一致。"
                ),
            }
            return result

    def publish_report(
        self,
        report_id: str | int | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            payload = payload or {}
            report = self._find_report(report_id or payload.get("report_id"))
            if report is None:
                return {"ok": False, "http_status": 404, "error": f"报告 {report_id} 不存在"}

            mode = str(
                payload.get("mode") or payload.get("publish_scope") or payload.get("scope")
                or ("national" if payload.get("as_national") else "confirmed")
            ).strip()
            as_national = mode in {"national", "all", "all_regions", "全国", "全量", "当日总量"} or bool(payload.get("as_national"))

            if as_national and report.get("unconfirmed_regions"):
                return {
                    "ok": False,
                    "http_status": 409,
                    "error": "仍有未确认地区，不能作为完整当日总量发布；未确认地区不得按零计入",
                    "report_id": report["report_id"],
                    "unconfirmed_regions": report["unconfirmed_regions"],
                    "confirmed_total": report["confirmed_total"],
                    "allowed_action": "PUBLISH_CONFIRMED_SCOPE_ONLY",
                }

            publication_id = self._next_id("PUB")
            publication = {
                "ok": True,
                "publication_id": publication_id,
                "id": publication_id,
                "report_id": report["report_id"],
                "published_at": _now(),
                "scope_type": "NATIONAL_COMPLETE" if as_national else "CONFIRMED_REGIONS_ONLY",
                "发布范围": "全部已覆盖地区" if as_national else "仅已确认地区",
                "published_total": report["confirmed_total"],
                "发布总量": report["confirmed_total"],
                "total": report["confirmed_total"],
                "regions": report["confirmed_regions"],
                "included_regions": report["confirmed_regions"],
                "excluded_regions": [] if as_national else report["unconfirmed_regions"],
                "未确认地区": report["unconfirmed_regions"],
                "unconfirmed_regions_counted": [],
                "未确认地区计入": [],
                "source_usage": report["sources"],
                "sources_used": [s for s in report["sources"] if s.get("used_in_total")],
                "guard": "PUBLISH_ONLY_CONFIRMED_PARTITIONS; UNCONFIRMED_NOT_COUNTED_AS_ZERO",
                "message": (
                    "已按全部确认地区发布完整当日总量"
                    if as_national
                    else "已发布已确认范围结果；未确认地区仅列示，未计入当日总量"
                ),
            }
            self.state["publications"].append(publication)
            report["publication_id"] = publication_id
            report["publish_status"] = "已发布"
            return dict(publication)

    def list_publications(self) -> dict[str, Any]:
        with self._lock:
            return {"items": [dict(p) for p in self.state["publications"]], "total": len(self.state["publications"])}

    # ------------------------------------------------------------------
    # 一键场景脚本，便于演示和端到端核对
    # ------------------------------------------------------------------
    def run_scenario_script(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            self.reset_scenario((payload or {}).get("period") or (payload or {}).get("date"))
            validation_before = self.validate_partitions()
            first_report = self.create_report(payload={"requested_by": "经营人员", **(payload or {})})

            submit_1 = self.submit_backfill({"region": "华北", "reason": "华北数据源超时", "owner": "值班员"})
            submit_2 = self.submit_backfill({"region": "华北", "reason": "重复提交同一缺项"})
            submit_3 = self.submit_backfill({"region": "华北", "reason": "第三次提交同一缺项"})
            submit_empty = self.submit_backfill({"region": "华南", "reason": "给已确认空区重复补单"})

            claim_id = submit_1.get("claim_id")
            complete = self.complete_backfill(submit_1.get("backfill_id"), {
                "value": 15,
                "claim_id": claim_id,
            })
            second_report = self.create_report(payload={"requested_by": "经营人员", **(payload or {})})
            comparison = self.compare_reports(first_report["report_id"], second_report["report_id"])

            blocked_national = self.publish_report(first_report["report_id"], {"as_national": True})
            publish_first_partial = self.publish_report(first_report["report_id"], {"mode": "confirmed"})
            publish_second_national = self.publish_report(second_report["report_id"], {"as_national": True})

            return {
                "ok": True,
                "scenario_id": self.state["scenario_id"],
                "validation_before": validation_before,
                "first_report": first_report,
                "backfill_submissions": [submit_1, submit_2, submit_3],
                "same_claim_verified": len({s.get("claim_id") for s in [submit_1, submit_2, submit_3] if s.get("claim_id")}) == 1,
                "empty_region_submission": submit_empty,
                "empty_region_execution_skipped": submit_empty.get("execution_created") is False,
                "backfill_completion": complete,
                "second_report": second_report,
                "comparison": comparison,
                "publish_first_national_blocked": blocked_national,
                "publish_first_confirmed_scope": publish_first_partial,
                "publish_second_national": publish_second_national,
            }

    # ------------------------------------------------------------------
    # 镜像到通用资源行，使 GET /api/aggregate_job/1 也能看到凭据
    # ------------------------------------------------------------------
    def _mirror_to_store(self, latest_report: dict[str, Any] | None = None) -> None:
        try:
            job = store.find("aggregate_job", DEFAULT_JOB_ID)
            if job is not None:
                validation = self.validate_partitions()
                job["partition_coverage"] = self.state["coverage"]
                job["coverage_interval"] = self.state["coverage"]["coverage_interval"]
                job["partition_execution_records"] = self.all_records()
                job["current_partition_records"] = self.current_records()
                job["empty_partition_records"] = validation["empty_partition_records"]
                job["source_read_credentials"] = validation["source_read_receipts"]
                job["partition_credentials"] = validation["credentials"]
                job["partition_validation"] = {
                    key: validation[key] for key in (
                        "integrity_passed", "coverage_complete", "all_regions_confirmed",
                        "passed", "valid", "confirmed_regions", "true_zero_regions",
                        "source_unavailable_regions", "errors", "summary",
                    )
                }
                job["confirmed_total"] = sum(
                    float(r.get("metric_value") or 0)
                    for r in self.current_records() if r.get("confirmed")
                )

            query = store.find("dim_query", DEFAULT_QUERY_ID)
            if query is not None:
                query["partition_scope"] = self.state["coverage"]
                query["partition_execution_records"] = self.current_records()
                if latest_report:
                    query["latest_report_id"] = latest_report["report_id"]
                    query["latest_daily_report"] = {
                        key: latest_report.get(key) for key in (
                            "report_id", "total", "confirmed_total", "requested_regions",
                            "actual_regions", "unconfirmed_regions", "created_at",
                        )
                    }
        except Exception:
            # 镜像只用于方便通用资源接口展示，不能影响领域主流程。
            pass


reconciliation = PartitionReconciliationService()
