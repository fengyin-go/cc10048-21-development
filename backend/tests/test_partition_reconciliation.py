"""空分区凭据、补算幂等和报告范围核对的场景测试。"""
from fastapi.testclient import TestClient

from app.main import app
from app.services.reconciliation import reconciliation

client = TestClient(app)


def setup_function() -> None:
    reconciliation.reset_scenario("2026-11-25")


def test_prepare_distinguishes_true_zero_from_unread_source():
    state = client.post("/api/reconciliation/scenario/prepare", json={"period": "2026-11-25"}).json()
    records = {row["region"]: row for row in state["partition_execution_records"]}

    assert records["华东"]["metric_value"] == 30
    assert records["华东"]["read_credential"]
    assert records["华南"]["confirmed_empty"] is True
    assert records["华南"]["metric_value"] == 0
    assert records["华南"]["rows_read"] == 0
    assert records["华南"]["read_credential"]
    assert records["华南"]["source_read_record"]["read_success"] is True

    assert records["华北"]["confirmed"] is False
    assert records["华北"]["classification"] == "SOURCE_UNAVAILABLE"
    assert records["华北"]["metric_value"] is None
    assert records["华北"]["read_credential"] is None
    assert "超时" in records["华北"]["error"]

    source_ids = {row["source_id"] for row in state["partition_execution_records"]}
    assert len(source_ids) == 3


def test_failed_source_cannot_be_recorded_as_numeric_zero():
    result = reconciliation.record_source_read({
        "region": "华北",
        "read_success": False,
        "metric_value": 0,
        "error": "timeout",
    })
    assert result["ok"] is False
    current = reconciliation.state["records"]["华北"]
    assert current["metric_value"] is None


def test_first_report_excludes_unconfirmed_north_without_treating_it_as_zero():
    response = client.get("/api/domain/dim_query/1/execute?date=2026-11-25")
    assert response.status_code == 200
    body = response.json()
    report = body["daily_report"]

    assert report["requested_regions"] == ["华东", "华南", "华北"]
    assert report["actual_regions"] == ["华东", "华南"]
    assert report["confirmed_regions"] == ["华东", "华南"]
    assert report["unconfirmed_regions"] == ["华北"]
    assert report["confirmed_total"] == 30
    assert report["national_total"] is None
    assert {source["region"] for source in report["sources"]} == {"华东", "华南", "华北"}
    assert all(source["credential"] for source in report["sources"] if source["used_in_total"])
    assert any(source["region"] == "华北" and not source["used_in_total"] for source in report["sources"])


def test_duplicate_backfill_keeps_one_claim_and_empty_region_skips_execution():
    responses = [
        client.post("/api/reconciliation/backfills", json={"region": "华北"}).json()
        for _ in range(3)
    ]
    assert responses[0]["new_claim"] is True
    assert all(item["backfill_id"] == responses[0]["backfill_id"] for item in responses)
    assert len({item["claim_id"] for item in responses}) == 1
    assert responses[1]["deduplicated"] is True
    assert responses[1]["new_claim"] is False

    south = client.post("/api/reconciliation/backfills", json={"region": "华南"}).json()
    assert south["status"] == "SKIPPED_CONFIRMED_EMPTY"
    assert south["execution_created"] is False


def test_full_backfill_requery_compare_and_publish_scope():
    first = client.post("/api/reconciliation/reports/query", json={"period": "2026-11-25"}).json()
    assert first["confirmed_total"] == 30

    submitted = client.post("/api/reconciliation/backfills", json={"region": "华北"}).json()
    completed = client.post(
        f"/api/reconciliation/backfills/{submitted['backfill_id']}/complete",
        json={"value": 15, "claim_id": submitted["claim_id"]},
    ).json()
    assert completed["ok"] is True
    assert completed["record"]["metric_value"] == 15
    assert completed["record"]["read_credential"]

    second = client.post("/api/reconciliation/reports/query", json={"period": "2026-11-25"}).json()
    assert second["confirmed_total"] == 45
    assert second["actual_regions"] == ["华东", "华南", "华北"]

    comparison = client.get(
        f"/api/reconciliation/reports/{first['report_id']}/compare/{second['report_id']}"
    ).json()
    assert comparison["requested_scope_unchanged"] is True
    assert comparison["scope_added"] == ["华北"]
    assert comparison["total_before"] == 30
    assert comparison["total_after"] == 45
    assert comparison["total_delta"] == 15

    blocked = client.post(
        f"/api/reconciliation/reports/{first['report_id']}/publish",
        json={"as_national": True},
    )
    assert blocked.status_code == 409
    assert blocked.json()["unconfirmed_regions"] == ["华北"]

    partial = client.post(
        f"/api/reconciliation/reports/{first['report_id']}/publish",
        json={"mode": "confirmed"},
    ).json()
    assert partial["ok"] is True
    assert partial["published_total"] == 30
    assert partial["excluded_regions"] == ["华北"]
    assert partial["unconfirmed_regions_counted"] == []

    national = client.post(
        f"/api/reconciliation/reports/{second['report_id']}/publish",
        json={"as_national": True},
    ).json()
    assert national["ok"] is True
    assert national["published_total"] == 45
    assert national["regions"] == ["华东", "华南", "华北"]


def test_aggregate_execution_contains_credentials_and_no_failed_north_zero():
    result = client.get("/api/domain/aggregate_job/1/execute?date=2026-11-25").json()
    assert "partition_execution_records" in result
    assert result["partition_audit"]["true_zero_regions"] == ["华南"]
    north = next(row for row in result["partition_execution_records"] if row["region"] == "华北")
    assert north["metric_value"] is None
    assert "华北" not in result["aggregation_results"]
    assert "华南" in result["aggregation_results"]
    assert result["aggregation_results"]["华南"]["sum"] == 0
