import pytest
from fastapi.testclient import TestClient
from etl.api import app


def test_health_returns_200():
    client = TestClient(app)
    r = client.get("/health")
    assert r.status_code == 200
    assert "ok" in r.json() or r.json().get("status") == "ok"


def test_status_returns_200_or_503_and_has_db_key():
    client = TestClient(app)
    r = client.get("/status")
    assert r.status_code in (200, 503)  # 503 if DB unavailable
    data = r.json()
    assert "database" in data or "db" in data or "status" in data


def test_scheduler_status_returns_200_or_503_has_tasks():
    client = TestClient(app)
    r = client.get("/scheduler/status")
    assert r.status_code in (200, 503)
    data = r.json()
    assert "tasks" in data
    if r.status_code == 200:
        assert isinstance(data["tasks"], list)


def test_ingest_current_run_returns_200_or_503_and_contract():
    client = TestClient(app)
    r = client.get("/ingest/current-run")
    assert r.status_code in (200, 503)
    data = r.json()
    assert "running" in data
    assert "run" in data


def test_db_info_returns_200_or_503_has_schemas_and_tables():
    client = TestClient(app)
    r = client.get("/db-info")
    assert r.status_code in (200, 503)
    if r.status_code == 200:
        data = r.json()
        assert "schemas" in data
        assert "tables" in data
        assert isinstance(data["schemas"], list)
        assert isinstance(data["tables"], list)


def test_borme_ingest_requires_anos():
    client = TestClient(app)
    r = client.post("/borme/ingest", json={})
    assert r.status_code == 422


def test_borme_anomalias_requires_anos():
    client = TestClient(app)
    r = client.post("/borme/anomalias", json={})
    assert r.status_code == 422


def test_scheduler_defaults_returns_valid_structure():
    client = TestClient(app)
    resp = client.get("/scheduler/defaults")
    assert resp.status_code == 200
    data = resp.json()
    assert "valid_exprs" in data
    assert "defaults" in data
    assert set(data["valid_exprs"]) == {"Diario", "Semanal", "Mensual", "Trimestral", "Semestral", "Anual"}
    assert isinstance(data["defaults"], list)
    assert len(data["defaults"]) > 0
    for item in data["defaults"]:
        assert "conjunto" in item
        assert "subconjunto" in item
        assert "schedule_expr" in item
        assert item["schedule_expr"] in data["valid_exprs"]
    assert any(item["conjunto"] == "nacional" for item in data["defaults"])


def test_scheduler_register_invalid_schedule_expr_returns_422():
    client = TestClient(app)
    resp = client.post("/scheduler/register", json={"schedule_expr": "Quincenal"})
    assert resp.status_code == 422


def test_scheduler_register_per_task_invalid_schedule_expr_returns_422():
    client = TestClient(app)
    resp = client.post(
        "/scheduler/register",
        json={"tasks": [{"conjunto": "nacional", "subconjunto": "licitaciones", "schedule_expr": "Quincenal"}]},
    )
    assert resp.status_code == 422
