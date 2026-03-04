from fastapi.testclient import TestClient

def test_machines_filters_contract(client: TestClient):
    response = client.get("/api/v1/machines/filters")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["data"], list)

def test_machines_table_contract(client: TestClient):
    response = client.get("/api/v1/machines/table?page=1&size=10")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "items" in data["data"]
    assert "meta" in data["data"]

def test_machines_summary_contract(client: TestClient):
    response = client.get("/api/v1/machines/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "total" in data["data"]

def test_machines_timeline_contract(client: TestClient):
    response = client.get("/api/v1/machines/timeline")
    assert response.status_code == 200

def test_machines_history_contract(client: TestClient):
    response = client.get("/api/v1/machines/items/test-id/history")
    assert response.status_code == 200
