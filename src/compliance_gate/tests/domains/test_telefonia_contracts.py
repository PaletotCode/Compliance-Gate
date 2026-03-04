from fastapi.testclient import TestClient

def test_telefonia_filters_contract(client: TestClient):
    response = client.get("/api/v1/telefonia/filters")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert isinstance(data["data"], list)

def test_telefonia_table_contract(client: TestClient):
    response = client.get("/api/v1/telefonia/table?page=1&size=10")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "items" in data["data"]
    assert "meta" in data["data"]

def test_telefonia_summary_contract(client: TestClient):
    response = client.get("/api/v1/telefonia/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert "total" in data["data"]
