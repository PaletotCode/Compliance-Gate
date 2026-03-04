from fastapi.testclient import TestClient

def test_get_machines_filters(client: TestClient):
    response = client.get("/api/v1/machines/filters")
    assert response.status_code == 200
    data = response.json()["data"]
    assert isinstance(data, list)
    assert len(data) >= 14 # We have at least 14 filters implemented
    
    # Check structure of the first filter
    first_filter = data[0]
    assert "key" in first_filter
    assert "label" in first_filter
    assert "severity" in first_filter
    assert "description" in first_filter
    assert "is_flag" in first_filter

def test_get_machines_table(client: TestClient):
    response = client.get("/api/v1/machines/table?page=1&size=10")
    assert response.status_code == 200
    json_resp = response.json()
    assert "data" in json_resp
    assert "items" in json_resp["data"]
    assert "meta" in json_resp["data"]
    
    meta = json_resp["data"]["meta"]
    assert meta["page"] == 1
    assert meta["size"] == 10
    
    # In-memory engine currently returns empty array if no test data is seeded
    items = json_resp["data"]["items"]
    assert isinstance(items, list)

def test_get_machines_summary(client: TestClient):
    response = client.get("/api/v1/machines/summary")
    assert response.status_code == 200
    json_resp = response.json()
    assert "data" in json_resp
    
    data = json_resp["data"]
    assert "total" in data
    assert "by_status" in data
    assert "by_flag" in data
    assert isinstance(data["by_status"], dict)
    assert isinstance(data["by_flag"], dict)
