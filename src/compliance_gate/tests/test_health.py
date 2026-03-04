from fastapi.testclient import TestClient

def test_health_check(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["data"] == "OK"

def test_ready_check(client: TestClient):
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json()["data"] == "Ready"

def test_version_check(client: TestClient):
    response = client.get("/version")
    assert response.status_code == 200
    assert response.json()["success"] is True
