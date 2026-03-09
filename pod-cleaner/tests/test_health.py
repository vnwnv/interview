"""
Unit tests for the health check module.
"""

import pytest

from src.health import app, set_healthy, set_ready, update_cleanup_status, _health_state, _state_lock


@pytest.fixture
def client():
    """Create a Flask test client."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def reset_health_state():
    """Reset health state before each test."""
    with _state_lock:
        _health_state["is_healthy"] = True
        _health_state["is_ready"] = True
        _health_state["last_cleanup_success"] = True
        _health_state["last_cleanup_time"] = None
    yield


class TestHealthEndpoint:
    """Tests for /health endpoint."""
    
    def test_healthy_returns_200(self, client):
        """Should return 200 when service is healthy."""
        response = client.get("/health")
        
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "healthy"
        assert "last_cleanup_success" in data
        assert "last_cleanup_time" in data
    
    def test_unhealthy_returns_503(self, client):
        """Should return 503 when service is unhealthy."""
        set_healthy(False)
        
        response = client.get("/health")
        
        assert response.status_code == 503
        data = response.get_json()
        assert data["status"] == "unhealthy"
    
    def test_health_includes_cleanup_status(self, client):
        """Should include last cleanup info in response."""
        update_cleanup_status(success=True, cleanup_time="2026-02-08T10:00:00Z")
        
        response = client.get("/health")
        
        data = response.get_json()
        assert data["last_cleanup_success"] is True
        assert data["last_cleanup_time"] == "2026-02-08T10:00:00Z"


class TestReadyEndpoint:
    """Tests for /ready endpoint."""
    
    def test_ready_returns_200(self, client):
        """Should return 200 when service is ready."""
        response = client.get("/ready")
        
        assert response.status_code == 200
        data = response.get_json()
        assert data["status"] == "ready"
    
    def test_not_ready_returns_503(self, client):
        """Should return 503 when service is not ready."""
        set_ready(False)
        
        response = client.get("/ready")
        
        assert response.status_code == 503
        data = response.get_json()
        assert data["status"] == "not ready"


class TestRootEndpoint:
    """Tests for / endpoint."""
    
    def test_root_returns_service_info(self, client):
        """Should return service info with available endpoints."""
        response = client.get("/")
        
        assert response.status_code == 200
        data = response.get_json()
        assert data["service"] == "pod-cleaner"
        assert "/health" in data["endpoints"]
        assert "/ready" in data["endpoints"]


class TestStateManagement:
    """Tests for state management functions."""
    
    def test_set_healthy_true(self):
        """set_healthy(True) should mark service as healthy."""
        set_healthy(False)
        set_healthy(True)
        
        with _state_lock:
            assert _health_state["is_healthy"] is True
    
    def test_set_healthy_false(self):
        """set_healthy(False) should mark service as unhealthy."""
        set_healthy(False)
        
        with _state_lock:
            assert _health_state["is_healthy"] is False
    
    def test_set_ready_true(self):
        """set_ready(True) should mark service as ready."""
        set_ready(False)
        set_ready(True)
        
        with _state_lock:
            assert _health_state["is_ready"] is True
    
    def test_set_ready_false(self):
        """set_ready(False) should mark service as not ready."""
        set_ready(False)
        
        with _state_lock:
            assert _health_state["is_ready"] is False
    
    def test_update_cleanup_status_success(self):
        """update_cleanup_status should update both fields."""
        update_cleanup_status(success=True, cleanup_time="2026-02-08T10:00:00Z")
        
        with _state_lock:
            assert _health_state["last_cleanup_success"] is True
            assert _health_state["last_cleanup_time"] == "2026-02-08T10:00:00Z"
    
    def test_update_cleanup_status_failure(self):
        """update_cleanup_status should record failure."""
        update_cleanup_status(success=False, cleanup_time=None)
        
        with _state_lock:
            assert _health_state["last_cleanup_success"] is False
            assert _health_state["last_cleanup_time"] is None
