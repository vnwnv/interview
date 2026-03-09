"""
Health check HTTP endpoint module.

Provides liveness and readiness probes for Kubernetes.
"""

import logging
import threading
from typing import Any

from flask import Flask, jsonify

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Health state management
_health_state = {
    "is_healthy": True,
    "is_ready": True,
    "last_cleanup_success": True,
    "last_cleanup_time": None,
}
_state_lock = threading.Lock()


@app.route("/health")
def health() -> tuple[dict[str, Any], int]:
    """
    Liveness probe endpoint.
    
    Returns 200 if the application is running and healthy.
    Returns 503 if the application is in an unhealthy state.
    """
    with _state_lock:
        if _health_state["is_healthy"]:
            return jsonify({
                "status": "healthy",
                "last_cleanup_success": _health_state["last_cleanup_success"],
                "last_cleanup_time": _health_state["last_cleanup_time"],
            }), 200
        return jsonify({"status": "unhealthy"}), 503


@app.route("/ready")
def ready() -> tuple[dict[str, Any], int]:
    """
    Readiness probe endpoint.
    
    Returns 200 if the application is ready to receive traffic.
    Returns 503 if the application is not ready.
    """
    with _state_lock:
        if _health_state["is_ready"]:
            return jsonify({"status": "ready"}), 200
        return jsonify({"status": "not ready"}), 503


@app.route("/")
def root() -> tuple[dict[str, Any], int]:
    """Root endpoint with basic info."""
    return jsonify({
        "service": "pod-cleaner",
        "endpoints": {
            "/health": "Liveness probe",
            "/ready": "Readiness probe",
        },
    }), 200


def set_healthy(healthy: bool) -> None:
    """
    Set the health state of the application.
    
    Args:
        healthy: Whether the application is healthy.
    """
    with _state_lock:
        _health_state["is_healthy"] = healthy


def set_ready(ready: bool) -> None:
    """
    Set the readiness state of the application.
    
    Args:
        ready: Whether the application is ready.
    """
    with _state_lock:
        _health_state["is_ready"] = ready


def update_cleanup_status(success: bool, cleanup_time: str) -> None:
    """
    Update the last cleanup status.
    
    Args:
        success: Whether the last cleanup was successful.
        cleanup_time: ISO format timestamp of the cleanup.
    """
    with _state_lock:
        _health_state["last_cleanup_success"] = success
        _health_state["last_cleanup_time"] = cleanup_time


def run_health_server(port: int = 8080) -> None:
    """
    Run the health check HTTP server.
    
    Uses waitress as the production WSGI server.
    This should be run in a separate daemon thread.
    
    Args:
        port: Port to listen on (default: 8080).
    """
    from waitress import serve

    # Suppress waitress internal logging
    logging.getLogger("waitress").setLevel(logging.WARNING)
    
    logger.info(f"Starting health check server on port {port}")
    serve(app, host="0.0.0.0", port=port, _quiet=True)
