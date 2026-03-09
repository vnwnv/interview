"""
Kubernetes client singleton module.

Provides thread-safe singleton access to Kubernetes API clients.
"""

import logging
import threading
from typing import Optional

from kubernetes import client, config
from kubernetes.client import AppsV1Api, CoreV1Api

logger = logging.getLogger(__name__)


class K8sClient:
    """
    Thread-safe singleton for Kubernetes API clients.
    
    This class ensures that only one instance of the Kubernetes client
    is created and shared across all threads, reducing resource consumption
    and connection overhead.
    
    Usage:
        client = K8sClient()
        pods = client.core_v1.list_namespaced_pod("default")
    """
    
    _instance: Optional["K8sClient"] = None
    _lock: threading.Lock = threading.Lock()
    
    def __new__(cls) -> "K8sClient":
        if cls._instance is None:
            with cls._lock:
                # Double-checked locking pattern
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        
        self._initialize_client()
        self._initialized = True
    
    def _initialize_client(self) -> None:
        """Initialize Kubernetes client configuration."""
        try:
            # Try in-cluster config first (when running inside K8s)
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            try:
                # Fall back to kubeconfig (for local development)
                config.load_kube_config()
                logger.info("Loaded kubeconfig configuration")
            except config.ConfigException as e:
                logger.error(f"Failed to load Kubernetes configuration: {e}")
                raise
        
        self._core_v1 = CoreV1Api()
        self._apps_v1 = AppsV1Api()
    
    @property
    def core_v1(self) -> CoreV1Api:
        """Get CoreV1Api client for core resources (pods, namespaces, etc.)."""
        return self._core_v1
    
    @property
    def apps_v1(self) -> AppsV1Api:
        """Get AppsV1Api client for apps resources (deployments, statefulsets, etc.)."""
        return self._apps_v1


def get_k8s_client() -> K8sClient:
    """
    Convenience function to get the K8sClient singleton instance.
    
    Returns:
        K8sClient: The singleton Kubernetes client instance.
    """
    return K8sClient()
