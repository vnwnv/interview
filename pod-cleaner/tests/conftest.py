"""
Pytest configuration and shared fixtures.
"""

import pytest
from unittest.mock import MagicMock


@pytest.fixture
def mock_pod():
    """Create a mock Pod object factory."""
    def _create_pod(
        name: str = "test-pod",
        namespace: str = "default",
        phase: str = "Running",
        init_container_running: bool = False,
        owner_kind: str = None,
        owner_name: str = None,
        container_statuses: list = None,
        init_container_statuses_override=None,
    ):
        pod = MagicMock()
        pod.metadata.name = name
        pod.metadata.namespace = namespace
        pod.status.phase = phase
        
        # Container statuses (important for _has_image_pull_error)
        pod.status.container_statuses = container_statuses
        
        # Init container status
        if init_container_statuses_override is not None:
            pod.status.init_container_statuses = init_container_statuses_override
        elif init_container_running:
            init_status = MagicMock()
            init_status.state.running = True
            pod.status.init_container_statuses = [init_status]
        else:
            pod.status.init_container_statuses = None
        
        # Owner references
        if owner_kind and owner_name:
            owner_ref = MagicMock()
            owner_ref.kind = owner_kind
            owner_ref.name = owner_name
            pod.metadata.owner_references = [owner_ref]
        else:
            pod.metadata.owner_references = None
        
        return pod
    
    return _create_pod


@pytest.fixture
def mock_replica_set():
    """Create a mock ReplicaSet object factory."""
    def _create_rs(
        name: str = "test-rs",
        namespace: str = "default",
        owner_kind: str = None,
        owner_name: str = None,
    ):
        rs = MagicMock()
        rs.metadata.name = name
        rs.metadata.namespace = namespace
        
        if owner_kind and owner_name:
            owner_ref = MagicMock()
            owner_ref.kind = owner_kind
            owner_ref.name = owner_name
            rs.metadata.owner_references = [owner_ref]
        else:
            rs.metadata.owner_references = None
        
        return rs
    
    return _create_rs


@pytest.fixture
def mock_cluster_snapshot(mock_pod, mock_replica_set):
    """Create a mock ClusterSnapshot factory."""
    def _create_snapshot(pods=None, replica_sets=None):
        from src.cleaner import ClusterSnapshot
        
        pod_list = MagicMock()
        pod_list.items = pods if pods is not None else []
        
        rs_list = MagicMock()
        rs_list.items = replica_sets if replica_sets is not None else []
        
        return ClusterSnapshot(pods=pod_list, replica_sets=rs_list)
    
    return _create_snapshot
