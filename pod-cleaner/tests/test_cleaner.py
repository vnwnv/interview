"""
Unit tests for the cleaner module.
"""

import pytest
from unittest.mock import MagicMock, patch

from src.cleaner import (
    is_pod_unhealthy,
    get_owner_controller,
    rollout_restart,
    analyze_snapshot,
    cleanup_unhealthy_pods,
    execute_restart_task,
    RestartTask,
    _has_image_pull_error,
)


class TestHasImagePullError:
    """Tests for _has_image_pull_error function."""
    
    def test_no_container_statuses(self, mock_pod):
        """Pods with no container statuses should not have image pull error."""
        pod = mock_pod(phase="Pending")
        assert _has_image_pull_error(pod) is False
    
    def test_err_image_pull(self, mock_pod):
        """Pods with ErrImagePull should be detected."""
        pod = mock_pod(phase="Pending")
        
        container_status = MagicMock()
        container_status.state.waiting.reason = "ErrImagePull"
        pod.status.container_statuses = [container_status]
        
        assert _has_image_pull_error(pod) is True
    
    def test_image_pull_back_off(self, mock_pod):
        """Pods with ImagePullBackOff should be detected."""
        pod = mock_pod(phase="Pending")
        
        container_status = MagicMock()
        container_status.state.waiting.reason = "ImagePullBackOff"
        pod.status.container_statuses = [container_status]
        
        assert _has_image_pull_error(pod) is True
    
    def test_other_waiting_reason_not_image_pull(self, mock_pod):
        """Pods with other waiting reasons should not be detected."""
        pod = mock_pod(phase="Pending")
        
        container_status = MagicMock()
        container_status.state.waiting.reason = "CrashLoopBackOff"
        pod.status.container_statuses = [container_status]
        
        assert _has_image_pull_error(pod) is False
    
    def test_init_container_image_pull_error(self, mock_pod):
        """Image pull errors in init containers should also be detected."""
        pod = mock_pod(phase="Pending")
        
        init_status = MagicMock()
        init_status.state.waiting.reason = "ErrImagePull"
        pod.status.init_container_statuses = [init_status]
        
        assert _has_image_pull_error(pod) is True
    
    def test_running_container_no_error(self, mock_pod):
        """Running containers without waiting state should not trigger."""
        pod = mock_pod(phase="Pending")
        
        container_status = MagicMock()
        container_status.state.waiting = None
        pod.status.container_statuses = [container_status]
        
        assert _has_image_pull_error(pod) is False


class TestIsPodUnhealthy:
    """Tests for is_pod_unhealthy function."""
    
    def test_running_pod_is_healthy(self, mock_pod):
        """Running pods should be considered healthy."""
        pod = mock_pod(phase="Running")
        assert is_pod_unhealthy(pod) is False
    
    def test_succeeded_pod_is_healthy(self, mock_pod):
        """Succeeded pods (completed jobs) should be considered healthy."""
        pod = mock_pod(phase="Succeeded")
        assert is_pod_unhealthy(pod) is False
    
    def test_failed_pod_is_unhealthy(self, mock_pod):
        """Failed pods should be considered unhealthy."""
        pod = mock_pod(phase="Failed")
        assert is_pod_unhealthy(pod) is True
    
    def test_pending_pod_is_unhealthy(self, mock_pod):
        """Pending pods (not initializing) should be considered unhealthy."""
        pod = mock_pod(phase="Pending")
        assert is_pod_unhealthy(pod) is True
    
    def test_unknown_phase_is_unhealthy(self, mock_pod):
        """Unknown phase pods should be considered unhealthy."""
        pod = mock_pod(phase="Unknown")
        assert is_pod_unhealthy(pod) is True
    
    def test_init_container_running_is_healthy(self, mock_pod):
        """Pods with running init containers should be considered healthy."""
        pod = mock_pod(phase="Pending", init_container_running=True)
        assert is_pod_unhealthy(pod) is False
    
    def test_image_pull_error_is_not_unhealthy(self, mock_pod):
        """Pods with image pull errors should not be considered unhealthy."""
        container_status = MagicMock()
        container_status.state.waiting.reason = "ImagePullBackOff"
        
        pod = mock_pod(phase="Pending", container_statuses=[container_status])
        assert is_pod_unhealthy(pod) is False


class TestGetOwnerController:
    """Tests for get_owner_controller function (using snapshot)."""
    
    def test_pod_without_owner_returns_none(
        self, mock_pod, mock_cluster_snapshot
    ):
        """Pods without owner references should return None."""
        pod = mock_pod()
        pod.metadata.owner_references = None
        snapshot = mock_cluster_snapshot()
        
        result = get_owner_controller(pod, snapshot)
        assert result is None
    
    def test_statefulset_owner_returns_correct_tuple(
        self, mock_pod, mock_cluster_snapshot
    ):
        """Pods owned by StatefulSet should return correct tuple."""
        pod = mock_pod(
            namespace="production",
            owner_kind="StatefulSet",
            owner_name="my-statefulset",
        )
        snapshot = mock_cluster_snapshot()
        
        result = get_owner_controller(pod, snapshot)
        assert result == ("StatefulSet", "my-statefulset", "production")
    
    def test_replicaset_traces_to_deployment(
        self, mock_pod, mock_replica_set, mock_cluster_snapshot
    ):
        """Pods owned by ReplicaSet should trace back to Deployment via snapshot."""
        rs = mock_replica_set(
            name="my-deployment-abc123",
            namespace="default",
            owner_kind="Deployment",
            owner_name="my-deployment",
        )
        
        snapshot = mock_cluster_snapshot(replica_sets=[rs])
        
        pod = mock_pod(
            namespace="default",
            owner_kind="ReplicaSet",
            owner_name="my-deployment-abc123",
        )
        
        result = get_owner_controller(pod, snapshot)
        assert result == ("Deployment", "my-deployment", "default")
    
    def test_daemonset_owner_returns_none(
        self, mock_pod, mock_cluster_snapshot
    ):
        """Pods owned by DaemonSet should return None (not supported)."""
        pod = mock_pod(
            namespace="kube-system",
            owner_kind="DaemonSet",
            owner_name="my-daemonset",
        )
        snapshot = mock_cluster_snapshot()
        
        result = get_owner_controller(pod, snapshot)
        assert result is None
    
    def test_replicaset_not_in_snapshot_returns_none(
        self, mock_pod, mock_cluster_snapshot
    ):
        """If ReplicaSet is not in snapshot, should return None."""
        snapshot = mock_cluster_snapshot(replica_sets=[])
        
        pod = mock_pod(
            namespace="default",
            owner_kind="ReplicaSet",
            owner_name="missing-rs",
        )
        
        result = get_owner_controller(pod, snapshot)
        assert result is None


class TestRolloutRestart:
    """Tests for rollout_restart function."""
    
    @patch("src.cleaner.get_k8s_client")
    def test_deployment_rollout_restart(self, mock_get_client):
        """Rollout restart should patch Deployment correctly."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        result = rollout_restart("Deployment", "my-app", "production")
        
        mock_client.apps_v1.patch_namespaced_deployment.assert_called_once()
        call_args = mock_client.apps_v1.patch_namespaced_deployment.call_args
        assert call_args.kwargs["name"] == "my-app"
        assert call_args.kwargs["namespace"] == "production"
        assert "restartedAt" in str(call_args.kwargs["body"])
        assert result["success"] is True
    
    @patch("src.cleaner.get_k8s_client")
    def test_statefulset_rollout_restart(self, mock_get_client):
        """Rollout restart should patch StatefulSet correctly."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        result = rollout_restart("StatefulSet", "my-db", "database")
        
        mock_client.apps_v1.patch_namespaced_stateful_set.assert_called_once()
        call_args = mock_client.apps_v1.patch_namespaced_stateful_set.call_args
        assert call_args.kwargs["name"] == "my-db"
        assert call_args.kwargs["namespace"] == "database"
        assert result["success"] is True
    
    @patch("src.cleaner.get_k8s_client")
    def test_unsupported_kind_raises_error(self, mock_get_client):
        """Unsupported resource kinds should raise ValueError."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        with pytest.raises(ValueError, match="Unsupported resource kind"):
            rollout_restart("DaemonSet", "my-ds", "default")


class TestExecuteRestartTask:
    """Tests for execute_restart_task function."""
    
    @patch("src.cleaner.rollout_restart")
    def test_successful_restart(self, mock_rollout):
        """Successful restart should return success result."""
        mock_rollout.return_value = {
            "controller": "Deployment/my-app",
            "namespace": "default",
            "success": True,
        }
        
        task = RestartTask(
            kind="Deployment",
            name="my-app",
            namespace="default",
            unhealthy_pods=["pod-1"],
            pod_phases=["Failed"],
        )
        
        result = execute_restart_task(task)
        assert result["success"] is True
        assert result["controller"] == "Deployment/my-app"
        assert result["pods"] == ["pod-1"]
    
    @patch("src.cleaner.rollout_restart")
    def test_api_exception_returns_failure(self, mock_rollout):
        """ApiException should be caught and return failure result."""
        from kubernetes.client.exceptions import ApiException
        
        mock_rollout.side_effect = ApiException(status=403, reason="Forbidden")
        
        task = RestartTask(
            kind="Deployment",
            name="my-app",
            namespace="default",
            unhealthy_pods=["pod-1"],
            pod_phases=["Failed"],
        )
        
        result = execute_restart_task(task)
        assert result["success"] is False
        assert "403" in result["error"]
    
    @patch("src.cleaner.rollout_restart")
    def test_unexpected_exception_returns_failure(self, mock_rollout):
        """Unexpected exceptions should be caught and return failure result."""
        mock_rollout.side_effect = RuntimeError("Unexpected")
        
        task = RestartTask(
            kind="Deployment",
            name="my-app",
            namespace="default",
            unhealthy_pods=["pod-1"],
            pod_phases=["Failed"],
        )
        
        result = execute_restart_task(task)
        assert result["success"] is False
        assert "Unexpected" in result["error"]


class TestAnalyzeSnapshot:
    """Tests for analyze_snapshot function."""
    
    def test_excludes_kube_system_namespace(
        self, mock_pod, mock_cluster_snapshot
    ):
        """kube-system namespace should be excluded from analysis."""
        pods = [
            mock_pod(
                name="system-pod",
                namespace="kube-system",
                phase="Failed",
                owner_kind="Deployment",
                owner_name="system-deploy",
            ),
            mock_pod(
                name="app-pod",
                namespace="default",
                phase="Failed",
                owner_kind="StatefulSet",
                owner_name="my-app",
            ),
        ]
        
        snapshot = mock_cluster_snapshot(pods=pods)
        tasks = analyze_snapshot(snapshot)
        
        assert len(tasks) == 1
        assert tasks[0].namespace == "default"
    
    def test_groups_pods_by_controller(
        self, mock_pod, mock_replica_set, mock_cluster_snapshot
    ):
        """Multiple unhealthy pods from same controller should be grouped."""
        rs = mock_replica_set(
            name="my-deploy-abc123",
            namespace="default",
            owner_kind="Deployment",
            owner_name="my-deploy",
        )
        
        pods = [
            mock_pod(
                name="my-deploy-abc123-pod1",
                namespace="default",
                phase="Failed",
                owner_kind="ReplicaSet",
                owner_name="my-deploy-abc123",
            ),
            mock_pod(
                name="my-deploy-abc123-pod2",
                namespace="default",
                phase="Failed",
                owner_kind="ReplicaSet",
                owner_name="my-deploy-abc123",
            ),
        ]
        
        snapshot = mock_cluster_snapshot(pods=pods, replica_sets=[rs])
        tasks = analyze_snapshot(snapshot)
        
        assert len(tasks) == 1
        assert tasks[0].name == "my-deploy"
        assert tasks[0].kind == "Deployment"
        assert len(tasks[0].unhealthy_pods) == 2
    
    def test_skips_healthy_pods(
        self, mock_pod, mock_cluster_snapshot
    ):
        """Running pods should not generate restart tasks."""
        pods = [
            mock_pod(
                name="healthy-pod",
                namespace="default",
                phase="Running",
                owner_kind="StatefulSet",
                owner_name="my-app",
            ),
        ]
        
        snapshot = mock_cluster_snapshot(pods=pods)
        tasks = analyze_snapshot(snapshot)
        
        assert len(tasks) == 0
    
    def test_skips_pods_without_owner(
        self, mock_pod, mock_cluster_snapshot
    ):
        """Pods without owner controller should be skipped."""
        pods = [
            mock_pod(
                name="standalone-pod",
                namespace="default",
                phase="Failed",
            ),
        ]
        
        snapshot = mock_cluster_snapshot(pods=pods)
        tasks = analyze_snapshot(snapshot)
        
        assert len(tasks) == 0


def _make_api_response(items=None):
    """Create a mock K8s API list response with proper pagination fields."""
    response = MagicMock()
    response.items = items if items is not None else []
    response.metadata._continue = None  # Terminate pagination
    return response


class TestCleanupUnhealthyPods:
    """Tests for cleanup_unhealthy_pods function (snapshot-based)."""
    
    @patch("src.cleaner.send_failure_notification")
    @patch("src.cleaner.get_k8s_client")
    def test_uses_snapshot_api(
        self, mock_get_client, mock_send_notification
    ):
        """Cleanup should use list_pod_for_all_namespaces for snapshot."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        mock_client.core_v1.list_pod_for_all_namespaces.return_value = _make_api_response()
        mock_client.apps_v1.list_replica_set_for_all_namespaces.return_value = _make_api_response()
        
        result = cleanup_unhealthy_pods()
        
        mock_client.core_v1.list_pod_for_all_namespaces.assert_called_once()
        mock_client.apps_v1.list_replica_set_for_all_namespaces.assert_called_once()
        mock_client.core_v1.list_namespaced_pod.assert_not_called()
    
    @patch("src.cleaner.send_failure_notification")
    @patch("src.cleaner.get_k8s_client")
    def test_returns_correct_statistics(
        self, mock_get_client, mock_send_notification
    ):
        """Cleanup should return correct success/failure statistics."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        
        mock_client.core_v1.list_pod_for_all_namespaces.return_value = _make_api_response()
        mock_client.apps_v1.list_replica_set_for_all_namespaces.return_value = _make_api_response()
        
        result = cleanup_unhealthy_pods()
        
        assert result["success"] is True
        assert result["total_processed"] == 0
        assert result["success_count"] == 0
        assert result["failed_count"] == 0
        assert "duration_seconds" in result
    
    @patch("src.cleaner.send_failure_notification")
    @patch("src.cleaner.rollout_restart")
    @patch("src.cleaner.get_k8s_client")
    def test_restarts_unhealthy_pods(
        self, mock_get_client, mock_rollout, mock_send_notification, mock_pod
    ):
        """Cleanup should restart controllers with unhealthy pods."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_rollout.return_value = {
            "controller": "StatefulSet/my-app",
            "namespace": "default",
            "success": True,
        }
        
        pod = mock_pod(
            name="my-app-0",
            namespace="default",
            phase="Failed",
            owner_kind="StatefulSet",
            owner_name="my-app",
        )
        
        mock_client.core_v1.list_pod_for_all_namespaces.return_value = _make_api_response([pod])
        mock_client.apps_v1.list_replica_set_for_all_namespaces.return_value = _make_api_response()
        
        result = cleanup_unhealthy_pods()
        
        mock_rollout.assert_called_once_with("StatefulSet", "my-app", "default")
        assert result["success"] is True
        assert result["success_count"] == 1
    
    @patch("src.cleaner.send_failure_notification")
    @patch("src.cleaner.get_k8s_client")
    def test_api_exception_returns_failure(
        self, mock_get_client, mock_send_notification
    ):
        """Cleanup should handle API exceptions gracefully."""
        from kubernetes.client.exceptions import ApiException
        
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.core_v1.list_pod_for_all_namespaces.side_effect = ApiException(
            status=500, reason="Internal Server Error"
        )
        
        result = cleanup_unhealthy_pods()
        
        assert result["success"] is False
        assert "error" in result
        assert "duration_seconds" in result
    
    @patch("src.cleaner.send_failure_notification")
    @patch("src.cleaner.rollout_restart")
    @patch("src.cleaner.get_k8s_client")
    def test_sends_notification_on_failure(
        self, mock_get_client, mock_rollout, mock_send_notification, mock_pod
    ):
        """Cleanup should send notification when restarts fail."""
        from kubernetes.client.exceptions import ApiException
        
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_rollout.side_effect = ApiException(status=403, reason="Forbidden")
        
        pod = mock_pod(
            name="my-app-0",
            namespace="default",
            phase="Failed",
            owner_kind="StatefulSet",
            owner_name="my-app",
        )
        
        mock_client.core_v1.list_pod_for_all_namespaces.return_value = _make_api_response([pod])
        mock_client.apps_v1.list_replica_set_for_all_namespaces.return_value = _make_api_response()
        
        result = cleanup_unhealthy_pods()
        
        assert result["success"] is False
        assert result["failed_count"] == 1
        mock_send_notification.assert_called_once()
