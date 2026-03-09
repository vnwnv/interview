"""
Unit tests for the notifier module.
"""

import pytest
from unittest.mock import patch, MagicMock

from src.notifier import send_failure_notification, _build_notification_message


class TestSendFailureNotification:
    """Tests for send_failure_notification function."""
    
    @patch.dict("os.environ", {"NTFY_URL": ""}, clear=False)
    def test_no_notification_without_url(self):
        """Should return False when NTFY_URL is not configured."""
        failed_pods = [{"pod": "test", "namespace": "default", "error": "test"}]
        result = send_failure_notification(failed_pods)
        assert result is False
    
    @patch("src.notifier.requests.post")
    @patch.dict(
        "os.environ",
        {"NTFY_URL": "http://ntfy.example.com/test"},
        clear=False,
    )
    def test_sends_notification_successfully(self, mock_post):
        """Should send notification when NTFY_URL is configured."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        failed_pods = [
            {
                "pod": "my-pod-abc",
                "namespace": "production",
                "controller": "Deployment/my-app",
                "error": "Permission denied",
            }
        ]
        
        result = send_failure_notification(failed_pods)
        
        assert result is True
        mock_post.assert_called_once()
        
        # Verify request content
        call_args = mock_post.call_args
        assert "http://ntfy.example.com/test" in str(call_args)
        assert "production/my-pod-abc" in call_args.kwargs["data"].decode()
    
    @patch("src.notifier.requests.post")
    @patch.dict(
        "os.environ",
        {
            "NTFY_URL": "http://ntfy.example.com/test",
            "NTFY_TOKEN": "secret-token",
        },
        clear=False,
    )
    def test_includes_auth_header_when_token_set(self, mock_post):
        """Should include Authorization header when NTFY_TOKEN is set."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        failed_pods = [{"pod": "test", "namespace": "default", "error": "test"}]
        send_failure_notification(failed_pods)
        
        call_args = mock_post.call_args
        headers = call_args.kwargs.get("headers", {})
        assert "Authorization" in headers
        assert "Bearer secret-token" in headers["Authorization"]
    
    @patch.dict(
        "os.environ",
        {"NTFY_URL": "http://ntfy.example.com/test"},
        clear=False,
    )
    def test_empty_failed_pods_returns_true(self):
        """Should return True when there are no failed pods."""
        result = send_failure_notification([])
        assert result is True
    
    @patch("src.notifier.requests.post")
    @patch.dict(
        "os.environ",
        {"NTFY_URL": "http://ntfy.example.com/test"},
        clear=False,
    )
    def test_handles_request_timeout(self, mock_post):
        """Should handle request timeout gracefully."""
        import requests
        
        mock_post.side_effect = requests.exceptions.Timeout("Connection timed out")
        
        failed_pods = [{"pod": "test", "namespace": "default", "error": "test"}]
        result = send_failure_notification(failed_pods)
        
        assert result is False
    
    @patch("src.notifier.requests.post")
    @patch.dict(
        "os.environ",
        {"NTFY_URL": "http://ntfy.example.com/test"},
        clear=False,
    )
    def test_handles_request_exception(self, mock_post):
        """Should handle request exceptions gracefully."""
        import requests
        
        mock_post.side_effect = requests.exceptions.ConnectionError("Failed")
        
        failed_pods = [{"pod": "test", "namespace": "default", "error": "test"}]
        result = send_failure_notification(failed_pods)
        
        assert result is False


class TestBuildNotificationMessage:
    """Tests for _build_notification_message function."""
    
    def test_message_contains_all_pod_info(self):
        """Message should contain all failed pod information."""
        failed_pods = [
            {
                "pod": "pod-1",
                "namespace": "ns-1",
                "controller": "Deployment/app-1",
                "error": "Error 1",
            },
            {
                "pod": "pod-2",
                "namespace": "ns-2",
                "controller": "StatefulSet/db-1",
                "error": "Error 2",
            },
        ]
        
        message = _build_notification_message(failed_pods)
        
        assert "pod-1" in message
        assert "ns-1" in message
        assert "Deployment/app-1" in message
        assert "Error 1" in message
        assert "pod-2" in message
        assert "ns-2" in message
        assert "StatefulSet/db-1" in message
        assert "Error 2" in message
    
    def test_message_has_correct_count(self):
        """Message should show correct count of failed pods."""
        failed_pods = [
            {"pod": f"pod-{i}", "namespace": "default", "error": "test"}
            for i in range(5)
        ]
        
        message = _build_notification_message(failed_pods)
        
        assert "5 pod(s)" in message
