"""
Unit tests for the main module.
"""

import signal
from unittest.mock import patch, MagicMock

from src.main import scheduled_cleanup, signal_handler


class TestScheduledCleanup:
    """Tests for scheduled_cleanup function."""
    
    @patch("src.main.update_cleanup_status")
    @patch("src.main.cleanup_unhealthy_pods")
    def test_successful_cleanup_updates_status(
        self, mock_cleanup, mock_update_status
    ):
        """Successful cleanup should update health status."""
        mock_cleanup.return_value = {
            "success": True,
            "total_processed": 0,
        }
        
        scheduled_cleanup()
        
        mock_update_status.assert_called_once()
        call_args = mock_update_status.call_args
        assert call_args.kwargs["success"] is True
        assert call_args.kwargs["cleanup_time"] is not None
    
    @patch("src.main.update_cleanup_status")
    @patch("src.main.cleanup_unhealthy_pods")
    def test_failed_cleanup_updates_status(
        self, mock_cleanup, mock_update_status
    ):
        """Failed cleanup should update health status with failure."""
        mock_cleanup.return_value = {
            "success": False,
            "error": "Some error",
        }
        
        scheduled_cleanup()
        
        mock_update_status.assert_called_once()
        call_args = mock_update_status.call_args
        assert call_args.kwargs["success"] is False
    
    @patch("src.main.update_cleanup_status")
    @patch("src.main.cleanup_unhealthy_pods")
    def test_exception_updates_status_with_failure(
        self, mock_cleanup, mock_update_status
    ):
        """Unexpected exceptions should update status with failure."""
        mock_cleanup.side_effect = RuntimeError("Connection lost")
        
        scheduled_cleanup()
        
        mock_update_status.assert_called_once()
        call_args = mock_update_status.call_args
        assert call_args.kwargs["success"] is False
        assert call_args.kwargs["cleanup_time"] is None


class TestSignalHandler:
    """Tests for signal_handler function."""
    
    @patch("src.main.sys.exit")
    @patch("src.main.set_ready")
    @patch("src.main.set_healthy")
    @patch("src.main.shutdown_event")
    def test_sets_shutdown_event(
        self, mock_event, mock_set_healthy, mock_set_ready, mock_exit
    ):
        """Signal handler should set shutdown event."""
        signal_handler(signal.SIGTERM, None)
        
        mock_event.set.assert_called_once()
    
    @patch("src.main.sys.exit")
    @patch("src.main.set_ready")
    @patch("src.main.set_healthy")
    @patch("src.main.shutdown_event")
    def test_marks_unhealthy_and_not_ready(
        self, mock_event, mock_set_healthy, mock_set_ready, mock_exit
    ):
        """Signal handler should mark service as unhealthy and not ready."""
        signal_handler(signal.SIGTERM, None)
        
        mock_set_healthy.assert_called_once_with(False)
        mock_set_ready.assert_called_once_with(False)
    
    @patch("src.main.sys.exit")
    @patch("src.main.set_ready")
    @patch("src.main.set_healthy")
    @patch("src.main.shutdown_event")
    def test_exits_with_zero(
        self, mock_event, mock_set_healthy, mock_set_ready, mock_exit
    ):
        """Signal handler should exit with code 0."""
        signal_handler(signal.SIGTERM, None)
        
        mock_exit.assert_called_once_with(0)
    
    @patch("src.main.sys.exit")
    @patch("src.main.set_ready")
    @patch("src.main.set_healthy")
    @patch("src.main.shutdown_event")
    def test_shuts_down_scheduler(
        self, mock_event, mock_set_healthy, mock_set_ready, mock_exit
    ):
        """Signal handler should shutdown the scheduler if it exists."""
        import src.main
        mock_scheduler = MagicMock()
        original = src.main.scheduler
        src.main.scheduler = mock_scheduler
        try:
            signal_handler(signal.SIGTERM, None)
            mock_scheduler.shutdown.assert_called_once_with(wait=False)
        finally:
            src.main.scheduler = original
