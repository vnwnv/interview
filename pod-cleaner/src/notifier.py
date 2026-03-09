"""
Notification module for sending alerts via ntfy.

Provides functionality to send notifications when pod cleanup fails.
"""

import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)


def _get_ntfy_config() -> tuple[str | None, str, int]:
    """
    Read ntfy configuration from environment variables.
    
    Returns:
        Tuple of (ntfy_url, ntfy_token, ntfy_timeout).
    """
    return (
        os.getenv("NTFY_URL"),
        os.getenv("NTFY_TOKEN", ""),
        int(os.getenv("NTFY_TIMEOUT", "10")),
    )


def send_failure_notification(failed_pods: list[dict[str, Any]]) -> bool:
    """
    Send notification for failed pod cleanup operations.
    
    Args:
        failed_pods: List of dictionaries containing failed pod information.
                    Each dict should have: pod, namespace, controller, error
    
    Returns:
        bool: True if notification was sent successfully, False otherwise.
    """
    ntfy_url, ntfy_token, ntfy_timeout = _get_ntfy_config()
    
    if not ntfy_url:
        logger.warning("NTFY_URL not configured, skipping notification")
        return False
    
    if not failed_pods:
        logger.debug("No failed pods to notify about")
        return True
    
    # Build notification message
    message = _build_notification_message(failed_pods)
    
    # Prepare headers
    headers = {
        "Title": "Pod Cleaner Alert",
        "Priority": "high",
        "Tags": "warning,kubernetes",
    }
    
    if ntfy_token:
        headers["Authorization"] = f"Bearer {ntfy_token}"
    
    try:
        response = requests.post(
            ntfy_url,
            data=message.encode("utf-8"),
            headers=headers,
            timeout=ntfy_timeout,
        )
        response.raise_for_status()
        logger.info(f"Notification sent successfully to {ntfy_url}")
        return True
    except requests.exceptions.Timeout:
        logger.error(f"Notification timeout after {ntfy_timeout}s")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send notification: {e}")
        return False


def _build_notification_message(failed_pods: list[dict[str, Any]]) -> str:
    """
    Build the notification message content.
    
    Args:
        failed_pods: List of failed pod information dictionaries.
    
    Returns:
        str: Formatted notification message.
    """
    lines = [
        "Pod Cleanup Failed Alert",
        "=" * 30,
        "",
        f"Failed to restart {len(failed_pods)} pod(s):",
        "",
    ]
    
    for pod in failed_pods:
        lines.extend([
            f"- {pod.get('namespace', 'unknown')}/{pod.get('pod', 'unknown')}",
            f"  Controller: {pod.get('controller', 'unknown')}",
            f"  Error: {pod.get('error', 'unknown error')}",
            "",
        ])
    
    return "\n".join(lines)
