"""
Pod Cleaner - Main entry point.

A Kubernetes service that automatically detects and restarts unhealthy pods
by triggering rollout restarts on their parent Deployment/StatefulSet.
"""

import logging
import os
import signal
import sys
import threading
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .cleaner import cleanup_unhealthy_pods
from .health import run_health_server, set_healthy, set_ready, update_cleanup_status
from .k8s_client import get_k8s_client

# Configuration
CLEANUP_INTERVAL_MINUTES = int(os.getenv("CLEANUP_INTERVAL_MINUTES", "10"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Global scheduler reference for graceful shutdown
scheduler: BlockingScheduler = None
shutdown_event = threading.Event()


def signal_handler(signum: int, frame) -> None:
    """
    Handle termination signals for graceful shutdown.
    
    Args:
        signum: Signal number received.
        frame: Current stack frame.
    """
    sig_name = signal.Signals(signum).name
    logger.info(f"Received signal {sig_name}, initiating graceful shutdown...")
    
    shutdown_event.set()
    set_healthy(False)
    set_ready(False)
    
    if scheduler:
        scheduler.shutdown(wait=False)
    
    logger.info("Graceful shutdown completed")
    sys.exit(0)


def scheduled_cleanup() -> None:
    """
    Scheduled job wrapper for cleanup_unhealthy_pods.
    
    Updates health status based on cleanup results.
    """
    try:
        result = cleanup_unhealthy_pods()
        
        # Update health check status
        cleanup_time = datetime.now(timezone.utc).isoformat()
        update_cleanup_status(
            success=result.get("success", False),
            cleanup_time=cleanup_time,
        )
        
    except Exception as e:
        logger.exception(f"Unexpected error during cleanup: {e}")
        update_cleanup_status(success=False, cleanup_time=None)


def main() -> None:
    """Main entry point for the Pod Cleaner service."""
    global scheduler
    
    logger.info("=" * 50)
    logger.info("Pod Cleaner Service Starting")
    logger.info("=" * 50)
    logger.info(f"Configuration:")
    logger.info(f"  - Cleanup interval: {CLEANUP_INTERVAL_MINUTES} minutes")
    logger.info(f"  - Health check port: {HEALTH_PORT}")
    logger.info(f"  - Log level: {LOG_LEVEL}")
    logger.info(f"  - Max workers: {os.getenv('MAX_WORKERS', '10')}")
    logger.info(f"  - NTFY URL: {os.getenv('NTFY_URL', 'not configured')}")
    logger.info("=" * 50)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Verify Kubernetes connectivity (fail-fast)
    try:
        get_k8s_client()
    except Exception as e:
        logger.error(f"Failed to initialize Kubernetes client: {e}")
        logger.error("Exiting — cannot operate without Kubernetes access")
        sys.exit(1)
    
    # Start health check server in a daemon thread
    health_thread = threading.Thread(
        target=run_health_server,
        kwargs={"port": HEALTH_PORT},
        daemon=True,
        name="health-server",
    )
    health_thread.start()
    logger.info(f"Health check server started on port {HEALTH_PORT}")
    
    # Mark service as ready
    set_ready(True)
    set_healthy(True)
    
    # Run initial cleanup immediately
    logger.info("Running initial cleanup...")
    scheduled_cleanup()
    
    # Setup scheduler
    scheduler = BlockingScheduler()
    scheduler.add_job(
        scheduled_cleanup,
        trigger=IntervalTrigger(minutes=CLEANUP_INTERVAL_MINUTES),
        id="pod_cleanup",
        name="Pod Cleanup Job",
        max_instances=1,  # Ensure only one instance runs at a time
        coalesce=True,    # Combine missed executions
    )
    
    logger.info(
        f"Scheduler configured: running every {CLEANUP_INTERVAL_MINUTES} minutes"
    )
    logger.info("Pod Cleaner is now running. Press Ctrl+C to stop.")
    
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
