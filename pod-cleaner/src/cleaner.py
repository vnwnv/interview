"""
Pod cleaner core logic module.

Contains the main logic for detecting unhealthy pods and triggering rollout restarts.

Architecture: Snapshot-based Processing
- Fetches a complete cluster snapshot (all pods + replicasets) in 2 API calls
- Builds in-memory indexes for efficient lookups
- Processes unhealthy pods concurrently without additional API queries
- Significantly reduces API server load compared to per-namespace scanning
"""

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from kubernetes.client import V1Pod, V1ReplicaSet, V1ReplicaSetList, V1PodList
from kubernetes.client.exceptions import ApiException

from .k8s_client import get_k8s_client
from .notifier import send_failure_notification

logger = logging.getLogger(__name__)

# Configuration
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "500"))
EXCLUDED_NAMESPACES = {"kube-system"}


@dataclass
class ClusterSnapshot:
    """
    Immutable snapshot of cluster state at a point in time.
    
    Contains all pods and replicasets with pre-built indexes for efficient lookups.
    """
    pods: V1PodList
    replica_sets: V1ReplicaSetList
    # Index: (namespace, rs_name) -> ReplicaSet
    _rs_index: dict[tuple[str, str], V1ReplicaSet] = None
    
    def __post_init__(self):
        """Build indexes after initialization."""
        self._rs_index = {
            (rs.metadata.namespace, rs.metadata.name): rs
            for rs in self.replica_sets.items
        }
    
    def get_replica_set(self, namespace: str, name: str) -> Optional[V1ReplicaSet]:
        """Get a ReplicaSet by namespace and name from the snapshot."""
        return self._rs_index.get((namespace, name))


def _paginated_list_pods(k8s, page_size: int) -> list[V1Pod]:
    """
    Fetch all pods using pagination.
    
    Args:
        k8s: Kubernetes client.
        page_size: Number of items per page.
    
    Returns:
        List of all pods in the cluster.
    """
    all_pods: list[V1Pod] = []
    _continue: Optional[str] = None
    page_count = 0
    
    while True:
        page_count += 1
        response = k8s.core_v1.list_pod_for_all_namespaces(
            limit=page_size,
            _continue=_continue
        )
        all_pods.extend(response.items)
        
        _continue = response.metadata._continue
        if not _continue:
            break
        
        logger.debug(f"Fetched pods page {page_count}, total so far: {len(all_pods)}")
    
    return all_pods


def _paginated_list_replica_sets(k8s, page_size: int) -> list[V1ReplicaSet]:
    """
    Fetch all ReplicaSets using pagination.
    
    Args:
        k8s: Kubernetes client.
        page_size: Number of items per page.
    
    Returns:
        List of all ReplicaSets in the cluster.
    """
    all_rs: list[V1ReplicaSet] = []
    _continue: Optional[str] = None
    page_count = 0
    
    while True:
        page_count += 1
        response = k8s.apps_v1.list_replica_set_for_all_namespaces(
            limit=page_size,
            _continue=_continue
        )
        all_rs.extend(response.items)
        
        _continue = response.metadata._continue
        if not _continue:
            break
        
        logger.debug(f"Fetched replicasets page {page_count}, total so far: {len(all_rs)}")
    
    return all_rs


def fetch_cluster_snapshot() -> ClusterSnapshot:
    """
    Fetch a complete cluster snapshot using paginated API calls.
    
    Uses pagination to handle large clusters efficiently, avoiding memory
    pressure and API server timeouts. Page size is controlled by the
    PAGE_SIZE environment variable (default: 500).
    
    Returns:
        ClusterSnapshot containing all pods and replicasets.
    
    Raises:
        ApiException: If API calls fail.
    """
    k8s = get_k8s_client()
    
    logger.info(f"Fetching cluster snapshot (page_size={PAGE_SIZE})...")
    
    # Fetch all resources using pagination
    all_pods = _paginated_list_pods(k8s, PAGE_SIZE)
    all_rs = _paginated_list_replica_sets(k8s, PAGE_SIZE)
    
    logger.info(
        f"Snapshot captured: {len(all_pods)} pods, "
        f"{len(all_rs)} replicasets"
    )
    
    # Wrap in list objects for compatibility with ClusterSnapshot
    pods_list = V1PodList(items=all_pods)
    rs_list = V1ReplicaSetList(items=all_rs)
    
    return ClusterSnapshot(pods=pods_list, replica_sets=rs_list)


def _has_image_pull_error(pod: V1Pod) -> bool:
    """
    Check if any container in the pod is stuck due to image pull errors.
    
    Restarting pods with image pull errors is pointless because the
    underlying issue (wrong image name/tag, missing registry credentials, etc.)
    won't be resolved by a rollout restart.
    
    Args:
        pod: Kubernetes Pod object.
    
    Returns:
        bool: True if any container has an image pull error, False otherwise.
    """
    IMAGE_PULL_REASONS = {"ErrImagePull", "ImagePullBackOff"}
    
    for statuses in (pod.status.container_statuses, pod.status.init_container_statuses):
        if not statuses:
            continue
        for status in statuses:
            if status.state and status.state.waiting:
                if status.state.waiting.reason in IMAGE_PULL_REASONS:
                    return True
    return False


def is_pod_unhealthy(pod: V1Pod) -> bool:
    """
    Determine if a pod is unhealthy and needs to be restarted.
    
    A pod is considered unhealthy if:
    - Its phase is not "Running"
    - AND it's not currently running init containers
    - AND it's not stuck on image pull errors (restart won't help)
    
    Args:
        pod: Kubernetes Pod object.
    
    Returns:
        bool: True if the pod is unhealthy, False otherwise.
    """
    # Running pods are healthy
    if pod.status.phase == "Running":
        return False
    
    # Succeeded pods (completed jobs) are not unhealthy
    if pod.status.phase == "Succeeded":
        return False
    
    # Check if init containers are still running (pod is initializing)
    if pod.status.init_container_statuses:
        for init_status in pod.status.init_container_statuses:
            if init_status.state and init_status.state.running:
                return False
    
    # Skip pods with image pull errors — restarting won't fix them
    if _has_image_pull_error(pod):
        logger.debug(
            f"[{pod.metadata.namespace}] Skipping pod {pod.metadata.name}: "
            "image pull error (restart won't help)"
        )
        return False
    
    return True


def get_owner_controller(
    pod: V1Pod, 
    snapshot: ClusterSnapshot
) -> Optional[tuple[str, str, str]]:
    """
    Find the owner controller (Deployment or StatefulSet) of a pod.
    
    Uses the pre-built snapshot index for O(1) ReplicaSet lookups,
    avoiding additional API calls.
    
    Args:
        pod: Kubernetes Pod object.
        snapshot: Cluster snapshot with ReplicaSet index.
    
    Returns:
        Tuple of (kind, name, namespace) if found, None otherwise.
    """
    if not pod.metadata.owner_references:
        return None
    
    namespace = pod.metadata.namespace
    
    for owner in pod.metadata.owner_references:
        if owner.kind == "ReplicaSet":
            # Trace ReplicaSet -> Deployment using snapshot index
            rs = snapshot.get_replica_set(namespace, owner.name)
            if rs and rs.metadata.owner_references:
                for rs_owner in rs.metadata.owner_references:
                    if rs_owner.kind == "Deployment":
                        return ("Deployment", rs_owner.name, namespace)
        
        elif owner.kind == "StatefulSet":
            return ("StatefulSet", owner.name, namespace)
    
    # Skip pods not owned by Deployment or StatefulSet
    return None


def rollout_restart(kind: str, name: str, namespace: str) -> dict[str, Any]:
    """
    Trigger a rollout restart for a Deployment or StatefulSet.
    
    This works by patching the pod template with a restart annotation,
    which triggers Kubernetes to recreate all pods.
    
    Args:
        kind: Resource kind ("Deployment" or "StatefulSet").
        name: Resource name.
        namespace: Resource namespace.
    
    Returns:
        Dictionary with restart result.
    
    Raises:
        ApiException: If the patch operation fails.
    """
    k8s = get_k8s_client()
    
    patch = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": datetime.now(
                            timezone.utc
                        ).isoformat()
                    }
                }
            }
        }
    }
    
    if kind == "Deployment":
        k8s.apps_v1.patch_namespaced_deployment(
            name=name,
            namespace=namespace,
            body=patch,
        )
        logger.info(f"[{namespace}] Triggered rollout restart: Deployment/{name}")
    elif kind == "StatefulSet":
        k8s.apps_v1.patch_namespaced_stateful_set(
            name=name,
            namespace=namespace,
            body=patch,
        )
        logger.info(f"[{namespace}] Triggered rollout restart: StatefulSet/{name}")
    else:
        raise ValueError(f"Unsupported resource kind: {kind}")
    
    return {
        "controller": f"{kind}/{name}",
        "namespace": namespace,
        "success": True,
    }


@dataclass
class RestartTask:
    """A task to restart a controller due to unhealthy pod(s)."""
    kind: str
    name: str
    namespace: str
    unhealthy_pods: list[str]  # List of pod names that triggered this restart
    pod_phases: list[str]      # Corresponding phases


def execute_restart_task(task: RestartTask) -> dict[str, Any]:
    """
    Execute a single restart task.
    
    Args:
        task: RestartTask to execute.
    
    Returns:
        Dictionary with result information.
    """
    try:
        rollout_restart(task.kind, task.name, task.namespace)
        return {
            "controller": f"{task.kind}/{task.name}",
            "namespace": task.namespace,
            "pods": task.unhealthy_pods,
            "phases": task.pod_phases,
            "success": True,
        }
    except ApiException as e:
        error_msg = f"{e.status}: {e.reason}"
        logger.error(
            f"[{task.namespace}] Failed to restart {task.kind}/{task.name}: "
            f"{error_msg}"
        )
        return {
            "controller": f"{task.kind}/{task.name}",
            "namespace": task.namespace,
            "pods": task.unhealthy_pods,
            "phases": task.pod_phases,
            "success": False,
            "error": error_msg,
        }
    except Exception as e:
        logger.error(
            f"[{task.namespace}] Unexpected error restarting "
            f"{task.kind}/{task.name}: {e}"
        )
        return {
            "controller": f"{task.kind}/{task.name}",
            "namespace": task.namespace,
            "pods": task.unhealthy_pods,
            "phases": task.pod_phases,
            "success": False,
            "error": str(e),
        }


def analyze_snapshot(snapshot: ClusterSnapshot) -> list[RestartTask]:
    """
    Analyze the cluster snapshot and identify controllers that need restart.
    
    This is a pure function that processes the snapshot in memory,
    grouping unhealthy pods by their owner controller.
    
    Args:
        snapshot: Cluster snapshot to analyze.
    
    Returns:
        List of RestartTask objects for controllers that need restart.
    """
    # Group unhealthy pods by controller
    # Key: (kind, name, namespace), Value: (pod_names, phases)
    controller_pods: dict[tuple[str, str, str], tuple[list[str], list[str]]] = {}
    
    for pod in snapshot.pods.items:
        namespace = pod.metadata.namespace
        pod_name = pod.metadata.name
        
        # Skip excluded namespaces
        if namespace in EXCLUDED_NAMESPACES:
            continue
        
        # Skip healthy pods
        if not is_pod_unhealthy(pod):
            continue
        
        logger.info(
            f"[{namespace}] Found unhealthy pod: {pod_name} "
            f"(phase: {pod.status.phase})"
        )
        
        # Find owner controller using snapshot (no API call)
        owner = get_owner_controller(pod, snapshot)
        if not owner:
            logger.debug(
                f"[{namespace}] Skipping pod {pod_name}: "
                "not owned by Deployment/StatefulSet"
            )
            continue
        
        # Group by controller
        controller_key = owner
        if controller_key not in controller_pods:
            controller_pods[controller_key] = ([], [])
        controller_pods[controller_key][0].append(pod_name)
        controller_pods[controller_key][1].append(pod.status.phase)
    
    # Convert to RestartTasks
    tasks = [
        RestartTask(
            kind=key[0],
            name=key[1],
            namespace=key[2],
            unhealthy_pods=pods,
            pod_phases=phases,
        )
        for key, (pods, phases) in controller_pods.items()
    ]
    
    return tasks


def cleanup_unhealthy_pods() -> dict[str, Any]:
    """
    Main cleanup function that scans all pods and restarts unhealthy ones.
    
    Architecture:
    1. Fetch a complete cluster snapshot (2 API calls)
    2. Analyze snapshot in memory to identify unhealthy pods
    3. Group by controller to avoid duplicate restarts
    4. Execute restarts concurrently
    
    Returns:
        Dictionary containing cleanup results and statistics.
    """
    start_time = time.time()
    logger.info("Starting unhealthy pod cleanup...")
    
    # Phase 1: Fetch cluster snapshot (2 API calls)
    try:
        snapshot = fetch_cluster_snapshot()
    except ApiException as e:
        logger.error(f"Failed to fetch cluster snapshot: {e.reason}")
        return {
            "success": False,
            "error": str(e),
            "duration_seconds": time.time() - start_time,
        }
    
    # Phase 2: Analyze snapshot (pure in-memory processing)
    tasks = analyze_snapshot(snapshot)
    
    if not tasks:
        duration = time.time() - start_time
        logger.info("No unhealthy pods found")
        logger.info(f"Cleanup completed in {duration:.2f}s")
        return {
            "success": True,
            "total_processed": 0,
            "success_count": 0,
            "failed_count": 0,
            "success_results": [],
            "failed_results": [],
            "duration_seconds": duration,
        }
    
    logger.info(
        f"Found {len(tasks)} controller(s) to restart, "
        f"using {MAX_WORKERS} workers"
    )
    
    # Phase 3: Execute restarts concurrently
    all_results: list[dict[str, Any]] = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(execute_restart_task, task): task
            for task in tasks
        }
        
        for future in as_completed(future_to_task):
            task = future_to_task[future]
            try:
                result = future.result()
                all_results.append(result)
            except Exception as e:
                logger.error(
                    f"[{task.namespace}] Unexpected error processing "
                    f"{task.kind}/{task.name}: {e}"
                )
                all_results.append({
                    "controller": f"{task.kind}/{task.name}",
                    "namespace": task.namespace,
                    "pods": task.unhealthy_pods,
                    "phases": task.pod_phases,
                    "success": False,
                    "error": str(e),
                })
    
    # Categorize results
    success_results = [r for r in all_results if r.get("success")]
    failed_results = [r for r in all_results if not r.get("success")]
    
    duration = time.time() - start_time
    
    # Log summary
    if success_results:
        logger.info(
            f"Successfully restarted {len(success_results)} controller(s): "
            f"{[r['controller'] for r in success_results]}"
        )
    
    if failed_results:
        logger.error(
            f"Failed to restart {len(failed_results)} controller(s): "
            f"{[r['controller'] for r in failed_results]}"
        )
        # Send notification for failures
        send_failure_notification(failed_results)
    
    logger.info(f"Cleanup completed in {duration:.2f}s")
    
    return {
        "success": len(failed_results) == 0,
        "total_processed": len(all_results),
        "success_count": len(success_results),
        "failed_count": len(failed_results),
        "success_results": success_results,
        "failed_results": failed_results,
        "duration_seconds": duration,
    }
