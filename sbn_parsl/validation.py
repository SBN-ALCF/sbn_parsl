import sys
from typing import Tuple, Dict, Any

def parse_walltime_to_seconds(walltime_str: str) -> int:
    """
    Parses walltime strings into seconds.
    Supports [DD-]HH:MM:SS format to accommodate both PBS and Slurm formats.
    """
    try:
        if '-' in walltime_str:
            days_str, time_str = walltime_str.split('-')
            days = int(days_str)
        else:
            days = 0
            time_str = walltime_str

        parts = list(map(int, time_str.split(':')))
        if len(parts) == 3:
            hours, minutes, seconds = parts
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = parts
        elif len(parts) == 1:
            hours = 0
            minutes = 0
            seconds = parts[0]
        else:
            raise ValueError()

        return days * 86400 + hours * 3600 + minutes * 60 + seconds
    except Exception:
        raise ValueError(f"Invalid walltime format: '{walltime_str}'. Expected format: [DD-]HH:MM:SS")


def check_queue_bounds(nodes: int, walltime_str: str, q_name: str, q_limits: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Verifies if nodes and walltime fall within the specified bounds.
    """
    n_min = q_limits.get("nodes_min", 0)
    n_max = q_limits.get("nodes_max", float("inf"))
    t_min_str = q_limits.get("walltime_min", "00:00:00")
    t_max_str = q_limits.get("walltime_max", "999:59:59")

    try:
        req_seconds = parse_walltime_to_seconds(walltime_str)
        t_min = parse_walltime_to_seconds(t_min_str)
        t_max = parse_walltime_to_seconds(t_max_str)
    except ValueError as e:
        return False, f"Validation error: {e}"

    errors = []
    if nodes < n_min or nodes > n_max:
        errors.append(f"nodes={nodes} (allowed {n_min}-{n_max})")
    if req_seconds < t_min or req_seconds > t_max:
        errors.append(f"walltime={walltime_str} (allowed {t_min_str}-{t_max_str})")

    if errors:
        return False, ", ".join(errors)
    return True, ""


def validate_queue_limits(cfg) -> None:
    """
    Main entry point for queue resource validation.
    Checks user-requested queue, node count, and walltime against site TOML rules.
    """
    site_queues = getattr(cfg.site, "queues", {})
    requested_queue = cfg.job.queue
    nodes = cfg.job.nodes_per_block
    walltime_str = cfg.job.walltime

    # If the queue is not defined under validation configs, warn the user and skip validation
    if requested_queue not in site_queues:
        if cfg.site.name != "local":
            print("=" * 60, file=sys.stderr)
            print(f"WARNING: No known queue limits for site '{cfg.site.name}' queue '{requested_queue}'.", file=sys.stderr)
            print("Please check your job settings manually to verify the job satisfies queue requirements.", file=sys.stderr)
            print(f"Specify limits in settings/sites/{cfg.site.name}.toml under '[site.queues]' to suppress this warning.", file=sys.stderr)
            print("=" * 60, file=sys.stderr)
        return

    q_limits = site_queues[requested_queue]
    is_routing = q_limits.get("is_routing", False)

    if is_routing:
        routes_to = q_limits.get("routes_to", [])
        valid_route_found = False
        sub_queue_results = {}

        for sub_q_name in routes_to:
            if sub_q_name in site_queues:
                is_valid, err_msg = check_queue_bounds(
                    nodes, walltime_str, sub_q_name, site_queues[sub_q_name]
                )
                if is_valid:
                    valid_route_found = True
                    break
                else:
                    sub_queue_results[sub_q_name] = err_msg

        if not valid_route_found:
            print("=" * 60, file=sys.stderr)
            print(f"WARNING: Requested resources (nodes={nodes}, walltime={walltime_str})", file=sys.stderr)
            print(f"do not match any execution sub-queues for routing queue '{requested_queue}'.", file=sys.stderr)
            print("Job routing failures:", file=sys.stderr)
            for sub_q_name, err in sub_queue_results.items():
                print(f"  - {sub_q_name}: {err}", file=sys.stderr)
            print("This job may be immediately rejected by the site scheduler.", file=sys.stderr)
            print("=" * 60, file=sys.stderr)
    else:
        is_valid, err_msg = check_queue_bounds(nodes, walltime_str, requested_queue, q_limits)
        if not is_valid:
            print("=" * 60, file=sys.stderr)
            print(f"WARNING: Job queue validation failed for queue '{requested_queue}':", file=sys.stderr)
            print(f"  - {err_msg}", file=sys.stderr)
            print("This job violates site queue policies and may fail to run.", file=sys.stderr)
            print("=" * 60, file=sys.stderr)
