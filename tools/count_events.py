#!/usr/bin/env python3
"""Sum "event_count" over every *.root.json metadata file under a directory tree.

Usage:
    count_events.py ROOT_DIR [ROOT_DIR ...] [-j THREADS] [--suffix .root.json] [--per-dir]

The job is I/O-bound (metadata lookups + tiny reads), not CPU-bound, so a
thread pool works well: file reads release the GIL. The directory walk is
parallel too: each task scans one directory, reads its JSON files, and hands
its subdirectories back to the main thread, which feeds them to the pool
depth-first with a bounded number of tasks in flight (memory stays flat).
"""
import argparse
import os
import re
import sys
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

EVENT_RE = re.compile(rb'"event_count"\s*:\s*"?(\d+)')


def scan_dir(path, suffix):
    """Return (events, n_files, subdirs, bad_files) for one directory."""
    events = n_files = 0
    subdirs, bad = [], []
    try:
        it = os.scandir(path)
    except OSError as e:
        return 0, 0, [], [(path, str(e))]
    with it:
        for entry in it:
            name = entry.name
            if name.endswith(suffix):
                n_files += 1
                try:
                    with open(entry.path, "rb") as f:
                        m = EVENT_RE.search(f.read())
                except OSError as e:
                    bad.append((entry.path, str(e)))
                    continue
                if m:
                    events += int(m.group(1))
                else:
                    bad.append((entry.path, "no event_count"))
            elif entry.is_dir(follow_symlinks=False):
                subdirs.append(entry.path)
    return events, n_files, subdirs, bad


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("roots", nargs="+")
    p.add_argument("-j", "--threads", type=int, default=64,
                   help="worker threads (default 64; try 128+ on Lustre/DAOS)")
    p.add_argument("--suffix", default=".root.json")
    p.add_argument("--per-dir", action="store_true",
                   help="print per-directory event counts (tab-separated)")
    p.add_argument("-q", "--quiet", action="store_true", help="no progress on stderr")
    args = p.parse_args()

    total_events = total_files = n_dirs = n_bad = 0
    first_bad = []  # keep only what we print; count the rest

    # Directories not yet submitted live here as plain strings (~100 B each)
    # rather than as queued Futures (~2 KB each). Popping from the end makes
    # the walk depth-first, which keeps this stack short. In-flight futures
    # are capped so memory and wait() cost stay flat regardless of tree size.
    todo = list(reversed(args.roots))
    max_in_flight = max(4 * args.threads, 1)
    next_report = 100

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        pending = {}
        while todo or pending:
            while todo and len(pending) < max_in_flight:
                d = todo.pop()
                pending[pool.submit(scan_dir, d, args.suffix)] = d
            done, _ = wait(pending, return_when=FIRST_COMPLETED)
            for fut in done:
                path = pending.pop(fut)
                events, n_files, subdirs, bad = fut.result()
                total_events += events
                total_files += n_files
                n_dirs += 1
                n_bad += len(bad)
                if len(first_bad) < 20:
                    first_bad.extend(bad[:20 - len(first_bad)])
                if args.per_dir and n_files:
                    print(f"{path}\t{n_files}\t{events}")
                todo.extend(subdirs)
            if not args.quiet and n_dirs >= next_report:
                next_report = n_dirs + 100
                print(f"\r{n_dirs} dirs, {total_files} files, {total_events} events",
                      end="", file=sys.stderr, flush=True)

    if not args.quiet:
        print(file=sys.stderr)
    for path, err in first_bad:
        print(f"WARNING: {path}: {err}", file=sys.stderr)
    if n_bad > len(first_bad):
        print(f"WARNING: ... and {n_bad - len(first_bad)} more problem files", file=sys.stderr)

    print(f"directories:  {n_dirs}")
    print(f"json files:   {total_files}")
    print(f"problems:     {n_bad}")
    print(f"total events: {total_events}")


if __name__ == "__main__":
    main()
