#!/usr/bin/env python3
"""Sum "event_count" over every *.root.json metadata file under a directory tree.

Usage:
    count_events.py ROOT_DIR [ROOT_DIR ...] [-j THREADS] [--suffix .root.json] [--per-dir]

The job is I/O-bound (metadata lookups + tiny reads), not CPU-bound, so a
thread pool works well: file reads release the GIL. Two kinds of task share
the pool: listing one directory, and reading one chunk of its JSON files.
Chunking means a directory with thousands of files is read by many threads
instead of one. The main thread feeds tasks depth-first with a bounded number
in flight, so memory stays flat regardless of tree size.
"""
import argparse
import os
import re
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

EVENT_RE = re.compile(rb'"event_count"\s*:\s*"?(\d+)')
CHUNK = 64          # files per read task
MAX_BAD_KEPT = 20   # problem files remembered for printing; the rest are counted


def list_dir(path, suffix):
    """Return (subdirs, matching_files, bad) for one directory."""
    subdirs, files = [], []
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.name.endswith(suffix):
                    files.append(entry.path)
                else:
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            subdirs.append(entry.path)
                    except OSError:
                        pass
    except OSError as e:
        return subdirs, files, [(path, str(e))]
    return subdirs, files, []


def read_files(paths):
    """Return (events, bad) for a chunk of JSON files."""
    events = 0
    bad = []
    for p in paths:
        try:
            with open(p, "rb") as f:
                m = EVENT_RE.search(f.read())
        except OSError as e:
            bad.append((p, str(e)))
            continue
        if m:
            events += int(m.group(1))
        else:
            bad.append((p, "no event_count"))
    return events, bad


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

    total_events = files_read = n_dirs = n_bad = 0
    first_bad = []
    # --per-dir only: dir -> [n_files, events, chunks_outstanding]
    per_dir = {}

    def note_bad(bad):
        nonlocal n_bad
        n_bad += len(bad)
        if len(first_bad) < MAX_BAD_KEPT:
            first_bad.extend(bad[:MAX_BAD_KEPT - len(first_bad)])

    # Work not yet submitted: ("dir", path) or ("files", dir, [paths]).
    # Plain tuples are far cheaper than queued Futures. LIFO order is
    # depth-first and drains a directory's file chunks before exploring
    # further, which keeps this stack short.
    todo = [("dir", r) for r in reversed(args.roots)]
    max_in_flight = max(4 * args.threads, 1)
    t0 = last_report = time.monotonic()
    last_files = 0

    def report(final=False):
        # "now" is the rate since the previous report; "avg" is since start
        nonlocal last_files, last_report
        t = time.monotonic()
        el = t - t0
        now_rate = (files_read - last_files) / max(t - last_report, 1e-9)
        last_files, last_report = files_read, t
        print(f"\r{n_dirs} dirs, {files_read} files "
              f"({now_rate:.0f}/s now, {files_read / max(el, 1e-9):.0f}/s avg), "
              f"{total_events} events, {len(todo)} queued, {len(pending)} in flight, {el:.0f}s ",
              end="\n" if final else "", file=sys.stderr, flush=True)

    with ThreadPoolExecutor(max_workers=args.threads) as pool:
        pending = {}
        while todo or pending:
            while todo and len(pending) < max_in_flight:
                item = todo.pop()
                if item[0] == "dir":
                    fut = pool.submit(list_dir, item[1], args.suffix)
                else:
                    fut = pool.submit(read_files, item[2])
                pending[fut] = item
            done, _ = wait(pending, timeout=1.0, return_when=FIRST_COMPLETED)
            for fut in done:
                item = pending.pop(fut)
                if item[0] == "dir":
                    path = item[1]
                    subdirs, files, bad = fut.result()
                    n_dirs += 1
                    note_bad(bad)
                    todo.extend(("dir", d) for d in subdirs)
                    chunks = [files[i:i + CHUNK] for i in range(0, len(files), CHUNK)]
                    if args.per_dir and chunks:
                        per_dir[path] = [len(files), 0, len(chunks)]
                    todo.extend(("files", path, c) for c in chunks)
                else:
                    path, chunk = item[1], item[2]
                    events, bad = fut.result()
                    total_events += events
                    files_read += len(chunk)
                    note_bad(bad)
                    if args.per_dir:
                        st = per_dir[path]
                        st[1] += events
                        st[2] -= 1
                        if st[2] == 0:
                            print(f"{path}\t{st[0]}\t{st[1]}")
                            del per_dir[path]
            if not args.quiet and time.monotonic() - last_report >= 2.0:
                report()

    if not args.quiet:
        report(final=True)
    for path, err in first_bad:
        print(f"WARNING: {path}: {err}", file=sys.stderr)
    if n_bad > len(first_bad):
        print(f"WARNING: ... and {n_bad - len(first_bad)} more problem files", file=sys.stderr)

    print(f"directories:  {n_dirs}")
    print(f"json files:   {files_read}")
    print(f"problems:     {n_bad}")
    print(f"total events: {total_events}")


if __name__ == "__main__":
    main()
