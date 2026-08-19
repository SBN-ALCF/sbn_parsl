import os
import sys
import subprocess
import argparse
import pathlib
from datetime import datetime

import parsl

from sbn_parsl.parsl_setup import create_parsl_config, detect_active_env, _worker_init
from sbn_parsl.dfk_hacks import apply_hacks
from sbn_parsl.config import Config


def parse_arguments(argv):
    """Parse command line arguments for sbn_parsl workflows."""
    parser = argparse.ArgumentParser(prog="sbn_parsl")
    parser.add_argument("settings", help="Path to TOML settings file")

    # Job/Site overrides
    parser.add_argument("-A", "--allocation", help="Allocation to use")
    parser.add_argument("-q", "--queue", help="Queue to use")
    parser.add_argument("-t", "--walltime", help="Walltime for the job")
    parser.add_argument(
        "-n", "--nodes", type=int, dest="nodes_per_block", help="Number of nodes"
    )
    parser.add_argument("--cpus-per-node", type=int, help="CPUs per node")
    parser.add_argument("--cores-per-worker", type=int, help="Cores per worker")
    parser.add_argument("--site", help="Override site detection (e.g. polaris, aurora)")

    parser.add_argument(
        "--daos", action="store_true", help="Use DAOS for output storage", default=False
    )
    parser.add_argument(
        "-o", "--output-dir", dest="output", help="Directory for outputs", required=True
    )
    parser.add_argument(
        "-r", "--runinfo-dir", dest="runinfo", help="Directory for workflow outputs"
    )
    parser.add_argument(
        "-l",
        "--local",
        action="store_true",
        help="Use local provider instead of PBS",
        default=False,
    )
    parser.add_argument(
        "-c", "--cycle", type=int, help="Cycle workflow submission count"
    )
    parser.add_argument(
        "--task-order",
        choices=["depth", "workflow"],
        help="Task dispatch strategy. 'depth' (default) runs dependency-free "
        "first stages before any later stage; 'workflow' carries each workflow "
        "to completion before starting the next",
    )
    parser.add_argument(
        "--check-existing-outputs",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Stat each stage's output at submit time and skip the task if it "
        "already exists. Off by default; costs one filesystem check per task, "
        "but avoids re-running stages the cache database failed to record",
    )
    parser.add_argument("--nsubruns", type=int, help="Number of subruns to execute")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force run even if config differs from existing run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing",
        default=False,
    )

    # We add an attribute for the script name so entry_point can use it
    args = parser.parse_args(argv[1:])
    args.script_name = pathlib.Path(argv[0]).stem if len(argv) > 0 else "sbn_parsl"
    return args


def print_config_summary(cfg: Config):
    """Print a clean and curated summary of the user's config instead of the verbose ParslConfig."""

    print("=" * 60)
    print("             SBN Parsl Workflow Config Summary")
    print("=" * 60)

    # 1. Queue & Site Settings
    print("Site & Job Settings:")
    print(f"  Site Name:        {cfg.site.name}")
    print(f"  Allocation:       {cfg.job.allocation}")
    print(f"  Queue:            {cfg.job.queue}")
    print(f"  Nodes Per Block:  {cfg.job.nodes_per_block}")
    print(f"  Walltime:         {cfg.job.walltime}")
    print(f"  Retries:          {cfg.job.retries}")

    # 2. Run Settings
    print("\nRun Settings:")
    print(f"  Output Directory: {cfg.run.output}")
    print(f"  Number of Subruns:{cfg.run.nsubruns}")
    print(f"  Require Success:  {cfg.run.require_success}")
    print(f"  Task Order:       {cfg.run.task_order}")
    print(f"  Check Outputs:    {cfg.run.check_existing_outputs}")

    # 3. Environment & Virtual Env
    print("\nPython Environment:")
    if cfg.site.virtual_env:
        print(f"  Configured Venv:  {cfg.site.virtual_env}")
    active_env = detect_active_env()
    if active_env:
        print(f"  Active Venv:      {active_env}")
    else:
        print("  Active Venv:      None (using fallback)")

    # 4. App & Workflow Settings
    print("\nApp & Workflow Settings:")
    if cfg.larsoft:
        print(f"  Experiment: {cfg.larsoft.experiment}")
        print(f"  Software:    {cfg.larsoft.software} {cfg.larsoft.version} {cfg.larsoft.qual}")
        if cfg.larsoft.env_file:
            print(f"  Custom Env File:    {cfg.larsoft.env_file}")

    if cfg.workflow.fcls:
        print("  Workflow FCLs:")
        for name, fcl in cfg.workflow.fcls.items():
            print(f"    - {name}: {fcl}")
    print("=" * 60)


def entry_point(argv, wfe_class):
    """Provide common setup and execution for sbn_parsl workflows using parsed args."""
    if isinstance(argv, list):
        args = parse_arguments(argv)
    else:
        args = argv

    # Load configuration with CLI overrides
    job_overrides = {
        "allocation": args.allocation,
        "queue": args.queue,
        "walltime": args.walltime,
        "nodes_per_block": args.nodes_per_block,
    }
    run_overrides = {
        "output": args.output,
        "runinfo": args.runinfo,
        "nsubruns": args.nsubruns,
        "daos": args.daos,
        "task_order": args.task_order,
        "check_existing_outputs": args.check_existing_outputs,
    }

    cfg = Config.load(
        pathlib.Path(args.settings),
        site_name=args.site,
        job_overrides=job_overrides,
        run_overrides=run_overrides,
        force=args.force,
    )

    # Set runinfo dir early for validation
    runinfo_base = pathlib.Path(cfg.run.runinfo or cfg.run.output)
    runinfo_dir = runinfo_base / "runinfo"
    runinfo_dir.mkdir(parents=True, exist_ok=True)

    config_file = runinfo_dir / "config.toml"
    launched_marker = runinfo_dir / ".launched"
    if config_file.exists() and launched_marker.exists() and not args.force and not args.dry_run:
        # Check compatibility
        existing_cfg = Config.load(config_file, site_name=cfg.site.name, validate=False)
        science_hash = cfg.get_science_hash()
        if science_hash != existing_cfg.get_science_hash():
            print(
                f"FATAL: Science identity mismatch in output directory {cfg.run.output}"
            )
            print(f"Current Hash:  {science_hash}")
            print(f"Existing Hash: {existing_cfg.get_science_hash()}")
            print("To resume, use the same LArSoft version and FCLs, or use --force.")
            return

        # Check if the expected file cache database exists
        db_file = runinfo_dir / "cmd" / f"file_cache_{science_hash}.db"
        if not db_file.exists():
            print(
                f"FATAL: The configuration file {config_file} was found, "
                f"but the expected file cache database {db_file} is missing."
            )
            print(
                "If you have recently upgraded the sbn_parsl package, the science hash "
                "format may have changed, causing a mismatch with the old database filename.\n\n"
                "To resolve this, you can:\n"
                "  1. Clear out the old config by deleting or renaming the runinfo/ directory.\n"
                "  2. Run with --force to overwrite and start a new cache.\n"
                "  3. Manually rename the old file_cache_*.db file in runinfo/cmd/ to:\n"
                f"     {db_file.name}"
            )
            return



    # Handle max_futures logic
    if args.local:
        cfg.site.max_futures = -1

    # If using daos, runinfo must be set
    if cfg.run.runinfo is None and cfg.run.daos is True:
        # Fallback to output if not provided, though old code required it
        cfg.run.runinfo = cfg.run.output

    cycle = args.cycle if args.cycle is not None else -1

    # Update site-specific dynamic values if provided
    if args.cpus_per_node:
        cfg.site.cpus_per_node = args.cpus_per_node
    if args.cores_per_worker:
        cfg.site.cores_per_worker = args.cores_per_worker

    # if we are local, submit via qsub unless we are already on a compute node
    if args.local and not args.dry_run:
        if os.environ.get("PBS_NODEFILE") is None:
            # qsub & return
            cmd_dir = runinfo_dir / "cmd"
            job_name = (
                f"parsl.{args.script_name}.{datetime.now().strftime('%Y%m%d%H%M%S')}"
            )

            hostfile_cmd = ""
            if cfg.job.nodes_per_block > 1:
                hostfile_cmd = HOSTFILE_CMD_EXCLUDE_SELF

            submit_script_dir = cmd_dir / "submit_scripts"
            submit_script_dir.mkdir(parents=True, exist_ok=True)

            # Reconstruct the workflow script path using sys.argv[0]
            workflow_path = pathlib.Path(sys.argv[0]).resolve()

            worker_init = "\n".join(_worker_init(cfg, mps=False).split("&&"))

            extra_args = []
            if args.allocation:
                extra_args.append(f"-A {args.allocation}")
            if args.queue:
                extra_args.append(f"-q {args.queue}")
            if args.walltime:
                extra_args.append(f"-t {args.walltime}")
            if args.nodes_per_block is not None:
                extra_args.append(f"-n {args.nodes_per_block}")
            if args.cpus_per_node is not None:
                extra_args.append(f"--cpus-per-node {args.cpus_per_node}")
            if args.cores_per_worker is not None:
                extra_args.append(f"--cores-per-worker {args.cores_per_worker}")
            if args.site:
                extra_args.append(f"--site {args.site}")
            if args.daos:
                extra_args.append("--daos")
            if args.runinfo:
                extra_args.append(f"-r {pathlib.Path(args.runinfo).resolve()}")
            if args.cycle is not None:
                extra_args.append(f"-c {args.cycle}")
            if args.nsubruns is not None:
                extra_args.append(f"--nsubruns {args.nsubruns}")
            if args.task_order is not None:
                extra_args.append(f"--task-order {args.task_order}")
            if args.check_existing_outputs is not None:
                extra_args.append(
                    "--check-existing-outputs"
                    if args.check_existing_outputs
                    else "--no-check-existing-outputs"
                )
            if args.force:
                extra_args.append("--force")

            extra_args_str = "".join(f" {arg}" for arg in extra_args)

            template = LOCAL_TEMPLATE.format(
                job_name=job_name,
                workflow=workflow_path,
                out_dir=cfg.run.output,
                cmd_dir=str(cmd_dir),
                settings=pathlib.Path(args.settings).resolve(),
                stdout=str(submit_script_dir / f"{job_name}.stdout"),
                stderr=str(submit_script_dir / f"{job_name}.stderr"),
                walltime=cfg.job.walltime,
                nodes_per_block=cfg.job.nodes_per_block,
                cpus_per_node=cfg.site.cpus_per_node,
                hostfile_cmd=hostfile_cmd,
                worker_init=worker_init,
                extra_args=extra_args_str,
            )

            script = cmd_dir / job_name
            with open(script, "w") as f:
                f.write(template)

            if not args.dry_run:
                cfg.save(config_file)

            subprocess.run(
                ["qsub", "-A", cfg.job.allocation, "-q", cfg.job.queue, str(script)]
            )
            return
        else:
            # subtract 1 from node count if node count > 1 so that the driver program gets its own node
            if cfg.job.nodes_per_block > 1:
                cfg.job.nodes_per_block -= 1

    if args.dry_run:
        wfe = wfe_class(cfg)
        wfe.execute(cycle, dry_run=True)
        return

    if not args.dry_run:
        cfg.save(config_file)

    parsl_config = create_parsl_config(cfg, local=args.local)
    print_config_summary(cfg)
    parsl.clear()

    with parsl.load(parsl_config) as dfk:
        apply_hacks(dfk)
        wfe = wfe_class(cfg)
        wfe.execute(cycle)


# when submitting with local provider, we call qsub directly on this script
LOCAL_TEMPLATE = r"""#!/bin/bash

#PBS -S /bin/bash
#PBS -N {job_name}
#PBS -m n
#PBS -l walltime={walltime}
#PBS -l select={nodes_per_block}:ncpus={cpus_per_node}
#PBS -o {stdout}
#PBS -e {stderr}
#PBS -l filesystems=home:flare

OUTDIR={cmd_dir}
mkdir -p $OUTDIR
cd $OUTDIR

# exclude this node (where the driver program runs) from the available nodes
{hostfile_cmd}

JOBNAME={job_name}
cat << 'EOL' > cmd_$JOBNAME.sh
{worker_init}
export PATH=/opt/cray/pals/1.4/bin:${{PATH}}

python {workflow} {settings} --local -o {out_dir}{extra_args}
EOL
chmod u+x cmd_$JOBNAME.sh

# line buffer to get logs faster
./cmd_$JOBNAME.sh

[[ "1" == "1" ]] && echo "All done"
"""

HOSTFILE_CMD_EXCLUDE_SELF = r"""sort -u $PBS_NODEFILE | grep -v $(hostname) > $OUTDIR/hostfile.noexec
export PBS_NODEFILE=$OUTDIR/hostfile.noexec
"""
