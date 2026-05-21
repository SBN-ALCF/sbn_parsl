import socket
import pathlib
from parsl.config import Config as ParslConfig
from parsl.dataflow.memoization import BasicMemoizer
from parsl.addresses import address_by_interface
from parsl.utils import get_all_checkpoints
from parsl.providers import PBSProProvider, LocalProvider
from parsl.launchers import MpiExecLauncher
from parsl.executors.high_throughput.executor import DEFAULT_LAUNCH_CMD

from sbn_parsl.config import Config


def aurora_affinity(per_worker: int = 1, ncpus: int = -1):
    """Return parsl CPU affinity list for aurora, pairing physical & virtual cores and excluding CPUs 0 and 52."""
    if per_worker < 1:
        per_worker = 1

    cpus = [i for i in range(1, 104) if i != 52]
    if ncpus > 0:
        cpus = cpus[0:ncpus]
    cpu_groups = [cpus[i : i + per_worker] for i in range(0, len(cpus), per_worker)]

    return "list:" + ":".join(
        [",".join([f"{cpu},{cpu + 104}" for cpu in group]) for group in cpu_groups]
    )


def _worker_init(cfg: Config, mps: bool = True):
    """Return list of worker init commands based on the Config."""
    cmds = []

    # Custom worker init from site config
    if cfg.site.worker_init:
        if isinstance(cfg.site.worker_init, list):
            cmds.extend(cfg.site.worker_init)
        else:
            cmds.extend([line.strip() for line in cfg.site.worker_init.split("\n") if line.strip()])
    else:
        venv_name = cfg.site.worker_venv_name
        hostname = socket.gethostname()
        if cfg.site.name == "polaris" or "polaris" in hostname or hostname.startswith("x3"):
            # use conda
            cmds += [
                "export TMPDIR=/tmp/",
                f"source ~/.venv/{venv_name}/bin/activate",
            ]
        elif cfg.site.name == "aurora" or "aurora" in hostname or hostname.startswith("x4"):
            # use pip with frameworks
            cmds += [
                "export TMPDIR=/tmp/",
                "module load frameworks",
                f"source ~/.venv/{venv_name}/bin/activate",
            ]
        else:
            # Generic local or other site
            if venv_name:
                cmds += [f"source ~/.venv/{venv_name}/bin/activate"]

    if mps and cfg.site.name == "polaris":
        cmds += [
            "export CUDA_MPS_PIPE_DIRECTORY=/tmp/nvidia-mps",
            "export CUDA_MPS_LOG_DIRECTORY=/tmp/nvidia-log",
            "CUDA_VISIBLE_DEVICES=0,1,2,3 nvidia-cuda-mps-control -d",
            'echo "start_server -uid $( id -u )" | nvidia-cuda-mps-control',
        ]

    return "&&".join(cmds)


def create_provider_by_hostname(cfg: Config, local: bool = False):
    mps = cfg.site.name == "polaris"
    worker_init = _worker_init(cfg, mps=mps)

    # extra command to change directory to run_dir (prevent home from filling up with junk temp files)
    run_dir = cfg.run.runinfo or cfg.run.output
    rundir_path = pathlib.Path(run_dir) / "runinfo" / "cmd"

    daos_cmd = ""
    if cfg.run.daos:
        daos_pool = cfg.job.daos_pool
        daos_cont = cfg.job.daos_cont
        daos_cmds = [
            "module use /soft/modulefiles",
            "module load daos",
            f"launch-dfuse.sh {daos_pool}:{daos_cont}",
        ]
        daos_cmd = "&&".join(daos_cmds) + "&&"
    cwd_cmd = f"{daos_cmd}mkdir -p {rundir_path}&&cd {rundir_path}"

    if local:
        # user has allocated the job. Just launch
        return LocalProvider(
            nodes_per_block=cfg.job.nodes_per_block,
            init_blocks=1,
            max_blocks=1,
            launcher=MpiExecLauncher(
                bind_cmd="--cpu-bind", overrides=cfg.site.launcher_options
            ),
            worker_init="&&".join(
                [cwd_cmd, worker_init, "export PATH=/opt/cray/pals/1.4/bin:${PATH}"]
            ),
        )

    # let parsl allocate the job
    return PBSProProvider(
        account=cfg.job.allocation,
        queue=cfg.job.queue,
        nodes_per_block=cfg.job.nodes_per_block,
        cpus_per_node=cfg.site.cpus_per_node,
        init_blocks=1,
        max_blocks=1,
        walltime=cfg.job.walltime,
        cmd_timeout=240,
        scheduler_options=cfg.site.scheduler_options,
        launcher=MpiExecLauncher(
            bind_cmd="--cpu-bind", overrides=cfg.site.launcher_options
        ),
        worker_init="&&".join(
            [cwd_cmd, worker_init, "export PATH=/opt/cray/pals/1.4/bin:${PATH}"]
        ),
    )


def create_executor_by_hostname(cfg: Config, provider):
    from parsl import HighThroughputExecutor

    max_workers_per_node = cfg.job.max_workers_per_node or cfg.site.cpus_per_node
    cpu_affinity = cfg.site.cpu_affinity

    if cpu_affinity == "aurora":
        cpu_affinity = aurora_affinity(
            per_worker=cfg.site.cores_per_worker, ncpus=max_workers_per_node
        )

    init_cmd = ""
    # copy larsoft tarballs to the node
    if cfg.larsoft.tarballs:
        tar_cmds = []
        for pkg_name, pkg_path in cfg.larsoft.tarballs.items():
            pkg_path = pathlib.Path(pkg_path)
            if not pkg_path.is_file():
                raise RuntimeError(
                    f"LArSoft package {pkg_name} has invalid path {pkg_path}"
                )
            dest_path = pathlib.PurePosixPath("/tmp")
            tarball_path = pathlib.PurePosixPath("/tmp", pkg_path.name)
            if pkg_name == "root":
                tar_cmds.append("mkdir -p /tmp/root_lib")
                dest_path = pathlib.PurePosixPath("/tmp", "root_lib")
            tar_cmds.append(
                f"cp {pkg_path} /tmp && tar -xf {tarball_path} -C {dest_path} && rm -f {tarball_path}"
            )
        init_cmd += "\n" + "\n".join(tar_cmds)

    run_dir = cfg.run.runinfo or cfg.run.output
    working_dir = str(pathlib.Path(run_dir) / "runinfo" / "cmd")

    return HighThroughputExecutor(
        label="htex",
        heartbeat_period=15,
        heartbeat_threshold=120,
        worker_debug=True,
        launch_cmd="\n".join([init_cmd, DEFAULT_LAUNCH_CMD]),
        max_workers_per_node=max_workers_per_node,
        cores_per_worker=cfg.site.cores_per_worker,
        available_accelerators=cfg.site.available_accelerators,
        address=address_by_interface("hsn0"),
        address_probe_timeout=120,
        cpu_affinity=cpu_affinity,
        prefetch_capacity=0,
        provider=provider,
        block_error_handler=False,
        working_dir=working_dir,
    )


def create_parsl_config(cfg: Config, local: bool = False):
    if cfg.run.daos:
        # This modification of cfg.site is a bit messy, but follows old logic
        cfg.site.pbs_filesystems += ":daos_user_fs"
        if cfg.site.scheduler_options:
            cfg.site.scheduler_options += ":daos_user_fs"

    provider = create_provider_by_hostname(cfg, local)
    executor = create_executor_by_hostname(cfg, provider)

    run_dir = cfg.run.runinfo or cfg.run.output
    run_dir_path = str(pathlib.Path(run_dir) / "runinfo")

    checkpoints = get_all_checkpoints(run_dir_path)

    config = ParslConfig(
        memoizer=BasicMemoizer(
            checkpoint_mode="task_exit",
            checkpoint_files=checkpoints,
        ),
        executors=[executor],
        run_dir=run_dir_path,
        strategy=cfg.job.strategy,
        retries=cfg.job.retries,
        initialize_logging=False,
    )

    return config
