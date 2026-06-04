#!/usr/bin/env python3

import os
import sqlite3
import tempfile
import pytest
import shutil
import functools
import pathlib

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app
from parsl.config import Config as ParslConfig
from parsl.executors.threads import ThreadPoolExecutor

from sbn_parsl.workflow import (
    Stage,
    Workflow,
    LArSoftExecutor,
    DefaultStageTypes,
    StageResult,
)
from sbn_parsl.config import (
    Config,
    SiteConfig,
    RunConfig,
    LArSoftConfig,
    WorkflowConfig,
    JobConfig,
)
from sbn_parsl.components import _transfer_ids


@bash_app(cache=False)
def fail_app(outputs, stdout, stderr):
    # return f'mkdir -p $(dirname {outputs[0]}); touch {outputs[0]}; exit 1'
    return "exit 1"


@bash_app(cache=False)
def pass_app(outputs, stdout, stderr):
    return f"mkdir -p $(dirname {outputs[0]}); touch {outputs[0]}; exit 0"


def default_parsl_runfunc(
    stage_self,
    fcl,
    parent_result: StageResult,
    output_dir,
    executor,
    app,
    retry_fails=False,
) -> StageResult:
    """Default function called when each stage is run."""
    if parent_result.outputs:
        " ".join([f"-s {str(file)}" for file in parent_result.outputs])

    output_filename = os.path.basename(fcl).replace(".fcl", ".root")
    if output_dir is None:
        output_dir = pathlib.Path(".")
    output_file = output_dir / pathlib.Path(output_filename)
    f"--output {str(output_file)}"

    if executor.stage_in_db(stage_self.stage_id_str, require_success=retry_fails):
        executor._skip_counter += 1
        return StageResult(outputs=[output_file])

    future = app(
        outputs=[File(str(output_file))],
        stdout=str(output_dir / "log.err"),
        stderr=str(output_dir / "log.out"),
    )
    _transfer_ids(stage_self, future.outputs[0])

    executor.futures.add(future.outputs[0])
    executor._stage_counter += 1
    return StageResult(outputs=[output_file])


def minimal_parsl_config(tmp_dir):
    """Returns a minimal Parsl config using ThreadPoolExecutor."""
    return ParslConfig(
        executors=[ThreadPoolExecutor(label="local_threads", max_threads=2)],
        run_dir=os.path.join(tmp_dir, "runinfo"),
    )


@pytest.fixture(scope="function")
def temp_db_dir():
    """Provides a temporary directory for the sqlite db file."""
    tmp_dir = tempfile.mkdtemp()
    yield tmp_dir
    shutil.rmtree(tmp_dir)


def create_mock_config(tmp_dir):
    return Config(
        site=SiteConfig(
            name="local",
            cpus_per_node=2,
            cores_per_worker=1,
            max_futures=-1,
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=LArSoftConfig(
            experiment="sbnd",
            version="v1",
            qual="e26",
            container_path="",
            larsoft_top="",
        ),
        workflow=WorkflowConfig(fcls={"gen": "gen.fcl"}),
        run=RunConfig(nsubruns=1, output=str(tmp_dir), require_success=False),
    )


class MinimalExecutor(LArSoftExecutor):
    def __init__(self, cfg: Config, retry_fails: bool):
        super().__init__(cfg)
        self.runfunc = functools.partial(
            default_parsl_runfunc, executor=self, retry_fails=retry_fails, app=fail_app
        )

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(stage_order=[DefaultStageTypes.GEN], default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.GEN)
        s.runfunc = self.runfunc
        s.run_dir = self.output_dir / "gen"
        workflow.add_final_stage(s)

        return workflow


class MinimalExecutorPass(LArSoftExecutor):
    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self.runfunc = functools.partial(
            default_parsl_runfunc, executor=self, retry_fails=False, app=pass_app
        )

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(stage_order=[DefaultStageTypes.GEN], default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.GEN)
        s.runfunc = self.runfunc
        s.run_dir = self.output_dir / "gen"
        workflow.add_final_stage(s)

        return workflow


def test_workflow_caching_fail(temp_db_dir):
    """Test that failed stage is not re-run if require_success is False."""
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    cfg = create_mock_config(temp_db_dir)
    wfe = MinimalExecutor(cfg, retry_fails=False)
    wfe.execute()

    assert wfe._stage_counter == 1
    assert wfe._success_counter == 0
    assert wfe._fail_counter == 1

    # check that the status was written to the db
    disk_db = sqlite3.connect(str(wfe._db_file))
    cursor = disk_db.cursor()
    result = cursor.execute(
        "SELECT status FROM stages WHERE stage_id=(?)", ("0_0",)
    ).fetchone()[0]
    disk_db.close()
    assert result == 1
    parsl.clear()

    # now try again, task should get skipped because it's in the DB and we don't require success
    parsl.load(config)
    wfe2 = MinimalExecutor(cfg, retry_fails=False)
    wfe2.execute()
    parsl.clear()

    assert wfe2._skip_counter == 1
    assert wfe2._stage_counter == 0
    assert wfe2._success_counter == 0
    assert wfe2._fail_counter == 0


def test_workflow_caching_pass(temp_db_dir):
    """Test that failed stage is re-run if require_success is True."""
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    cfg = create_mock_config(temp_db_dir)
    wfe = MinimalExecutor(cfg, retry_fails=False)
    wfe.execute()

    assert wfe._stage_counter == 1
    assert wfe._success_counter == 0
    assert wfe._fail_counter == 1

    # check that the status was written to the db
    disk_db = sqlite3.connect(str(wfe._db_file))
    cursor = disk_db.cursor()
    result = cursor.execute(
        "SELECT status FROM stages WHERE stage_id=(?)", ("0_0",)
    ).fetchone()[0]
    disk_db.close()
    assert result == 1
    parsl.clear()

    # now try again, task should get re-submitted because we require success
    parsl.load(config)
    wfe2 = MinimalExecutor(cfg, retry_fails=True)
    wfe2.execute()
    parsl.clear()

    assert wfe2._skip_counter == 0
    assert wfe2._stage_counter == 1
    assert wfe2._success_counter == 0
    assert wfe2._fail_counter == 1


def test_workflow_counters_pass(temp_db_dir):
    """Test that successful stage has correct counters."""
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    cfg = create_mock_config(temp_db_dir)
    wfe = MinimalExecutorPass(cfg)
    wfe.execute()
    parsl.clear()

    assert wfe._stage_counter == 1
    assert wfe._success_counter == 1
    assert wfe._fail_counter == 0
    assert wfe._skip_counter == 0


def test_detect_active_env(monkeypatch):
    from sbn_parsl.parsl_setup import detect_active_env

    # 1. Test VIRTUAL_ENV detection
    monkeypatch.setenv("VIRTUAL_ENV", "/path/to/my_venv")
    monkeypatch.delenv("CONDA_PREFIX", raising=False)
    assert detect_active_env() == "/path/to/my_venv"

    # 2. Test CONDA_PREFIX detection
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    monkeypatch.setenv("CONDA_PREFIX", "/path/to/my_conda")
    assert detect_active_env() == "/path/to/my_conda"

    # 3. Test fallback to sys.executable prefix
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    monkeypatch.delenv("CONDA_PREFIX", raising=False)
    monkeypatch.setattr("sys.executable", "/some/custom/path/.venv/bin/python")
    assert detect_active_env() == "/some/custom/path/.venv"


def test_worker_init_automatic_env(monkeypatch):
    from sbn_parsl.parsl_setup import _worker_init

    # Set mock environment
    monkeypatch.setenv("VIRTUAL_ENV", "/home/user/my_active_venv")
    monkeypatch.delenv("CONDA_PREFIX", raising=False)

    cfg = Config(
        site=SiteConfig(
            name="polaris",
            cpus_per_node=32,
            cores_per_worker=1,
            worker_init=[
                "export TMPDIR=/tmp/",
                "source /some/custom/path/bin/activate"
            ]
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    worker_init_str = _worker_init(cfg, mps=False)
    # The active env path should be prepended, AND custom worker_init commands kept unmodified
    assert worker_init_str.startswith("source /home/user/my_active_venv/bin/activate")
    assert "source /some/custom/path/bin/activate" in worker_init_str


def test_worker_init_configured_env(monkeypatch):
    from sbn_parsl.parsl_setup import _worker_init

    # Set mock environment that should be ignored
    monkeypatch.setenv("VIRTUAL_ENV", "/home/user/my_active_venv")
    monkeypatch.delenv("CONDA_PREFIX", raising=False)

    cfg = Config(
        site=SiteConfig(
            name="polaris",
            cpus_per_node=32,
            cores_per_worker=1,
            virtual_env="/custom/venv/path",
            worker_init=[
                "export TMPDIR=/tmp/"
            ]
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    worker_init_str = _worker_init(cfg, mps=False)
    # The configured virtual_env should be prepended
    assert worker_init_str.startswith("source /custom/venv/path/bin/activate")
    assert "export TMPDIR=/tmp/" in worker_init_str


def test_worker_init_fallback(monkeypatch):
    from sbn_parsl.parsl_setup import _worker_init

    # Clear environments
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)
    monkeypatch.delenv("CONDA_PREFIX", raising=False)
    # Set sys.executable to standard path to avoid custom python matching
    monkeypatch.setattr("sys.executable", "/usr/bin/python")

    cfg = Config(
        site=SiteConfig(
            name="polaris",
            cpus_per_node=32,
            cores_per_worker=1,
            worker_init=[
                "export TMPDIR=/tmp/"
            ]
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    worker_init_str = _worker_init(cfg, mps=False)
    # Should not prepend any activation since no env is active/configured
    assert "bin/activate" not in worker_init_str
    assert worker_init_str == "export TMPDIR=/tmp/"


def test_worker_init_explicit_disable(monkeypatch):
    from sbn_parsl.parsl_setup import _worker_init

    # Set mock environment that should be ignored because of explicit empty virtual_env
    monkeypatch.setenv("VIRTUAL_ENV", "/home/user/my_active_venv")

    cfg = Config(
        site=SiteConfig(
            name="polaris",
            cpus_per_node=32,
            cores_per_worker=1,
            virtual_env="",
            worker_init=[
                "export TMPDIR=/tmp/"
            ]
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    worker_init_str = _worker_init(cfg, mps=False)
    # Should not prepend any activation
    assert "bin/activate" not in worker_init_str
    assert worker_init_str == "export TMPDIR=/tmp/"


def test_initialize_logging_config(monkeypatch):
    from sbn_parsl.parsl_setup import create_parsl_config

    # Mock address_by_interface to avoid network resolution errors on non-HPC systems
    monkeypatch.setattr("sbn_parsl.parsl_setup.address_by_interface", lambda iface: "127.0.0.1")

    cfg = Config(
        site=SiteConfig(
            name="local",
            cpus_per_node=2,
            cores_per_worker=1,
        ),
        job=JobConfig(allocation="test", queue="test", initialize_logging=True),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    parsl_config = create_parsl_config(cfg, local=True)
    assert parsl_config.initialize_logging is True


def test_fcl_future_formatting():
    from sbn_parsl.components import fcl_future

    # fcl_future is a Parsl bash_app. To test the underlying formatting function
    # wrapped by Parsl without loading a full Parsl DFK, we can call the original
    # Python function directly using .func attribute of the Parsl app wrapper.
    orig_func = fcl_future.func

    template = "FHC={fhicl} OUT={output} IN={input} CMD={cmd} TOP={larsoft_top}"

    # 1. Test formatting with a single input file (e.g. generator stage)
    res = orig_func(
        workdir="/tmp",
        stdout="stdout",
        stderr="stderr",
        template=template,
        cmd="lar",
        larsoft_opts={"larsoft_top": "/soft"},
        inputs=["my_fcl.fcl"],
        outputs=["my_out.root"]
    )
    assert res == "FHC=my_fcl.fcl OUT=my_out.root IN= CMD=lar TOP=/soft"

    # 2. Test formatting with two input files (e.g. typical stage)
    res_two = orig_func(
        workdir="/tmp",
        stdout="stdout",
        stderr="stderr",
        template=template,
        cmd="lar",
        larsoft_opts={"larsoft_top": "/soft"},
        inputs=["my_fcl.fcl", "my_in.root"],
        outputs=["my_out.root"]
    )
    assert res_two == "FHC=my_fcl.fcl OUT=my_out.root IN=my_in.root CMD=lar TOP=/soft"


def test_env_file_config():
    cfg = Config(
        site=SiteConfig(
            name="local",
            cpus_per_node=2,
            cores_per_worker=1,
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=LArSoftConfig(
            experiment="sbnd",
            version="v1",
            qual="e26",
            env_file="/path/to/my_env_file.sh",
            container_path="",
            larsoft_top="",
        ),
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    assert cfg.larsoft.env_file == "/path/to/my_env_file.sh"


def test_monitor_cmd_config():
    cfg = Config(
        site=SiteConfig(
            name="local",
            cpus_per_node=2,
            cores_per_worker=1,
            monitor_cmd="/path/to/lar_mon.sh -i 10 -o node_mon_$(hostname).jsonl",
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )
    assert cfg.site.monitor_cmd == "/path/to/lar_mon.sh -i 10 -o node_mon_$(hostname).jsonl"


def test_worker_init_monitor_cmd():
    from sbn_parsl.parsl_setup import _worker_init, create_executor_by_hostname
    from unittest.mock import patch

    cfg = Config(
        site=SiteConfig(
            name="local",
            cpus_per_node=2,
            cores_per_worker=1,
            monitor_cmd="/path/to/lar_mon.sh -i 10 -o node_mon_$(hostname).jsonl",
        ),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=None,
        workflow=WorkflowConfig(),
        run=RunConfig(nsubruns=1, output="test_output")
    )

    worker_init_str = _worker_init(cfg, mps=False)
    assert 'pgrep -f "lar_mon.sh"' not in worker_init_str
    assert '/path/to/lar_mon.sh -i 10 -o node_mon_$(hostname).jsonl &' not in worker_init_str

    with patch('sbn_parsl.parsl_setup.address_by_interface', return_value='127.0.0.1'):
        executor = create_executor_by_hostname(cfg, None)

    launch_cmd = executor.launch_cmd
    assert 'mkdir -p test_output/runinfo/cmd && cd test_output/runinfo/cmd' in launch_cmd
    assert 'pgrep -f "lar_mon.sh"' in launch_cmd
    assert '/path/to/lar_mon.sh -i 10 -o node_mon_$(hostname).jsonl &' in launch_cmd
    assert 'PARSL_RUN_NUM=""' in launch_cmd
    assert 'mkdir -p "$PARSL_RUN_NUM" && cd "$PARSL_RUN_NUM"' in launch_cmd
    assert 'cd ..' in launch_cmd


def test_entry_point_file_cache_missing_error(temp_db_dir, capsys):
    from sbn_parsl.app import entry_point

    settings_file = pathlib.Path(temp_db_dir) / "settings.toml"
    with open(settings_file, "w") as f:
        f.write("""
[larsoft]
experiment = "sbnd"
version = "v1"
qual = "e26"

[workflow.fcls]
gen = "gen.fcl"
""")

    runinfo_dir = pathlib.Path(temp_db_dir) / "runinfo"
    runinfo_dir.mkdir(parents=True, exist_ok=True)
    config_file = runinfo_dir / "config.toml"

    cfg = Config(
        site=SiteConfig(name="local", cpus_per_node=2, cores_per_worker=1),
        job=JobConfig(allocation="test", queue="test"),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        workflow=WorkflowConfig(fcls={"gen": "gen.fcl"}),
        run=RunConfig(nsubruns=1, output=str(temp_db_dir), runinfo=str(temp_db_dir))
    )
    cfg.save(config_file)

    class DummyWorkflowExecutor:
        def __init__(self, cfg):
            self.cfg = cfg
        def execute(self, cycle, dry_run=False):
            pass

    argv = ["sbn_parsl", str(settings_file), "-o", str(temp_db_dir), "-r", str(temp_db_dir)]

    entry_point(argv, DummyWorkflowExecutor)

    # Load config via Config.load to accurately compute the expected science hash with site defaults
    loaded_cfg = Config.load(
        settings_file,
        site_name="local",
        run_overrides={"output": str(temp_db_dir)}
    )
    expected_hash = loaded_cfg.get_science_hash()

    captured = capsys.readouterr()
    assert "FATAL: The configuration file" in captured.out
    assert "expected file cache database" in captured.out
    assert f"file_cache_{expected_hash}.db" in captured.out







