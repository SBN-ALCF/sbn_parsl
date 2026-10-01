import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sbn_parsl.app import check_daos_config, entry_point
from sbn_parsl.components import larsoft_runfunc
from sbn_parsl.config import (
    Config,
    JobConfig,
    LArSoftConfig,
    RunConfig,
    SiteConfig,
    WorkflowConfig,
)
from sbn_parsl.parsl_setup import add_daos_filesystem, create_provider_by_hostname
from sbn_parsl.workflow import DefaultStageTypes, Stage, StageResult

# never created: with run.daos the driver must not touch the output directory
DAOS_OUT = "/tmp/sbn_parsl_test_pool/sbn_parsl_test_cont/out"


def _daos_cfg(runinfo, output=DAOS_OUT, pool="sbn_parsl_test_pool",
              cont="sbn_parsl_test_cont"):
    return Config(
        site=SiteConfig(name="test", cpus_per_node=8, cores_per_worker=1,
                        max_futures=1000),
        job=JobConfig(nodes_per_block=1, daos_pool=pool, daos_cont=cont),
        workflow=WorkflowConfig(),
        run=RunConfig(output=output, runinfo=runinfo and str(runinfo), daos=True),
    )


def _stop(ex):
    ex._monitor_stop.set()
    ex._db_worker_stop.set()
    ex._db_update_thread.join(timeout=30)


def test_daos_bookkeeping_lives_in_runinfo(tmp_path):
    """The cache database and launch marker must be under -r, where the driver
    can see them, and where entry_point looks for them on resume."""
    from sbn_parsl.workflow import WorkflowExecutor

    ex = WorkflowExecutor(_daos_cfg(tmp_path))
    try:
        assert ex._db_file.parent == tmp_path / "runinfo" / "cmd"
        assert ex._db_file.exists()
        # the marker is only written once a real (non-dry) run starts
        assert not (tmp_path / "runinfo" / ".launched").exists()
        ex._mark_launched()
    finally:
        _stop(ex)
    # same path app.entry_point checks
    assert (tmp_path / "runinfo" / ".launched").exists()
    assert not (tmp_path / ".launched").exists()
    assert not pathlib.Path(DAOS_OUT).exists()


def test_resume_check_sees_real_marker_and_db(tmp_path):
    """Round trip: a marker and database written by the real executor must arm
    the resume check in entry_point, which then passes for a matching config."""
    from sbn_parsl.workflow import WorkflowExecutor

    settings = tmp_path / "settings.toml"
    settings.write_text(
        '[job]\ndaos_pool = "sbn_parsl_test_pool"\n'
        'daos_cont = "sbn_parsl_test_cont"\n'
        "[workflow]\n[workflow.fcls]\ngen = 'gen.fcl'\n"
    )
    argv = ["sbn_parsl", str(settings), "--site", "local", "--daos",
            "-o", DAOS_OUT, "-r", str(tmp_path)]

    executors = []

    class RecordingExecutor(WorkflowExecutor):
        def __init__(self, cfg):
            super().__init__(cfg)
            executors.append(self)

        def execute(self, cycle, dry_run=False):
            self._mark_launched()
            _stop(self)

    with (
        patch("sbn_parsl.app.parsl.load"),
        patch("sbn_parsl.app.parsl.clear"),
        patch("sbn_parsl.app.create_parsl_config"),
        patch("sbn_parsl.app.apply_hacks"),
    ):
        entry_point(argv, RecordingExecutor)
        assert len(executors) == 1
        assert (tmp_path / "runinfo" / ".launched").exists()

        # second launch: the resume check runs and finds the database
        with patch("builtins.print") as mock_print:
            entry_point(argv, RecordingExecutor)
        printed = " ".join(str(c.args) for c in mock_print.call_args_list)
        assert "FATAL" not in printed
        assert len(executors) == 2


def test_check_daos_config_requires_pool_and_cont(tmp_path):
    assert check_daos_config(_daos_cfg(tmp_path)) is True
    assert check_daos_config(_daos_cfg(tmp_path, pool=None)) is False
    assert check_daos_config(_daos_cfg(tmp_path, cont="")) is False


def test_check_daos_config_requires_runinfo():
    assert check_daos_config(_daos_cfg(None)) is False


def test_check_daos_config_disables_output_check(tmp_path):
    cfg = _daos_cfg(tmp_path)
    cfg.run.check_existing_outputs = True
    assert check_daos_config(cfg) is True
    assert cfg.run.check_existing_outputs is False


def test_check_daos_config_warns_outside_mount(tmp_path, capsys):
    assert check_daos_config(_daos_cfg(tmp_path, output="/lus/flare/out")) is True
    assert "not under the compute-node DAOS mount" in capsys.readouterr().out


def test_add_daos_filesystem_multiline_and_idempotent():
    cfg = SimpleNamespace(site=SimpleNamespace(
        scheduler_options="#PBS -l filesystems=home:flare\n#PBS -l place=scatter",
        pbs_filesystems="home:flare",
    ))
    add_daos_filesystem(cfg)
    add_daos_filesystem(cfg)
    assert cfg.site.scheduler_options == (
        "#PBS -l filesystems=home:flare:daos_user_fs\n#PBS -l place=scatter"
    )
    assert cfg.site.pbs_filesystems == "home:flare:daos_user_fs"


def test_add_daos_filesystem_without_directive():
    cfg = SimpleNamespace(site=SimpleNamespace(scheduler_options="",
                                               pbs_filesystems="home"))
    add_daos_filesystem(cfg)
    assert cfg.site.scheduler_options == "#PBS -l filesystems=home:daos_user_fs"


def test_provider_mounts_daos(tmp_path):
    provider = create_provider_by_hostname(_daos_cfg(tmp_path), local=True)
    assert "launch-dfuse.sh sbn_parsl_test_pool:sbn_parsl_test_cont" in (
        provider.worker_init
    )


def _g4_cmd(output_dir, parent_output):
    """Submit a g4 stage whose parent produced parent_output; return its cmd."""
    executor = MagicMock()
    executor.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output=str(output_dir), require_success=False),
    )
    executor.cfg.larsoft.container_path = "/c"
    executor.cfg.larsoft.larsoft_top = "/l"
    executor.output_dir = pathlib.Path(output_dir)
    executor.name_salt = "salt"
    executor.stage_in_db.return_value = False
    executor.dry_run = False
    executor.futures = set()
    executor.task_priority.return_value = 0
    executor._skip_counter = 0
    executor._file_skip_counter = 0
    executor._stage_counter = 0

    order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]
    stage = Stage(DefaultStageTypes.G4, stage_order=order)
    stage.workflow_id = 0
    stage.stage_id = (0,)
    stage._is_finalized = True

    captured = {}

    def mock_future_func(**kwargs):
        captured.update(kwargs)
        f = MagicMock()
        f.outputs = [MagicMock()]
        return f

    larsoft_runfunc(
        stage,
        fcl="g4.fcl",
        parent_result=StageResult(outputs=[parent_output]),
        run_dir=pathlib.Path(output_dir) / "run",
        template="{cmd}",
        executor=executor,
        future_func=mock_future_func,
    )
    return captured["cmd"]


def test_daos_stage_outputs_are_not_removed():
    """Outputs on a DAOS mount are under /tmp but are real stage outputs."""
    parent = f"{DAOS_OUT}/gen/000000/000000/gen-abc.root"
    cmd = _g4_cmd(DAOS_OUT, parent)
    assert f"rm {parent}" not in cmd


def test_combined_scratch_inputs_are_still_removed():
    """Combined-stage outputs go to /tmp/<stage>/ and should be cleaned up."""
    parent = "/tmp/gen/000000/000000/gen-abc.root"
    cmd = _g4_cmd(DAOS_OUT, parent)
    assert f"rm {parent}" in cmd
