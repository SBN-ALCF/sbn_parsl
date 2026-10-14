import functools
import pathlib
from unittest.mock import MagicMock, patch

from sbn_parsl.components import (
    build_caf_filename_cmd,
    larsoft_runfunc,
    output_filepath_generic,
    regular_caf_path,
)
from sbn_parsl.config import Config, JobConfig, LArSoftConfig, RunConfig, SiteConfig, WorkflowConfig
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.workflow import DefaultStageTypes, Stage, StageResult

OUT = pathlib.PurePosixPath("/out")
INPUT = "/out/reco2/000000/000000/reco2-abc.root"


def _caf_stage():
    stage = Stage(DefaultStageTypes.CAF, stage_order=[DefaultStageTypes.RECO2, DefaultStageTypes.CAF])
    stage.workflow_id = 0
    stage.stage_id = (0,)
    stage._is_finalized = True
    return stage


def _submit_caf(output_func, meta=None):
    """Run larsoft_runfunc for a CAF stage; return the kwargs it submits."""
    executor = MagicMock()
    executor.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output=str(OUT), require_success=False),
    )
    executor.output_dir = pathlib.Path(OUT)
    executor.name_salt = "salt"
    executor.stage_in_db.return_value = False
    executor.dry_run = False
    executor.futures = set()
    executor.task_priority.return_value = 0
    executor._skip_counter = executor._file_skip_counter = executor._stage_counter = 0

    captured = {}

    def future_func(**kwargs):
        captured.update(kwargs)
        f = MagicMock()
        f.outputs = [MagicMock()]
        return f

    larsoft_runfunc(
        _caf_stage(),
        fcl="cafmaker.fcl",
        parent_result=StageResult(outputs=[INPUT]),
        run_dir=pathlib.Path(OUT) / "000000" / "caf" / "subrun_000000",
        template="{cmd}",
        executor=executor,
        future_func=future_func,
        output_filename_func=output_func,
        meta=meta,
    )
    return captured


unblinded = functools.partial(output_filepath_generic, is_mc=True)
blinded = functools.partial(output_filepath_generic, is_mc=False, blind_caf=True)

FLAT = OUT / "flatcaf/000000/000000/reco2-abc.flat.caf.root"
CAF = OUT / "caf/000000/000000/reco2-abc.caf.root"


def test_flat_caf_goes_to_flatcaf():
    out = _submit_caf(unblinded)
    assert out["outputs"][0].filepath == str(FLAT)


def test_blinded_caf_layout_unchanged():
    out = _submit_caf(blinded)
    assert out["outputs"][0].filepath == str(
        OUT / "caf/000000/000000/reco2-abc.Blind.OKTOLOOK.flat.caf.root"
    )
    assert "CAFFilename" not in out["pre_job_hook"]


def test_regular_caf_path():
    assert regular_caf_path(FLAT) == CAF


def test_caf_filenames_injected_into_fcl():
    hook = _submit_caf(unblinded)["pre_job_hook"]
    assert f"mkdir -p {CAF.parent}" in hook
    assert f'physics.producers.cafmaker.CAFFilename: \\"{CAF}\\"" >> cafmaker.fcl' in hook
    assert f'physics.producers.cafmaker.FlatCAFFilename: \\"{FLAT}\\"" >> cafmaker.fcl' in hook


def test_no_caf_clause_for_other_stages():
    ctx = MagicMock()
    ctx.stage.stage_type = DefaultStageTypes.RECO2
    ctx.output_file = pathlib.PurePosixPath("/out/reco2/000000/000000/x.root")
    assert build_caf_filename_cmd(ctx) == ""


def _meta():
    return MetadataGenerator(
        {"exe": "injector.sh"}, {"reco2": "reco2.fcl", "caf": "cafmaker.fcl"}, defer_check=True
    )


def test_fcl_injection_runs_before_metadata():
    hook = _submit_caf(unblinded, meta=_meta())["pre_job_hook"]
    assert hook.index("CAFFilename") < hook.index("injector.sh")
    assert "--cafname" not in hook


def test_metadata_has_no_cafname():
    assert "cafname" not in _meta().metadata_stage("caf")


def test_configured_cafname_is_ignored(capsys):
    meta = MetadataGenerator(
        {"exe": "injector.sh", "cafname": "caf"}, {"caf": "cafmaker.fcl"}, defer_check=True
    )
    assert "cafname" not in meta.metadata_stage("caf")
    assert "'cafname' is ignored" in capsys.readouterr().out


def test_icarus_reprocess_reweights_regular_caf():
    """UpdateReweight used to read a cwd file literally named 'caf'."""
    import importlib.util

    script = pathlib.Path(__file__).parent.parent / "workflows" / "icarus_reprocess.py"
    spec = importlib.util.spec_from_file_location("icarus_reprocess", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    ctx = MagicMock()
    ctx.stage.stage_type = DefaultStageTypes.CAF
    ctx.output_file = FLAT
    with patch.object(module, "build_larsoft_cmd", return_value="lar"):
        cmd = module.build_larsoft_cmd_caf(ctx, "env.sh", "nusyst.fcl")
    assert f'echo "{CAF}" > list.tmp' in cmd
    assert f"UpdateReweight -c nusyst.fcl -i list.tmp -o {FLAT}" in cmd
