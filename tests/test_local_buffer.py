import pathlib
from unittest.mock import MagicMock

import pytest

from sbn_parsl.components import (
    RunContext,
    build_larsoft_cmd,
    output_filepath_generic,
)
from sbn_parsl.config import LArSoftConfig, RunConfig
from sbn_parsl.experiments.icarus import build_modify_fcl_cmd_icarus
from sbn_parsl.experiments.sbnd import build_modify_fcl_cmd_sbnd_mc
from sbn_parsl.workflow import DefaultStageTypes, Stage

OUT = "/out"
KEEP = "$SBN_LOCAL_BUFFER/keep"
DISCARD = "$SBN_LOCAL_BUFFER/discard"


def _context(stage_type, parent_type, combined=False, label=""):
    stage = Stage(stage_type, stage_order=[parent_type, stage_type])
    stage.workflow_id = 0
    stage.stage_id = (0,)
    stage._is_finalized = True
    cfg = MagicMock()
    cfg.run = RunConfig(output=OUT)
    context = RunContext(
        stage=stage,
        cfg=cfg,
        input_files=[pathlib.PurePosixPath(f"/out/{parent_type.name}/000000/000000/in.root")],
        # combined stages write their own output to /tmp, see larsoft_runfunc
        out_dir=pathlib.Path("/tmp") if combined else pathlib.Path(OUT),
        fcl=pathlib.PurePosixPath(f"{stage_type.name}.fcl"),
        lar_args=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        label=label,
    )
    context.output_file = output_filepath_generic(context, is_mc=False, use_label=True)
    return context


def _reco2(**kw):
    return _context(DefaultStageTypes.RECO2, DefaultStageTypes.RECO1, **kw)


@pytest.mark.parametrize("combined", [False, True])
def test_calib_ntuple_kept_under_output_dir(combined):
    """A combined reco2 used to send its ntuple to /tmp/calib_ntuple, never copied."""
    cmd = build_larsoft_cmd(_reco2(combined=combined), calib_ntuple_stage=DefaultStageTypes.RECO2)
    path = f"{KEEP}/calib_ntuple/000000/000000/hists_in.root"
    assert f" -T {path}" in cmd
    assert f"mkdir -p {KEEP}/calib_ntuple/000000/000000\n" in cmd


def test_calib_ntuple_can_be_discarded():
    cmd = build_larsoft_cmd(
        _reco2(), calib_ntuple_stage=DefaultStageTypes.RECO2, save_calib_ntuple=False
    )
    assert f" -T {DISCARD}/hists_in.root" in cmd
    assert KEEP not in cmd


def test_calib_ntuple_keeps_label():
    cmd = build_larsoft_cmd(_reco2(label="lbl"), calib_ntuple_stage=DefaultStageTypes.RECO2)
    assert f" -T {KEEP}/lbl/calib_ntuple/000000/000000/hists_in.root" in cmd


def test_no_calib_ntuple_for_other_stages():
    cmd = build_larsoft_cmd(_reco2(), calib_ntuple_stage=DefaultStageTypes.RECO1)
    assert "calib_ntuple" not in cmd
    assert " -T $SBN_LOCAL_BUFFER/keep/reco2/000000/000000/hists_" in cmd


# histograms: every non-calibration stage now names its TFileService file, so
# it no longer lands in the shared run dir and gets swept into the final
# stage's output directory


def _hists_path(cmd):
    return cmd.split(" -T ")[1].split()[0]


def test_hists_kept_in_own_output_dir():
    ctx = _context(DefaultStageTypes.G4, DefaultStageTypes.GEN)
    assert _hists_path(build_larsoft_cmd(ctx)) == (
        f"{KEEP}/g4/000000/000000/hists_{ctx.output_file.name}"
    )


def test_combined_stage_hists_discarded():
    ctx = _context(DefaultStageTypes.G4, DefaultStageTypes.GEN, combined=True)
    ctx.stage.combine = True
    assert _hists_path(build_larsoft_cmd(ctx)) == f"{DISCARD}/hists_{ctx.output_file.name}"


def test_hists_can_be_discarded():
    ctx = _context(DefaultStageTypes.G4, DefaultStageTypes.GEN)
    assert _hists_path(build_larsoft_cmd(ctx, save_hists=False)) == (
        f"{DISCARD}/hists_{ctx.output_file.name}"
    )


def test_combined_calib_ntuple_still_kept():
    """Discarding combined-stage histograms must not discard the calib ntuple."""
    ctx = _reco2(combined=True)
    ctx.stage.combine = True
    cmd = build_larsoft_cmd(ctx, calib_ntuple_stage=DefaultStageTypes.RECO2)
    assert _hists_path(cmd) == f"{KEEP}/calib_ntuple/000000/000000/hists_in.root"


def _larcv_out(fcl_cmd, module):
    line = next(l for l in fcl_cmd.splitlines() if f"{module}.out_filename" in l)
    return line.split('\\"')[1]


@pytest.mark.parametrize("combined", [False, True])
def test_sbnd_larcv_kept_under_output_dir(combined):
    ctx = _context(DefaultStageTypes.RECO1, DefaultStageTypes.DETSIM, combined=combined)
    cmd = build_modify_fcl_cmd_sbnd_mc(ctx)
    assert _larcv_out(cmd, "supera") == f"{KEEP}/larcv/000000/000000/larcv_{ctx.output_file.name}"
    assert f"mkdir -p {KEEP}/larcv/000000/000000" in cmd


def test_sbnd_larcv_can_be_discarded():
    ctx = _context(DefaultStageTypes.RECO1, DefaultStageTypes.DETSIM)
    cmd = build_modify_fcl_cmd_sbnd_mc(ctx, save_larcv=False)
    assert _larcv_out(cmd, "supera") == f"{DISCARD}/larcv_{ctx.output_file.name}"


@pytest.mark.parametrize("combined", [False, True])
def test_icarus_larcv_kept_under_output_dir(combined):
    """Used to depend on stage1 being the last stage in its task."""
    ctx = _context(DefaultStageTypes.STAGE1, DefaultStageTypes.STAGE0, combined=combined)
    cmd = build_modify_fcl_cmd_icarus(ctx, is_mc=True)
    assert _larcv_out(cmd, "superaMC") == f"{KEEP}/larcv/000000/000000/larcv_{ctx.output_file.name}"


def test_icarus_larcv_keeps_label():
    """Used to drop any path components between the output dir and stage1."""
    ctx = _context(DefaultStageTypes.STAGE1, DefaultStageTypes.STAGE0, label="lbl")
    cmd = build_modify_fcl_cmd_icarus(ctx, is_mc=False)
    assert _larcv_out(cmd, "supera") == f"{KEEP}/lbl/larcv/000000/000000/larcv_{ctx.output_file.name}"


def test_icarus_larcv_can_be_discarded():
    ctx = _context(DefaultStageTypes.STAGE1, DefaultStageTypes.STAGE0)
    cmd = build_modify_fcl_cmd_icarus(ctx, save_larcv=False)
    assert _larcv_out(cmd, "supera") == f"{DISCARD}/larcv_{ctx.output_file.name}"


# metadata JSON: named with a full path into the stage's own output directory,
# and not generated at all for combined stages


def _run_stage(stage_type, parent_type, combine=False):
    from sbn_parsl.components import larsoft_runfunc
    from sbn_parsl.config import Config, JobConfig, SiteConfig, WorkflowConfig
    from sbn_parsl.metadata import MetadataGenerator
    from sbn_parsl.workflow import StageResult

    executor = MagicMock()
    executor.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output=OUT, require_success=False),
    )
    executor.output_dir = pathlib.Path(OUT)
    executor.name_salt = "salt"
    executor.stage_in_db.return_value = False
    executor.dry_run = False
    executor.futures = set()
    executor.task_priority.return_value = 0
    executor._skip_counter = executor._file_skip_counter = executor._stage_counter = 0

    stage = Stage(stage_type, stage_order=[parent_type, stage_type])
    stage.workflow_id = 0
    stage.stage_id = (0,)
    stage.combine = combine
    stage._is_finalized = True

    fcls = {parent_type.name: parent_type.name + ".fcl", stage_type.name: stage_type.name + ".fcl"}
    meta = MetadataGenerator({"exe": "injector.sh"}, fcls, defer_check=True)
    captured = {}

    def future_func(**kwargs):
        captured.update(kwargs)
        f = MagicMock()
        f.outputs = [MagicMock()]
        return f

    result = larsoft_runfunc(
        stage,
        fcl=stage_type.name + ".fcl",
        parent_result=StageResult(outputs=[f"/out/{parent_type.name}/000000/000000/in.root"]),
        run_dir=pathlib.Path(OUT) / "000000" / "000000" / "subrun_000000",
        template="{cmd}",
        executor=executor,
        future_func=future_func,
        meta=meta,
    )
    return captured, result


def test_json_kept_in_own_output_dir():
    captured, _ = _run_stage(DefaultStageTypes.RECO1, DefaultStageTypes.DETSIM)
    output = pathlib.PurePosixPath(captured["outputs"][0].filepath)
    json_file = f"{KEEP}/reco1/000000/000000/{output.name}.json"
    assert f"--tfilemdjsonname {json_file}" in captured["pre_job_hook"]
    assert f"mkdir -p {KEEP}/reco1/000000/000000" in captured["pre_job_hook"]


def test_no_metadata_for_combined_stage():
    _, result = _run_stage(DefaultStageTypes.G4, DefaultStageTypes.GEN, combine=True)
    # a combined stage hands its whole command to its child instead of submitting
    assert result.command
    assert "injector.sh" not in result.command
    assert "tfilemdjsonname" not in result.command
