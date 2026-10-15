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
    assert " -T " not in build_larsoft_cmd(_reco2(), calib_ntuple_stage=DefaultStageTypes.RECO1)


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
