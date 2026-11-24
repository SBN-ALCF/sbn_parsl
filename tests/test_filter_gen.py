"""Restart handling in the funnel workflow (workflows/sbnd_filter_gen.py): a
successful filter deletes its GEN inputs, so a cached GEN may have no file."""

import importlib.util
import pathlib
from types import SimpleNamespace

import pytest

from sbn_parsl.workflow import DefaultStageTypes

_SCRIPT = pathlib.Path(__file__).parent.parent / "workflows" / "sbnd_filter_gen.py"
_spec = importlib.util.spec_from_file_location("sbnd_filter_gen", _SCRIPT)
filter_gen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(filter_gen)

FCL = "prodgenie.fcl"


class FakeExecutor:
    def __init__(self, output_dir, cached, require_success=True):
        self.output_dir = output_dir
        self.name_salt = str(output_dir)
        self.dry_run = False
        self.cfg = SimpleNamespace(
            run=SimpleNamespace(require_success=require_success, output=str(output_dir)),
            larsoft=SimpleNamespace(nevts=10, nskip=0),
        )
        self.db = dict(cached)  # stage_id -> status
        self.marked = []

    def stage_in_db(self, stage_id, require_success=False):
        if stage_id not in self.db:
            return False
        return self.db[stage_id] == 0 if require_success else True

    def mark_stage_in_db(self, stage_id, status=0):
        self.marked.append((stage_id, status))
        self.db[stage_id] = status


def _gen_stage():
    return SimpleNamespace(
        stage_type=DefaultStageTypes.GEN,
        is_first=lambda: True,
        workflow_id=0,
        stage_id=(0, 0),
        stage_id_str="0_0_0",
    )


FILTER = SimpleNamespace(stage_id_str="0_0")


@pytest.fixture
def run_gen(monkeypatch, tmp_path):
    """Call gen_runfunc with mc_runfunc_sbnd stubbed out; return what the
    stub saw in the cache when it was called."""
    seen = {}

    def fake_mc_runfunc(stage, fcl, parent_result, run_dir, executor, **kwargs):
        seen["skipped"] = executor.stage_in_db(stage.stage_id_str, require_success=True)
        return "result"

    monkeypatch.setattr(filter_gen, "mc_runfunc_sbnd", fake_mc_runfunc)

    def _run(executor, gen_file_exists):
        stage = _gen_stage()
        if gen_file_exists:
            ctx = filter_gen.RunContext(
                stage=stage, cfg=executor.cfg, out_dir=executor.output_dir,
                fcl=pathlib.PurePosixPath(FCL), lar_args=executor.cfg.larsoft,
                salt=executor.name_salt,
            )
            path = pathlib.Path(filter_gen.output_filepath(ctx))
            path.parent.mkdir(parents=True)
            path.touch()
        out = filter_gen.gen_runfunc(
            stage, FCL, None, tmp_path, executor=executor, filter_stage=FILTER
        )
        assert out == "result"
        return seen["skipped"]

    return _run


def test_cached_gen_with_missing_file_and_unfinished_filter_reruns(run_gen, tmp_path):
    ex = FakeExecutor(tmp_path, cached={"0_0_0": 0})
    assert run_gen(ex, gen_file_exists=False) is False
    assert ex.marked == [("0_0_0", 1)]


def test_cached_gen_with_file_present_is_skipped(run_gen, tmp_path):
    ex = FakeExecutor(tmp_path, cached={"0_0_0": 0})
    assert run_gen(ex, gen_file_exists=True) is True
    assert ex.marked == []


def test_finished_filter_means_missing_gen_file_is_expected(run_gen, tmp_path):
    ex = FakeExecutor(tmp_path, cached={"0_0_0": 0, "0_0": 0})
    assert run_gen(ex, gen_file_exists=False) is True
    assert ex.marked == []


def test_uncached_gen_is_not_checked(run_gen, tmp_path):
    ex = FakeExecutor(tmp_path, cached={})
    assert run_gen(ex, gen_file_exists=False) is False
    assert ex.marked == []


def test_stream_path():
    assert str(filter_gen.stream_path("/a/b/filter-gen-x.root", "eta")) == "/a/b/filter-gen-x_eta.root"
