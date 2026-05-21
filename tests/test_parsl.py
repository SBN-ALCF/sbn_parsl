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


def test_workflow_caching_fail(temp_db_dir):
    """Test that failed stage is not re-run if require_success is False."""
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    cfg = create_mock_config(temp_db_dir)
    wfe = MinimalExecutor(cfg, retry_fails=False)
    wfe.execute()

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


def test_workflow_caching_pass(temp_db_dir):
    """Test that failed stage is re-run if require_success is True."""
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    cfg = create_mock_config(temp_db_dir)
    wfe = MinimalExecutor(cfg, retry_fails=False)
    wfe.execute()

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
