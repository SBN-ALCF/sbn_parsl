#!/usr/bin/env python3

import os
import sqlite3
import json
import tempfile
import pytest
import shutil
import functools
import pathlib

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor

from sbn_parsl.workflow import Stage, Workflow, WorkflowExecutor, DefaultStageTypes
from sbn_parsl.components import _transfer_ids
 
@bash_app(cache=False)
def fail_app(outputs, stdout, stderr):
    # return f'mkdir -p $(dirname {outputs[0]}); touch {outputs[0]}; exit 1'
    return f'exit 1'

@bash_app(cache=False)
def pass_app(outputs, stdout, stderr):
    return f'mkdir -p $(dirname {outputs[0]}); touch {outputs[0]}; exit 0'

def default_parsl_runfunc(stage_self, fcl, input_files, output_dir, executor, app, retry_fails=False):
    """Default function called when each stage is run."""
    input_file_arg_str = ''
    if input_files is not None:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' for file in input_files])

    output_filename = os.path.basename(fcl).replace(".fcl", ".root")
    if output_dir is None:
        output_dir = pathlib.Path('.')
    output_file = output_dir / pathlib.Path(output_filename)
    output_file_arg_str = f'--output {str(output_file)}'

    if executor.stage_in_db(stage_self.stage_id_str, require_success=retry_fails):
        executor._skip_counter += 1
        return [output_file]

    future = app(
            outputs=[File(str(output_file))],
            stdout=str(output_dir / 'log.err'),
            stderr=str(output_dir / 'log.out')
    )
    _transfer_ids(stage_self, future.outputs[0])

    executor.futures.append(future.outputs[0])
    return [output_file]


def minimal_parsl_config(tmp_dir):
    """Returns a minimal Parsl config using ThreadPoolExecutor."""
    return Config(
        executors=[
            ThreadPoolExecutor(
                label="local_threads",
                max_threads=2
            )
        ],
        run_dir=os.path.join(tmp_dir, "runinfo")
    )


@pytest.fixture(scope="function")
def temp_db_dir():
    """Provides a temporary directory for the mysql db file."""
    tmp_dir = tempfile.mkdtemp()
    yield tmp_dir
    shutil.rmtree(tmp_dir)


class MinimalExecutor(WorkflowExecutor):
    def __init__(self, settings: json, retry_fails):
        super().__init__(settings)
        self.runfunc = functools.partial(
                default_parsl_runfunc, 
                executor=self, 
                retry_fails=retry_fails,
                app=fail_app
        )

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(stage_order=[DefaultStageTypes.GEN], default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.GEN)
        s.runfunc = self.runfunc
        s.run_dir = self.output_dir / 'gen'
        workflow.add_final_stage(s)

        return workflow


def test_workflow_caching_fail(temp_db_dir):
    '''Test that failed stage is not re-run.'''
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    settings = {
        'larsoft' : {},
        'run': {'nsubruns': 1, 'output': temp_db_dir, 'max_futures': -1, 'seed': ''},
        'fcls': {'gen': 'gen.fcl'},
        'workflow': {},
        'queue': {}
    }
    wfe = MinimalExecutor(settings, retry_fails=False)
    wfe.execute()

    # check that the status was written to the db
    disk_db = sqlite3.connect(str(wfe._db_file))
    cursor = disk_db.cursor()
    result = cursor.execute(
        "SELECT status FROM stages WHERE stage_id=(?)",
        ('0_0',)
    ).fetchone()[0]
    disk_db.close()
    assert result == 1
    parsl.clear()

    # now try again, task should get skipped
    parsl.load(config)
    wfe2 = MinimalExecutor(settings, retry_fails=False)
    wfe2.execute()
    parsl.clear()

    assert wfe2._skip_counter == 1


def test_workflow_caching_pass(temp_db_dir):
    '''Test that failed stage is not re-run.'''
    config = minimal_parsl_config(temp_db_dir)
    parsl.clear()
    parsl.load(config)

    settings = {
        'larsoft' : {},
        'run': {'nsubruns': 1, 'output': temp_db_dir, 'max_futures': -1, 'seed': ''},
        'fcls': {'gen': 'gen.fcl'},
        'workflow': {},
        'queue': {}
    }
    wfe = MinimalExecutor(settings, retry_fails=False)
    wfe.execute()

    # check that the status was written to the db
    disk_db = sqlite3.connect(str(wfe._db_file))
    cursor = disk_db.cursor()
    result = cursor.execute(
        "SELECT status FROM stages WHERE stage_id=(?)",
        ('0_0',)
    ).fetchone()[0]
    disk_db.close()
    assert result == 1
    parsl.clear()

    # now try again, task should get re-submitted
    parsl.load(config)
    wfe2 = MinimalExecutor(settings, retry_fails=True)
    wfe2.execute()
    parsl.clear()

    assert wfe2._skip_counter == 0

