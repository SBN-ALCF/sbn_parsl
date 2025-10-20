#!/usr/bin/env python

# This workflow runs the decoder on raw data files

import sys
import json
import pathlib
import functools
import itertools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
    DefaultStageTypes
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.templates import CMD_TEMPLATE_SPACK, CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import data_runfunc_icarus
from sbn_parsl.app import entry_point



class DecoderExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [DefaultStageTypes.from_str(key) for key in self.fcls.keys()]
        self.files_per_subrun = settings['run']['files_per_subrun']
        self.run_list = None
        if 'run_list' in settings['workflow']:
            with open(settings['workflow']['run_list'], 'r') as f:
                self.run_list = [int(l.strip()) for l in f.readlines()]

        self.rawdata_path = pathlib.Path(settings['workflow']['rawdata_path'])

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob('[0-9]?/*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(self, iteration: int, rawdata_files: List[pathlib.Path], last_file=None):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(data_runfunc_icarus, meta=self.meta, template=CMD_TEMPLATE_CONTAINER, executor=self, last_file=last_file)
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        # pass in extra larsoft opts to only process 5 events per task
        stage0_runfuncs = [functools.partial(data_runfunc_icarus, meta=self.meta, template=CMD_TEMPLATE_CONTAINER, executor=self, \
                nevts=1, nskip=(i * 5)) for i in range(10)]

        for i, file in enumerate(rawdata_files):
            for j in range(10):
                s2 = Stage(DefaultStageTypes.STAGE1)
                s2.run_dir = get_subrun_dir(self.output_dir, iteration * self.files_per_subrun + i) / f'{j:03d}'
                s.add_parents(s2, workflow.default_fcls)
                s3 = Stage(DefaultStageTypes.STAGE0, runfunc=stage0_runfuncs[j])
                s2.add_parents(s3, workflow.default_fcls)
                s3.add_input_file(file)

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


if __name__ == '__main__':
    entry_point(sys.argv, DecoderExecutor)
