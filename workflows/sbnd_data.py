#!/usr/bin/env python

# This workflow runs the decoder on raw data files

import sys, os
import json
import pathlib
import functools
import itertools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
    DefaultStageTypes
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.templates import CMD_TEMPLATE_SPACK, CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import data_runfunc_sbnd
from sbn_parsl.app import entry_point


POT = StageType('pot')


class DecoderExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [
            DefaultStageTypes.DECODE,
            POT,
            DefaultStageTypes.RECO1,
            DefaultStageTypes.RECO2,
            DefaultStageTypes.CAF,
        ]
        # without this, workflow tries to make DefaultStageOrders out of
        # strings, but this fails since we added some custom ones
        self.fcls = { so: settings['fcls'][so.name] for so in self.stage_order }

        self.files_per_subrun = settings['run']['files_per_subrun']
        self.run_list = None
        if 'run_list' in settings['workflow']:
            with open(settings['workflow']['run_list'], 'r') as f:
                self.run_list = [int(l.strip()) for l in f.readlines()]

        self.rawdata_path = pathlib.Path(settings['workflow']['rawdata_path'])

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob('*strmBNBZeroBias*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(self, iteration: int, rawdata_files: List[pathlib.Path], last_file=None):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(data_runfunc_sbnd, meta=self.meta, 
                                     template=CMD_TEMPLATE_CONTAINER, executor=self, last_file=last_file)
        runfunc_no_meta = functools.partial(data_runfunc_sbnd, meta=None, 
                                     template=CMD_TEMPLATE_CONTAINER, executor=self, last_file=last_file)
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        for i, file in enumerate(rawdata_files):
            sreco2 = Stage(DefaultStageTypes.RECO2)
            sreco2.run_dir = get_subrun_dir(self.output_dir, iteration * self.files_per_subrun + i)
            s.add_parents(sreco2, workflow.default_fcls)

            sreco1 = Stage(DefaultStageTypes.RECO1)
            sreco2.add_parents(sreco1, workflow.default_fcls)

            spot = Stage(POT)
            spot.runfunc = runfunc_no_meta
            sreco1.add_parents(spot, workflow.default_fcls)

            sdecode = Stage(DefaultStageTypes.DECODE)
            sdecode.runfunc = runfunc_
            spot.add_parents(sdecode, workflow.default_fcls)

            sdecode.add_input_file(file)
            
            # run decode & POT stage in the same job
            sdecode.combine = True

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


if __name__ == '__main__':
    entry_point(sys.argv, DecoderExecutor)
