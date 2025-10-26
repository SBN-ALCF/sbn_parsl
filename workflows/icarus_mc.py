#!/usr/bin/env python

# This workflow runs the decoder on raw data files

import sys
import json
import time
import pathlib
import functools
import itertools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
    DefaultStageTypes
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.components import mc_runfunc_icarus
from sbn_parsl.templates import CMD_TEMPLATE_SPACK, CMD_TEMPLATE_CONTAINER
from sbn_parsl.app import entry_point


OVERLAY = StageType('overlay')
OVERLAY_WFM = StageType('overlay_wfm')


class DecoderExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [
            DefaultStageTypes.DECODE,
            OVERLAY,
            DefaultStageTypes.G4,
            DefaultStageTypes.DETSIM,
            OVERLAY_WFM,
            DefaultStageTypes.STAGE0,
            DefaultStageTypes.STAGE1,
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
        path_generators = [self.rawdata_path.rglob('*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(self, iteration: int, rawdata_files: List[pathlib.Path], last_file=None):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(mc_runfunc_icarus, template=CMD_TEMPLATE_CONTAINER, \
                meta=self.meta, executor=self, last_file=last_file)
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        decode_runfuncs = [functools.partial(mc_runfunc_icarus, template=CMD_TEMPLATE_CONTAINER, \
                meta=self.meta, executor=self, nevts=1, nskip=(i * 5)) for i in range(10)]

        for i, file in enumerate(rawdata_files):
            for j in range(10):
                s_stage1 = Stage(DefaultStageTypes.STAGE1)
                s_stage0 = Stage(DefaultStageTypes.STAGE0)
                s_overlay_wfm = Stage(OVERLAY_WFM)
                s_detsim = Stage(DefaultStageTypes.DETSIM)
                s_g4 = Stage(DefaultStageTypes.G4)
                s_overlay = Stage(OVERLAY)
                s_decode = Stage(DefaultStageTypes.DECODE, runfunc=decode_runfuncs[j])

                s_stage1.run_dir = get_subrun_dir(self.output_dir, iteration * self.files_per_subrun + i) / f'{j:03d}'

                s.add_parents(s_stage1, workflow.default_fcls)
                s_stage1.add_parents(s_stage0, workflow.default_fcls)
                s_stage0.add_parents(s_overlay_wfm, workflow.default_fcls)
                s_overlay_wfm.add_parents(s_detsim, workflow.default_fcls)
                s_detsim.add_parents(s_g4, workflow.default_fcls)
                s_g4.add_parents(s_overlay, workflow.default_fcls)
                s_overlay.add_parents(s_decode, workflow.default_fcls)

                s_decode.add_input_file(file)
                s_decode.combine = True
                s_detsim.combine = True

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


if __name__ == '__main__':
    entry_point(sys.argv, DecoderExecutor)
