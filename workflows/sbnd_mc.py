#!/usr/bin/env python

# This workflow generates full MC events from generator through CAF stage

import sys
import json
import pathlib
import functools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
        DefaultStageTypes
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import mc_runfunc_sbnd
from sbn_parsl.app import entry_point
from sbn_parsl.metadata import MetadataGenerator


class CAFFromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)
        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [DefaultStageTypes.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = settings['workflow']['subruns_per_caf']
        self.runfunc = functools.partial(mc_runfunc_sbnd, executor=self, 
                                         template=CMD_TEMPLATE_CONTAINER,
                                         meta=self.meta)

    def _setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = self.runfunc
        s = Stage(DefaultStageTypes.CAF)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = get_caf_dir(self.output_dir, iteration)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i

            s_reco2 = Stage(DefaultStageTypes.RECO2)
            s_reco1 = Stage(DefaultStageTypes.RECO1)
            s_detsim = Stage(DefaultStageTypes.DETSIM)
            s_g4 = Stage(DefaultStageTypes.G4)
            s_gen = Stage(DefaultStageTypes.GEN)

            s.add_parents(s_reco2, workflow.default_fcls)
            s_reco2.add_parents(s_reco1, workflow.default_fcls)
            s_reco1.add_parents(s_detsim, workflow.default_fcls)
            s_detsim.add_parents(s_g4, workflow.default_fcls)
            s_g4.add_parents(s_gen, workflow.default_fcls)

            s_gen.combine = True
            s_reco2.combine = True

            # each workflow will have its own directory
            s_reco2.run_dir = get_subrun_dir(self.output_dir, inst)

        return workflow

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = self.runfunc
        s = Stage(DefaultStageTypes.GEN)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / f"{(subrun//100):06d}" / f"subrun_{subrun:06d}"

def get_caf_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/caf/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / 'caf' / f"subrun_{subrun:06d}"

if __name__ == '__main__':
    entry_point(sys.argv, CAFFromGenExecutor)
