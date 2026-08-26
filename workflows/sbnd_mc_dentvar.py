#!/usr/bin/env python

# This workflow generates full MC events from generator through CAF stage

import sys
import json
import pathlib
import functools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, LArSoftExecutor, \
        DefaultStageTypes
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.experiments.sbnd import mc_runfunc_sbnd
from sbn_parsl.app import entry_point
from sbn_parsl.metadata import MetadataGenerator


class CAFFromGenExecutor(LArSoftExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, cfg):
        super().__init__(cfg)
        self.meta = None
        self.cv_fcls = self.fcls['cv']
        self.vars = [k for k in self.fcls if k != 'cv']
        if cfg.larsoft.metadata.exe:
            self.meta = MetadataGenerator(cfg, self.cv_fcls, defer_check=True)
        self.var_fcls = {var: self.fcls[var] for var in self.vars}

        self.stage_order = [StageType.from_str(key) for key in self.cv_fcls.keys()]
        self.var_stage_order = [StageType.from_str(key) for key in self.cv_fcls.keys() if key != 'gen']

        self.subruns_per_caf = cfg.workflow.subruns_per_caf
        self.runfunc = functools.partial(
            mc_runfunc_sbnd,
            label='cv',
            executor=self,
            template=CMD_TEMPLATE_CONTAINER,
            meta=None,
        )
        self.var_runfuncs = {
                var: functools.partial(
                    mc_runfunc_sbnd,
                    executor=self,
                    label=var,
                    template=CMD_TEMPLATE_CONTAINER,
                    meta=None,
                    )
                for var in self.vars
                }

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.cv_fcls)
        runfunc_ = self.runfunc
        s_cv = Stage(DefaultStageTypes.CAF)
        s_cv.runfunc = self.runfunc
        s_cv.run_dir = get_caf_dir(self.output_dir, iteration) / 'cv'
        workflow.add_final_stage(s_cv)

        for var in self.vars:
            s_var = Stage(DefaultStageTypes.CAF, fcl=self.var_fcls[var]['caf'])
            s_var.runfunc = self.var_runfuncs[var]
            s_var.run_dir = get_caf_dir(self.output_dir, iteration) / var
            workflow.add_final_stage(s_var)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            s_reco2 = Stage(DefaultStageTypes.RECO2)
            s_reco1 = Stage(DefaultStageTypes.RECO1)
            s_detsim = Stage(DefaultStageTypes.DETSIM)
            s_g4 = Stage(DefaultStageTypes.G4)
            s_gen = Stage(DefaultStageTypes.GEN)

            s_cv.add_parents(s_reco2)
            s_reco2.add_parents(s_reco1)
            s_reco1.add_parents(s_detsim)
            s_detsim.add_parents(s_g4)
            s_g4.add_parents(s_gen)

            s_g4.combine = True
            s_reco2.combine = True

            # each workflow will have its own directory
            s_reco2.run_dir = get_subrun_dir(self.output_dir, inst) / 'cv'

            for var in self.vars:
                s_reco2_var = Stage(DefaultStageTypes.RECO2, fcl=self.var_fcls[var]['reco2'])
                s_reco1_var = Stage(DefaultStageTypes.RECO1, fcl=self.var_fcls[var]['reco1'])
                s_detsim_var = Stage(DefaultStageTypes.DETSIM, fcl=self.var_fcls[var]['detsim'])
                s_g4_var = Stage(DefaultStageTypes.G4, fcl=self.var_fcls[var]['g4'])

                s_var.add_parents(s_reco2_var)
                s_reco2_var.add_parents(s_reco1_var)
                s_reco1_var.add_parents(s_detsim_var)
                s_detsim_var.add_parents(s_g4_var)

                # same GEN stage parent as CV
                s_g4_var.add_parents(s_gen)

                s_g4_var.combine = True
                s_reco2.combine = True

                # each workflow will have its own directory
                s_reco2_var.run_dir = get_subrun_dir(self.output_dir, inst) / var


        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / f"{(subrun//100):06d}" / f"subrun_{subrun:06d}"

def get_caf_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/caf/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / 'caf' / f"subrun_{subrun:06d}"

if __name__ == '__main__':
    entry_point(sys.argv, CAFFromGenExecutor)
