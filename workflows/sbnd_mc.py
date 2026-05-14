#!/usr/bin/env python

# This workflow generates full MC events from generator through CAF stage

import sys
import functools

from sbn_parsl.workflow import (
    StageType,
    Stage,
    Workflow,
    LArSoftExecutor,
    DefaultStageTypes,
)
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.experiments.sbnd import mc_runfunc_sbnd
from sbn_parsl.app import entry_point
from sbn_parsl.metadata import MetadataGenerator


from sbn_parsl.config import Config


class CAFFromGenExecutor(LArSoftExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self.meta = None
        if cfg.metadata.exe:
            self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)

        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = cfg.workflow.subruns_per_caf
        self.runfunc = functools.partial(
            mc_runfunc_sbnd,
            executor=self,
            template=CMD_TEMPLATE_CONTAINER,
            meta=self.meta,
        )

    def _setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.CAF)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = self.get_caf_dir(iteration)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i

            s_reco2 = Stage(DefaultStageTypes.RECO2)
            s_reco1 = Stage(DefaultStageTypes.RECO1)
            s_detsim = Stage(DefaultStageTypes.DETSIM)
            s_g4 = Stage(DefaultStageTypes.G4)
            s_gen = Stage(DefaultStageTypes.GEN)

            s.add_parents(s_reco2)
            s_reco2.add_parents(s_reco1)
            s_reco1.add_parents(s_detsim)
            s_detsim.add_parents(s_g4)
            s_g4.add_parents(s_gen)

            s_gen.combine = True
            s_reco2.combine = True

            # each workflow will have its own directory
            s_reco2.run_dir = self.get_run_dir(inst)

        return workflow

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.GEN)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = self.get_run_dir(iteration)

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, CAFFromGenExecutor)
