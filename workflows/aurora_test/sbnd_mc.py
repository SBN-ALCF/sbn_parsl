#!/usr/bin/env python

# This workflow generates full MC events from generator through CAF stage

import sys
import json
import pathlib
import functools

from sbn_parsl.workflow import (
    StageType,
    Stage,
    Workflow,
    LArSoftExecutor,
    DefaultStageTypes,
)
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import mc_runfunc_sbnd
from sbn_parsl.app import entry_point
from sbn_parsl.metadata import MetadataGenerator


class CAFFromGenExecutor(LArSoftExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""

    def __init__(self, settings: json):
        super().__init__(settings)
        self.meta = MetadataGenerator(settings["metadata"], self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.subruns_per_caf = settings["workflow"]["subruns_per_caf"]
        self.runfunc = functools.partial(
            mc_runfunc_sbnd,
            executor=self,
            template=CMD_TEMPLATE_CONTAINER,
            meta=self.meta,
        )

    # up to reco1
    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.RECO1)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)

        return workflow

    # full version with caf
    def _setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        s = Stage(DefaultStageTypes.CAF)
        s.runfunc = self.runfunc
        workflow.add_final_stage(s)
        s.run_dir = get_caf_dir(self.output_dir, iteration)

        for i in range(self.subruns_per_caf):
            inst = iteration * self.subruns_per_caf + i
            # create reco2 file from MC, only need to specify the last stage
            # since there are no inputs
            s2 = Stage(DefaultStageTypes.RECO2)
            s.add_parents(s2)

            # each reco2 file will have its own directory
            s2.run_dir = get_subrun_dir(self.output_dir, inst)

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return (
        prefix
        / f"{(subrun // 1000):06d}"
        / f"{(subrun // 100):06d}"
        / f"subrun_{subrun:06d}"
    )


def get_caf_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/caf/XXXXXX"""
    return prefix / f"{(subrun // 1000):06d}" / "caf" / f"subrun_{subrun:06d}"


if __name__ == "__main__":
    entry_point(sys.argv, CAFFromGenExecutor)
