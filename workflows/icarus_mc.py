#!/usr/bin/env python

# This workflow runs the decoder on raw data files

import sys
import pathlib
import functools
import itertools
from typing import List

from sbn_parsl.workflow import (
    StageType,
    Stage,
    Workflow,
    LArSoftExecutor,
    DefaultStageTypes,
)
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.experiments.icarus import mc_runfunc_icarus
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.app import entry_point
from sbn_parsl.config import Config


OVERLAY = StageType("overlay")
OVERLAY_WFM = StageType("overlay_wfm")


class OverlayExecutor(LArSoftExecutor):
    """Execute a decoder workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)
        self.stage_order = [
            OVERLAY,
            DefaultStageTypes.G4,
            DefaultStageTypes.DETSIM,
            DefaultStageTypes.DECODE,
            OVERLAY_WFM,
            DefaultStageTypes.STAGE0,
            DefaultStageTypes.STAGE1,
            DefaultStageTypes.CAF,
        ]

        # without this, workflow tries to make DefaultStageOrders out of
        # strings, but this fails since we added some custom ones
        self.fcls = {so: cfg.workflow.fcls[so.name] for so in self.stage_order}

        self.files_per_subrun = cfg.run.files_per_subrun
        self.run_list = None

        wf_dict = cfg.workflow.extra
        if wf_dict.get("run_list"):
            with open(wf_dict["run_list"], "r") as f:
                self.run_list = [int(line.strip()) for line in f.readlines()]

        self.rawdata_path = pathlib.Path(wf_dict.get("rawdata_path", "."))

        # event splitting: e.g., 50 events/file split by 10 -> 5 events per task
        self.max_events_per_file = int(wf_dict.get("events_per_file", 1))
        self.events_per_split = int(wf_dict.get("events_per_split", 1))

        # ceiling division
        self._nsplits = -(self.max_events_per_file // -self.events_per_split)
        if self._nsplits < 1:
            self._nsplits = 1
            self.max_events_per_file = -1
        if self._nsplits > 1:
            print(
                f"Using event splitting: {self.events_per_split} events per file, assuming {self.max_events_per_file} => {self._nsplits} splits"
            )

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob("*.root")]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(
        self, iteration: int, rawdata_files: List[pathlib.Path], last_file=None
    ):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(
            mc_runfunc_icarus,
            template=CMD_TEMPLATE_CONTAINER,
            meta=self.meta,
            executor=self,
            last_file=last_file,
        )
        runfunc_no_meta = functools.partial(
            mc_runfunc_icarus,
            template=CMD_TEMPLATE_CONTAINER,
            meta=None,
            executor=self,
            last_file=last_file,
        )

        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = self.get_run_dir(iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        # set up larsoft runfuncs to skip & split
        input_runfuncs = [
            functools.partial(
                mc_runfunc_icarus,
                template=CMD_TEMPLATE_CONTAINER,
                meta=None,
                executor=self,
                nevts=self.events_per_split,
                nskip=(i * self.events_per_split),
            )
            for i in range(self._nsplits)
        ]

        for i, file in enumerate(rawdata_files):
            for j in range(self._nsplits):
                s_stage1 = Stage(DefaultStageTypes.STAGE1)
                s_stage0 = Stage(DefaultStageTypes.STAGE0)

                s_overlay_wfm = Stage(OVERLAY_WFM, runfunc=runfunc_no_meta)
                s_decode = Stage(DefaultStageTypes.DECODE, runfunc=runfunc_no_meta)
                s_detsim = Stage(DefaultStageTypes.DETSIM, runfunc=runfunc_no_meta)

                s_g4 = Stage(DefaultStageTypes.G4, runfunc=runfunc_no_meta)
                s_overlay = Stage(OVERLAY, runfunc=input_runfuncs[j])

                s_stage1.run_dir = (
                    self.get_run_dir(iteration * self.files_per_subrun + i) / f"{j:03d}"
                )
                s_stage0.run_dir = (
                    self.get_run_dir(iteration * self.files_per_subrun + i) / f"{j:03d}"
                )

                s.add_parents(s_stage1)
                s_stage1.add_parents(s_stage0)
                s_stage0.add_parents(s_overlay_wfm)
                s_overlay_wfm.add_parents(s_decode)
                s_decode.add_parents(s_detsim)
                s_detsim.add_parents(s_g4)
                s_g4.add_parents(s_overlay)

                s_overlay.add_input_file(file)

                # combines stage1 with caf
                # s_stage1.combine = True

                # combines overlay with G4
                # s_overlay.combine = True

                # combines detsim & decode with overlay_wfm
                # s_detsim.combine = True
                # s_decode.combine = True

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, OverlayExecutor)
