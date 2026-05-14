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
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.experiments.sbnd import data_runfunc_sbnd
from sbn_parsl.app import entry_point
from sbn_parsl.config import Config


POT = StageType("pot")


class DecoderExecutor(LArSoftExecutor):
    """Execute a decoder workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)
        self.stage_order = [
            DefaultStageTypes.DECODE,
            POT,
            DefaultStageTypes.RECO1,
            DefaultStageTypes.RECO2,
            DefaultStageTypes.CAF,
        ]
        # without this, workflow tries to make DefaultStageOrders out of
        # strings, but this fails since we added some custom ones
        self.fcls = {so: cfg.workflow.fcls[so.name] for so in self.stage_order}

        self.files_per_subrun = cfg.run.files_per_subrun
        self.run_list = None
        # workflow settings are in cfg.workflow or custom fields in TOML
        # Assuming they are mapped to the Config dataclasses.
        # Based on config.py, we might need to handle extra fields or ensure they are there.
        # For now, following established pattern.

        # Note: rawdata_path and others might need to be added to Config if not there.
        # Looking at config.py, WorkflowConfig only has subruns_per_caf, full_keep_fraction, fcls.
        # We might need to access them via cfg.workflow.__dict__ if they were extra in TOML.
        wf_dict = cfg.workflow.__dict__
        if "run_list" in wf_dict:
            with open(wf_dict["run_list"], "r") as f:
                self.run_list = [int(line.strip()) for line in f.readlines()]

        self.rawdata_path = pathlib.Path(wf_dict.get("rawdata_path", "."))

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob("*strmBNBZeroBias*.root")]
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
            data_runfunc_sbnd,
            meta=self.meta,
            template=CMD_TEMPLATE_CONTAINER,
            executor=self,
            last_file=last_file,
        )
        runfunc_no_meta = functools.partial(
            data_runfunc_sbnd,
            meta=None,
            template=CMD_TEMPLATE_CONTAINER,
            executor=self,
            last_file=last_file,
        )
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = self.get_run_dir(iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        for i, file in enumerate(rawdata_files):
            sreco2 = Stage(DefaultStageTypes.RECO2)
            sreco2.run_dir = self.get_run_dir(iteration * self.files_per_subrun + i)
            s.add_parents(sreco2)

            sreco1 = Stage(DefaultStageTypes.RECO1)
            sreco2.add_parents(sreco1)

            spot = Stage(POT)
            spot.runfunc = runfunc_no_meta
            sreco1.add_parents(spot)

            sdecode = Stage(DefaultStageTypes.DECODE)
            sdecode.runfunc = runfunc_
            spot.add_parents(sdecode)

            sdecode.add_input_file(file)

            # run decode & POT stage in the same job
            sdecode.combine = True

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, DecoderExecutor)
