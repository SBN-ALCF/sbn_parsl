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
from sbn_parsl.experiments.icarus import data_runfunc_icarus
from sbn_parsl.app import entry_point
from sbn_parsl.config import Config


class DecoderExecutor(LArSoftExecutor):
    """Execute a decoder workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)
        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        self.files_per_subrun = cfg.run.files_per_subrun
        self.run_list = None

        wf_dict = cfg.workflow.extra
        if wf_dict.get("run_list"):
            with open(wf_dict["run_list"], "r") as f:
                self.run_list = [int(line.strip()) for line in f.readlines()]

        self.rawdata_path = pathlib.Path(wf_dict.get("rawdata_path", "."))

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob("[0-9]?/*.root")]
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
            data_runfunc_icarus,
            meta=self.meta,
            template=CMD_TEMPLATE_CONTAINER,
            executor=self,
            last_file=last_file,
        )
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = self.get_run_dir(iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        # pass in extra larsoft opts to only process 5 events per task
        stage0_runfuncs = [
            functools.partial(
                data_runfunc_icarus,
                meta=self.meta,
                template=CMD_TEMPLATE_CONTAINER,
                executor=self,
                nevts=5,
                nskip=(i * 5),
            )
            for i in range(10)
        ]

        for i, file in enumerate(rawdata_files):
            for j in range(10):
                s2 = Stage(DefaultStageTypes.STAGE1)
                s2.run_dir = (
                    self.get_run_dir(iteration * self.files_per_subrun + i) / f"{j:03d}"
                )
                s.add_parents(s2)
                s3 = Stage(DefaultStageTypes.STAGE0, runfunc=stage0_runfuncs[j])
                s2.add_parents(s3)
                s3.add_input_file(file)

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, DecoderExecutor)
