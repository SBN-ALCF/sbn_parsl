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
from sbn_parsl.components import RunContext, larsoft_runfunc, build_larsoft_cmd, output_filepath_generic
from sbn_parsl.experiments.icarus import data_runfunc_icarus, build_modify_fcl_cmd_icarus
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.app import entry_point
from sbn_parsl.config import Config


POT = StageType("pot")


class DataExecutor(LArSoftExecutor):
    """Execute a decoder workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.stage_order = [
            POT,
            DefaultStageTypes.STAGE1,
            DefaultStageTypes.CAF,
        ]

        # without this, workflow tries to make DefaultStageOrders out of
        # strings, but this fails since we added some custom ones
        # self.fcls = {so: cfg.workflow.fcls[so.name] for so in self.stage_order}
        self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)

        self.files_per_subrun = cfg.run.files_per_subrun
        self.run_list = None

        self._file_map = {}
        with open(cfg.workflow.file_map, 'r') as f:
            for line in f.readlines():
                fname, subdir = line.strip().split()
                self._file_map[fname] = subdir

            print(f'loaded {len(self._file_map)} lines')

        wf_dict = cfg.workflow.extra
        if wf_dict.get("run_list"):
            with open(wf_dict["run_list"], "r") as f:
                self.run_list = [int(line.strip()) for line in f.readlines()]

        self.stage0_path = wf_dict.get("stage0_path", ".")

        caf_runfunc = functools.partial(
            larsoft_runfunc,
            lar_cmd_func=build_larsoft_cmd,
            output_filename_func=functools.partial(
                output_filepath_generic, is_mc=True, use_label=False, include_skip=True
                ),
            fcl_cmd_func=build_modify_fcl_cmd_icarus,
        )
        self._caf_runfunc = functools.partial(caf_runfunc, executor=self,
                                              template=CMD_TEMPLATE_CONTAINER,
                                              meta=self.meta)

        self._runfunc = functools.partial(
            data_runfunc_icarus,
            template=CMD_TEMPLATE_CONTAINER,
            meta=self.meta,
            executor=self,
            last_file=None,
        )
        self._runfunc_no_meta = functools.partial(
            data_runfunc_icarus,
            template=CMD_TEMPLATE_CONTAINER,
            meta=None,
            executor=self,
            last_file=None,
            nevts=1
        )


    def file_generator(self):
        # for CV
        if isinstance(self.stage0_path, str):
            p = pathlib.Path(self.stage0_path) 
            path_generators = [p.rglob("reco1-000*.root")]
        else:
            path_generators = [pathlib.Path(p).rglob("reco1-000*.root") for p in self.stage0_path]
        # for dirt
        # path_generators = [self.stage0_path.rglob("stage0*mix-000*.root")]
        # generator = itertools.chain(*path_generators)
        # for run 2
        # path_generators = [self.stage0_path.rglob("stage0*.root")]

        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(
        self, iteration: int, rawdata_files: List[pathlib.Path], last_file=None
    ):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)

        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = self.get_run_dir(iteration)
        s.runfunc = self._caf_runfunc
        workflow.add_final_stage(s)

        for file in rawdata_files:
            for itr, i in enumerate(range(0, 50, 5)):
                s_stage1 = Stage(DefaultStageTypes.STAGE1)
                s_stage1.runfunc = self._runfunc_no_meta
                s_stage1.run_dir = self.get_run_dir(iteration) / f'{i:03d}'
                s.add_parents(s_stage1)

                s_pot = Stage(POT)
                s_stage1.add_parents(s_pot)
                s_pot.combine = True

                # file_itr = pathlib.PurePath(str(file).replace('mix-000', f'mix-{i:03d}'))
                file_itr = pathlib.PurePath(str(file).replace('reco1-000-', f'reco1-{i:03d}-'))
                parts = list(file_itr.parts)
                try:
                    subdir = self._file_map[parts[-1]]
                except KeyError:
                    print(f'Could not find {parts[-1]}, expect this job to fail.')
                    subdir = '000000'
                
                # print(file_itr)
                del parts[-4:-1]
                parts[-2] = subdir
                file_itr = pathlib.PurePath(*parts)
                # print(subdir)
                # print(file_itr)
                # sys.exit(0)
                s_pot.add_input_file(file_itr)

                # when running with no caf
                # workflow.add_final_stage(s_stage1)
            '''
            # for run 2
            s_stage1 = Stage(DefaultStageTypes.STAGE1)
            s_stage1.runfunc = self._runfunc_no_meta
            s.add_parents(s_stage1)
            s_stage1.add_input_file(file)
            '''

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, DataExecutor)
