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
from sbn_parsl.experiments.icarus import overlay_runfunc_icarus, build_modify_fcl_cmd_icarus
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.app import entry_point
from sbn_parsl.config import Config



def build_larsoft_cmd_caf(context: RunContext, sbnnusyst_env: str, sbnnusyst_fcl: str) -> str:
    """
    For CAF stage, run sbnnusyst
    """
    if context.stage.stage_type != DefaultStageTypes.CAF:
        raise RuntimeError('Tried to use larsoft cmd component with non-CAF stage')

    lar_cmd = build_larsoft_cmd(context)

    nusyst_cmd = '\n'.join([
        f'source {sbnnusyst_env}',
        f'echo "caf" > list.tmp',
        f'UpdateReweight -c {sbnnusyst_fcl} -i list.tmp -o {context.output_file.name}',
    ])
    return f'{lar_cmd}\n{nusyst_cmd}'



class OverlayExecutor(LArSoftExecutor):
    """Execute a decoder workflow from user settings."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)

        self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)
        self.stage_order = [
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

        self.stage0_path = pathlib.Path(wf_dict.get("stage0_path", "."))

        caf_lar_cmd = functools.partial(build_larsoft_cmd_caf,
                                        sbnnusyst_env=wf_dict['sbnnusyst_env'],
                                        sbnnusyst_fcl=wf_dict['sbnnusyst_fcl'])

        caf_runfunc = functools.partial(
            larsoft_runfunc,
            lar_cmd_func=caf_lar_cmd,
            output_filename_func=functools.partial(
                output_filepath_generic, is_mc=True, use_label=False, include_skip=True
                ),
            fcl_cmd_func=build_modify_fcl_cmd_icarus,
        )
        self._caf_runfunc = functools.partial(caf_runfunc, executor=self,
                                              template=CMD_TEMPLATE_CONTAINER,
                                              meta=self.meta)

        self._runfunc = functools.partial(
            overlay_runfunc_icarus,
            template=CMD_TEMPLATE_CONTAINER,
            meta=self.meta,
            executor=self,
            last_file=None,
        )
        self._runfunc_no_meta = functools.partial(
            overlay_runfunc_icarus,
            template=CMD_TEMPLATE_CONTAINER,
            meta=None,
            executor=self,
            last_file=None,
            nevts=1
        )


    def file_generator(self):
        # for CV
        path_generators = [self.stage0_path.rglob("stage0*overlay-000*.root")]
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
            # for run4, dirt
            for i in range(0, 50, 10):
                s_stage1 = Stage(DefaultStageTypes.STAGE1)
                s_stage1.runfunc = self._runfunc_no_meta
                s_stage1.run_dir = self.get_run_dir(iteration) / f'{i:03d}'
                s.add_parents(s_stage1)
                # for dirt
                # file_itr = pathlib.PurePath(str(file).replace('mix-000', f'mix-{i:03d}'))
                file_itr = pathlib.PurePath(str(file).replace('overlay-000-', f'overlay-{i:03d}-'))
                s_stage1.add_input_file(file_itr)
            '''
            # for run 2
            s_stage1 = Stage(DefaultStageTypes.STAGE1)
            s_stage1.runfunc = self._runfunc_no_meta
            s.add_parents(s_stage1)
            s_stage1.add_input_file(file)
            '''

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, OverlayExecutor)
