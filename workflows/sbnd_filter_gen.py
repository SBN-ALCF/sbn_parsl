#!/usr/bin/env python

# This workflow generates full MC events from generator through CAF stage

import sys
import os
import copy
import json
import pathlib
import functools
import itertools
from typing import Dict, List

from sbn_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
        DefaultStageTypes
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import mc_runfunc_sbnd, larsoft_runfunc, \
        build_larsoft_cmd_drop_reco2, build_modify_fcl_cmd_sbnd_mc, RunContext, \
        output_filepath_sbnd_mc
from sbn_parsl.app import entry_point
from sbn_parsl.metadata import MetadataGenerator

FILTER = StageType('filter')


def output_filepath_sbnd_filter(context: RunContext) -> pathlib.Path:
    """
    Use input file name in output file names.
    """
    if context.stage.stage_type.name != 'filter':
        raise RuntimeError('Tried to use filter output component with non-filter stage')

    output_filename = pathlib.Path(context.stage.stage_type.name + '-' \
            + os.path.splitext(context.input_files[0].name)[0] + '.root')

    return context.out_dir / pathlib.Path(context.label, context.stage.stage_type.name, \
            f'{context.stage.workflow_id // 1000:06d}', \
            f'{context.stage.workflow_id // 100:06d}', output_filename)


def build_larsoft_cmd_filter(context: RunContext, streams) -> str:
    """
    For filter stage, send each stream to its own file
    """
    if context.stage.stage_type.name != 'filter':
        raise RuntimeError('Tried to use larsoft cmd component with non-filter stage')

    output_file_arg_str = ' '.join(f'--output=out{stream}:filter-{context.input_files[0].stem}_{stream}.root' for stream in streams)

    input_file_arg_str = ''
    if context.input_files:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' for file in context.input_files])

    # stages after gen: don't limit number of events; use all events from all input files
    nevts = f' --nevts=-1'
    nskip = ''
    try:
        nskip = f' --nskip={context.lar_args["nskip"]}'
    except KeyError:
        pass

    # touch: make a real file with no stream for the data future, even though downstream stages won't use it
    rm_cmd = 'rm -f ' + ' '.join(f'{str(f)}' for f in context.input_files)
    return f'lar -c {context.fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip} --tmpdir=/tmp && touch {context.output_file} && {rm_cmd}'


def build_larsoft_g4_filtered(context: RunContext, stream) -> str:
    """
    Override G4 runfunc to read in output file with stream suffix
    """
    if context.stage.stage_type.name != 'g4':
        raise RuntimeError('Tried to use larsoft cmd component with non-g4 stage')

    new_context = copy.copy(context)
    new_context.input_files = [str(f).replace('.root', f'_{stream}.root') for f in context.input_files]
    return build_larsoft_cmd_drop_reco2(new_context)

def output_filepath_sbnd_filter_g4(context: RunContext, stream) -> pathlib.Path:
    """
    Use input file name in output file names.
    """
    if context.stage.stage_type.name != 'g4':
        raise RuntimeError('Tried to use filter output component with non-filter stage')

    output_filename = pathlib.Path(context.stage.stage_type.name + '-' \
            + os.path.splitext(context.input_files[0].name)[0] + f'_{stream}.root')

    return context.out_dir / pathlib.Path(context.label, context.stage.stage_type.name, \
            f'{context.stage.workflow_id // 1000:06d}', \
            f'{context.stage.workflow_id // 100:06d}', output_filename)


class CAFFromGenExecutor(WorkflowExecutor):
    """Execute a Gen -> G4 -> Detsim -> Reco1 -> Reco2 workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)
        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [
                DefaultStageTypes.GEN,
                FILTER,
                DefaultStageTypes.G4,
                DefaultStageTypes.DETSIM,
                DefaultStageTypes.RECO1,
                DefaultStageTypes.RECO2,
                DefaultStageTypes.CAF,
        ]
        self.fcls = { so: settings['fcls'][so.name] for so in self.stage_order }
        self.streams = settings['workflow']['streams']
        self.gen_path = pathlib.Path(settings['workflow']['gen_path'])
        self.files_per_filter = settings['workflow']['files_per_filter']

        filter_lar_cmd = functools.partial(build_larsoft_cmd_filter, streams=self.streams)
        filter_runfunc = functools.partial(larsoft_runfunc,
                                          output_filename_func=output_filepath_sbnd_filter,
                                          lar_cmd_func=filter_lar_cmd,
                                          fcl_cmd_func=build_modify_fcl_cmd_sbnd_mc)

        self.filter_runfunc = functools.partial(filter_runfunc, executor=self, 
                                         template=CMD_TEMPLATE_CONTAINER,
                                         meta=None)

        self.g4_runfuncs = {stream: functools.partial(larsoft_runfunc,
                                                      output_filename_func=functools.partial(output_filepath_sbnd_filter_g4, stream=stream),
                                                      lar_cmd_func=functools.partial(build_larsoft_g4_filtered, stream=stream),
                                                      fcl_cmd_func=build_modify_fcl_cmd_sbnd_mc,
                                                      executor=self,
                                                      template=CMD_TEMPLATE_CONTAINER, meta=None)
                            for stream in self.streams}
        
        self.runfuncs = {stream: functools.partial(mc_runfunc_sbnd, executor=self, 
                                         template=CMD_TEMPLATE_CONTAINER,
                                         label=stream,
                                         meta=self.meta)
                         for stream in self.streams}

        self.gen_runfunc = functools.partial(mc_runfunc_sbnd, executor=self, 
                                         template=CMD_TEMPLATE_CONTAINER,
                                         meta=None)

        # updates as we call setup_single_workflow, accessible by subsequent workflows
        self._nodes_per_block = settings['queue']['nodes_per_block']
        self._current_filter_stages = [None] * self._nodes_per_block
        print(f'blocking at {self._nodes_per_block} workflows')

    def file_generator(self):
        """Use when gen stage is input file"""
        path_generators = [self.gen_path.rglob('gen*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f


    def _setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        """ No gen stage (use input files) """
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
    
        s_filter = Stage(FILTER)
        s_filter.runfunc = self.filter_runfunc
        s_filter.run_dir = get_subrun_dir(self.output_dir, iteration)

        for stream in self.streams:
            s = Stage(DefaultStageTypes.CAF)
            s.runfunc = self.runfuncs[stream]
            output_dir = self.output_dir / stream
            s.run_dir = get_caf_dir(output_dir, iteration)

            s_reco2 = Stage(DefaultStageTypes.RECO2)
            s_reco2.run_dir = get_subrun_dir(output_dir, iteration)

            s_reco1 = Stage(DefaultStageTypes.RECO1)
            s_detsim = Stage(DefaultStageTypes.DETSIM)
            s_g4 = Stage(DefaultStageTypes.G4)
            s.add_parents(s_reco2, workflow.default_fcls)
            s_reco2.add_parents(s_reco1, workflow.default_fcls)
            s_reco1.add_parents(s_detsim, workflow.default_fcls)
            s_detsim.add_parents(s_g4, workflow.default_fcls)
            s_g4.add_parents(s_filter, workflow.default_fcls)
            s_g4.runfunc = self.g4_runfuncs[stream]

            s_g4.combine = True
            s_reco2.combine = True

            workflow.add_final_stage(s)

        for f in file_slice:
            s_filter.add_input_file(f)

        return workflow


    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        """ workflow also including gen """
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
    
        s_filter = Stage(FILTER)
        s_filter.runfunc = self.filter_runfunc
        s_filter.run_dir = get_subrun_dir(self.output_dir, iteration)

        for stream in self.streams:
            s = Stage(DefaultStageTypes.CAF)
            s.runfunc = self.runfuncs[stream]
            output_dir = self.output_dir / stream
            s.run_dir = get_caf_dir(output_dir, iteration)

            s_reco2 = Stage(DefaultStageTypes.RECO2)
            s_reco2.run_dir = get_subrun_dir(output_dir, iteration)

            s_reco1 = Stage(DefaultStageTypes.RECO1)
            s_detsim = Stage(DefaultStageTypes.DETSIM)
            s_g4 = Stage(DefaultStageTypes.G4)
            s.add_parents(s_reco2, workflow.default_fcls)
            s_reco2.add_parents(s_reco1, workflow.default_fcls)
            s_reco1.add_parents(s_detsim, workflow.default_fcls)
            s_detsim.add_parents(s_g4, workflow.default_fcls)
            s_g4.add_parents(s_filter, workflow.default_fcls)
            s_g4.runfunc = self.g4_runfuncs[stream]

            s_g4.combine = True
            s_reco2.combine = True

            workflow.add_final_stage(s)
        '''
        for f in file_slice:
            s_filter.add_input_file(f)
        '''
        # get previous filter stage and replace it with this one for the next workflow
        dependency = self._current_filter_stages[iteration % self._nodes_per_block]

        for i in range(self.files_per_filter):
            s_gen = Stage(DefaultStageTypes.GEN)
            s_gen.dependency = dependency
            s_gen.runfunc = self.gen_runfunc
            s_gen.run_dir = get_subrun_dir(self.output_dir, iteration) / f'{i:03d}'
            s_filter.add_parents(s_gen, workflow.default_fcls)

        self._current_filter_stages[iteration % self._nodes_per_block] = s_filter

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / f"{(subrun//100):06d}" / f"subrun_{subrun:06d}"

def get_caf_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/caf/XXXXXX"""
    return prefix / f"{(subrun//1000):06d}" / 'caf' / f"subrun_{subrun:06d}"

if __name__ == '__main__':
    entry_point(sys.argv, CAFFromGenExecutor)
