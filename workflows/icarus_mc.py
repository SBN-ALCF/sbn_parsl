#!/usr/bin/env python

# This workflow runs the decoder on raw data files

import sys, os
import re
import json
import time
import pathlib
import functools
import itertools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbnd_parsl.workflow import StageType, Stage, Workflow, WorkflowExecutor, \
    DefaultStageTypes
from sbnd_parsl.metadata import MetadataGenerator
from sbnd_parsl.templates import CMD_TEMPLATE_SPACK, CMD_TEMPLATE_CONTAINER
from sbnd_parsl.utils import create_default_useropts, create_parsl_config, \
    hash_name
from sbnd_parsl.dfk_hacks import apply_hacks


OVERLAY = StageType('overlay')
OVERLAY_WFM = StageType('overlay_wfm')


@bash_app(cache=True)
def fcl_future(workdir, stdout, stderr, template, cmd, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook=''):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1],
        cmd=cmd,
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


def runfunc(self, fcl, inputs, run_dir, executor, last_file=None, nevts=-1, nskip=0):
    """Method bound to each Stage object and run during workflow execution."""

    run_dir.mkdir(parents=True, exist_ok=True)

    # unpack the "inputs" arg. Should be: [[list of input files], [list of dependencies], parent_cmd]
    # the last two elements are specifically to allow combining stages
    # careful: if the stage has multiple input files, must chain together all of the args
    if self.stage_type == DefaultStageTypes.DECODE:
        inputs = [inputs, [], '']

    input_files = list(itertools.chain.from_iterable(inputs[0::3]))
    depends = list(itertools.chain.from_iterable(inputs[1::3]))
    parent_cmd = '&&'.join(pc for pc in inputs[2::3] if pc != '')

    first_file_name = ''
    if self.stage_type == DefaultStageTypes.DECODE:
        # decode stage takes a string filename
        first_file_name = input_files[0]
    else:
        if not isinstance(inputs[0][0], parsl.app.futures.DataFuture):
            first_file_name = inputs[0][0].name
        else:
            # pathlib path
            first_file_name = inputs[0][0].filename

    if self.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        if self.stage_type == DefaultStageTypes.DECODE:
            # decode stage: separate based on nskip parameter
            parts = [
                str(self.stage_type.name), f'{nskip:03d}', os.path.basename(first_file_name), 
            ]
        else:
            # other stages will build on nskip parameter; no need to add to file name
            parts = [
                str(self.stage_type.name), os.path.basename(first_file_name), 
            ]
        output_filename = '-'.join(parts)
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.flat.caf.root'

    output_dir = executor.output_dir / self.stage_type.name / f'{self.workflow_id // 1000:06d}' / f'{self.workflow_id // 100:06d}'

    if self.combine:
        # if we are combining, save this stage's result only on node-local disk
        # output_dir.name gets the instance number for this task
        # run_dir = pathlib.Path('/local/scratch') / run_dir.name
        # output_dir = run_dir
        output_dir.mkdir(parents=True, exist_ok=True)
    else:
        # save this result to filesystem (eagle). Make sure it exists
        output_dir.mkdir(parents=True, exist_ok=True)


    output_file = output_dir / output_filename
    output_file_arg_str = ''

    if self.stage_type != DefaultStageTypes.CAF:
        output_file_arg_str = f'--output {str(output_file)}'

    if executor.stage_in_db(self.stage_id_str):
        executor._skip_counter += 1
        return [[output_file], [], '']

    if output_file.is_file():
        executor._skip_counter += 1
        return [[output_file], [], '']

    # output_filepath = output_dir / output_filename

    input_file_arg_str = ''
    dummy_input = None
    if last_file is not None:
        dummy_input = last_file[0][0]
    input_arg = [str(fcl), dummy_input]

    if inputs is not None:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' if not isinstance(file, parsl.app.futures.DataFuture) else f'-s {str(file.filepath)}' for file in input_files])
        input_arg += [str(f) if not isinstance(f, parsl.app.futures.DataFuture) else f for f in input_files] + depends

    cmd = f'mkdir -p {run_dir} && cd {run_dir} && lar -c {fcl} {input_file_arg_str} {output_file_arg_str} --nevts={nevts} --nskip={nskip}'

    # sort the analyzer module files into directories for easier transfer to FNAL later
    if self.stage_type == DefaultStageTypes.STAGE0:
        purity_dir = executor.output_dir / 'purity' / f'{self.workflow_id // 1000:06d}' / f'{self.workflow_id // 100:06d}'
        purity_dir.mkdir(parents=True, exist_ok=True)
        purity_filename = purity_dir / f"hist_{output_filename}"
        cmd += f' -T {str(purity_dir / purity_filename)}'


    if parent_cmd != '':
        cmd = ' && '.join([parent_cmd, cmd])


    if self.combine:
        # don't submit work, just forward commands to the next task
        # we need to forward this task's dependencies too
        return [[output_file], input_files, cmd]

    mg_cmd = ''

    # fcl overrides for supera need to be processed in pre_job_hook
    if self.stage_type == DefaultStageTypes.STAGE1:
        calib_dir = executor.output_dir / 'calib_ntuple' / f'{self.workflow_id // 1000:06d}' / f'{self.workflow_id // 100:06d}'
        larcv_dir = executor.output_dir / 'larcv' / f'{self.workflow_id // 1000:06d}' / f'{self.workflow_id // 100:06d}'
        calib_dir.mkdir(parents=True, exist_ok=True)
        larcv_dir.mkdir(parents=True, exist_ok=True)
        calib_filename = calib_dir / f"hist_{output_filename}"
        larcv_filename = larcv_dir / f"larcv_{output_filename}"
        cmd += f' -T {str(calib_dir / calib_filename)}'
        mg_cmd = '\n'.join([mg_cmd,
            f'''echo "physics.analyzers.superaMC.out_filename: \\"{str(larcv_filename)}\\"" >> {os.path.basename(fcl)}''',
            f'''echo "physics.analyzers.superaMC.unique_filename: false" >> {os.path.basename(fcl)}''',
        ])

    if self.stage_type == OVERLAY:
        mg_cmd = '\n'.join([mg_cmd,
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFilesIcarus/\\"" >> {os.path.basename(fcl)}''',
            f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\\" ]" >> {os.path.basename(fcl)}''',
            f'''echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {os.path.basename(fcl)}''',
        ])
        mg_cmd += '\n'

    mg_cmd = '\n'.join([mg_cmd, executor.meta.run_cmd(
        output_filename + '.json', os.path.basename(fcl), check_exists=False)])

    future = fcl_future(
        workdir = str(run_dir),
        stdout = str(run_dir / output_filename.replace(".root", ".out")),
        stderr = str(run_dir / output_filename.replace(".root", ".err")),
        template = CMD_TEMPLATE_CONTAINER,
        cmd=cmd,
        larsoft_opts = executor.larsoft_opts,
        inputs = input_arg,
        outputs = [File(str(output_file))],
        pre_job_hook = mg_cmd
    )

    # add some custom member functions to track the parent workflow when these
    # stages complete
    future.outputs[0].workflow_id = self.workflow_id
    future.outputs[0].stage_id = self.stage_id_str
    future.outputs[0].final = self.final

    # this modifies the list passed in by WorkflowExecutor so it can track progress
    executor.futures.append(future.outputs[0])

    return [future.outputs, [], '']


class DecoderExecutor(WorkflowExecutor):
    """Execute a decoder workflow from user settings."""
    def __init__(self, settings: json):
        super().__init__(settings)

        self.meta = MetadataGenerator(settings['metadata'], self.fcls, defer_check=True)
        self.stage_order = [
            DefaultStageTypes.DECODE,
            OVERLAY,
            DefaultStageTypes.G4,
            DefaultStageTypes.DETSIM,
            OVERLAY_WFM,
            DefaultStageTypes.STAGE0,
            DefaultStageTypes.STAGE1,
            DefaultStageTypes.CAF,
        ]
        # without this, workflow tries to make DefaultStageOrders out of
        # strings, but this fails since we added some custom ones
        self.fcls = { so: settings['fcls'][so.name] for so in self.stage_order }

        self.files_per_subrun = settings['run']['files_per_subrun']
        self.run_list = None
        if 'run_list' in settings['workflow']:
            with open(settings['workflow']['run_list'], 'r') as f:
                self.run_list = [int(l.strip()) for l in f.readlines()]

        self.rawdata_path = pathlib.Path(settings['workflow']['rawdata_path'])

    def file_generator(self):
        path_generators = [self.rawdata_path.rglob('*.root')]
        generator = itertools.chain(*path_generators)
        for f in generator:
            yield f

    def setup_single_workflow(self, iteration: int, rawdata_files: List[pathlib.Path], last_file=None):
        if not rawdata_files:
            raise RuntimeError()

        workflow = Workflow(self.stage_order, default_fcls=self.fcls)
        runfunc_ = functools.partial(runfunc, executor=self, last_file=last_file)
        s = Stage(DefaultStageTypes.CAF)
        s.run_dir = get_subrun_dir(self.output_dir, iteration)
        s.runfunc = runfunc_
        workflow.add_final_stage(s)

        decode_runfuncs = [functools.partial(runfunc, executor=self, nevts=1, nskip=(i * 5)) for i in range(10)]

        for i, file in enumerate(rawdata_files):
            for j in range(10):
                s_stage1 = Stage(DefaultStageTypes.STAGE1)
                s_stage0 = Stage(DefaultStageTypes.STAGE0)
                s_overlay_wfm = Stage(OVERLAY_WFM)
                s_detsim = Stage(DefaultStageTypes.DETSIM)
                s_g4 = Stage(DefaultStageTypes.G4)
                s_overlay = Stage(OVERLAY)
                s_decode = Stage(DefaultStageTypes.DECODE, runfunc=decode_runfuncs[j])

                s_stage1.run_dir = get_subrun_dir(self.output_dir, iteration * self.files_per_subrun + i) / f'{j:03d}'

                s.add_parents(s_stage1, workflow.default_fcls)
                s_stage1.add_parents(s_stage0, workflow.default_fcls)
                s_stage0.add_parents(s_overlay_wfm, workflow.default_fcls)
                s_overlay_wfm.add_parents(s_detsim, workflow.default_fcls)
                s_detsim.add_parents(s_g4, workflow.default_fcls)
                s_g4.add_parents(s_overlay, workflow.default_fcls)
                s_overlay.add_parents(s_decode, workflow.default_fcls)

                s_decode.add_input_file(str(file))
                s_decode.combine = True
                s_detsim.combine = True

        return workflow


def get_subrun_dir(prefix: pathlib.Path, subrun: int):
    """Returns a path with directory structure like XXXX00/XXXXXX"""
    return prefix / f"{100*(subrun//100):06d}" / f"subrun_{subrun:06d}"


def main(settings):
    # parsl
    user_opts = create_default_useropts()
    user_opts['run_dir'] = str(pathlib.Path(settings['run']['output']) / 'runinfo')
    user_opts.update(settings['queue'])
    # parsl_config = create_parsl_config(user_opts, [settings['larsoft']['spack_top'], settings['larsoft']['version'], settings['larsoft']['software']])
    parsl_config = create_parsl_config(user_opts)
    print(parsl_config)
    parsl.clear()

    with parsl.load(parsl_config) as dfk:
        apply_hacks(dfk)
        wfe = DecoderExecutor(settings)
        wfe.execute()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Please provide a json configuration file")
        sys.exit(1)

    with open(sys.argv[1], 'r') as f:
        settings = json.load(f)

    main(settings)
