"""
Components module which contains runfunc & helper functions for common workflows
New runfuncs can be build by passing different helpers ("components") to the runfunc
"""
import sys, os
import pathlib
import functools
import itertools
from typing import Dict, List

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbn_parsl.workflow import StageType, Stage, DefaultStageTypes

from sbn_parsl.utils import hash_name



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


def output_filepath(stage: Stage, first_file_name: str, fcl: pathlib.Path, label: str='', salt='', lar_opts={}):
    """Pick an output file name based on a stage and its input."""
    if stage.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        _label = label
        if label != '':
            _label = label + '-'

        output_filename = ''.join([
            str(stage.stage_type.name), '-', _label,
            hash_name(os.path.basename(fcl) + salt + stage.stage_id_str),
            ".root"
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.flat.caf.root'

    return pathlib.PurePosixPath(label, stage.stage_type.name, f'{stage.workflow_id // 1000:06d}', \
            f'{stage.workflow_id // 100:06d}', output_filename)



def build_larsoft_cmd(stage: Stage, fcl, inputs: List=None, output_file: pathlib.Path=pathlib.Path('sbn_parsl_out.root'), lar_args={}) -> str:
    """Build larsoft command with input & output flags"""
    # caf stage does not get an output argument
    output_file_arg_str = ''
    if stage.stage_type != DefaultStageTypes.CAF:
        output_file_arg_str = f'--output={str(output_file)}'

    input_file_arg_str = ''
    if inputs:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' if not isinstance(file, parsl.app.futures.DataFuture) else f'-s {str(file.filepath)}' for file in inputs])

    # stages after gen: don't limit number of events; use all events from all input files
    nevts = f' --nevts={lar_args["nevts"]}'
    nskip = ''
    try:
        nskip = f' --nskip={lar_args["nskip"]}'
    except KeyError:
        pass

    return f'lar -c {fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}'


def transfer_ids(stage: Stage, future):
    """add some custom member functions to track the parent workflow when these stages complete"""
    future.workflow_id = stage.workflow_id
    future.stage_id = stage.stage_id_str
    future.final = stage.final


def build_modify_fcl_cmd(stage: Stage, fcl):
    """generate bash commands that modify fcl"""
    fcl_cmd = ''
    fcl_name = os.path.basename(fcl)
    if stage.stage_type == DefaultStageTypes.GEN:
        run_number = 1 + (stage.workflow_id // 100)
        subrun_number = stage.workflow_id % 100
        fcl_cmd = '\n'.join([
            f'echo "source.firstRun: {run_number}" >> {fcl_name}',
            f'echo "source.firstSubRun: {subrun_number}" >> {fcl_name}',
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFiles/\\"" >> {fcl_name}''',
            f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\\" ]" >> {fcl_name}''',
            f'''echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {os.path.basename(fcl)}''',
        ])

    return fcl_cmd


def larsoft_runfunc(self, fcl, inputs, run_dir, template, meta, executor, label='', last_file=None, \
        output_filename_func=output_filepath, lar_cmd_func=build_larsoft_cmd, fcl_cmd_func=build_modify_fcl_cmd, \
        future_func=fcl_future, **kwargs):
    """
    Method bound to each Stage object and run during workflow execution. Use
    this runfunc for generating MC events.

    Supported features:
     - Metadata injection
     - Combined stages: Forward command to the next stage, so both stages run in 1 task
     - wait for last_file (even if no dependency)

    Default components may be overridden:
     - output_filename_func: Specify output filename based on stage
     - lar_cmd_func: Specify how lar command is built
     - fcl_cmd_func: Function that generates bash script to override fcl params
     - future func: Function that typically submits a parsl future

    Extra kwargs will override larsoft options
    """

    run_dir.mkdir(parents=True, exist_ok=True)
    lar_opts = executor.larsoft_opts.copy()
    lar_opts.update(kwargs)

    # first stage for file workflows will have a string or path as input
    # put it in a general form that can be passed to the next stage
    if not isinstance(inputs, list):
        first_arg = []
        if inputs is not None:
            first_arg = [inputs]
        inputs = [first_arg, [], '']
    else:
        if len(inputs) == 1:
            inputs = [inputs, [], '']

    input_files = list(itertools.chain.from_iterable(inputs[0::3]))
    # if we pass in pathlib Paths, parsl will complain that it can't memoize
    # them, so they need to be strings or datafutures
    input_files = [str(f) if not isinstance(f, parsl.app.futures.DataFuture) else f for f in input_files]

    # this list of inputs will be passed to user components so users don't need
    # to interact with parsl objects
    input_files_user = [f.filename if isinstance(f, parsl.app.futures.DataFuture) else f for f in input_files]

    depends = list(itertools.chain.from_iterable(inputs[1::3]))
    parent_cmd = '&&'.join(pc for pc in inputs[2::3] if pc != '')

    first_file_name = ''
    if self.stage_type != DefaultStageTypes.GEN:
        if not isinstance(inputs[0][0], parsl.app.futures.DataFuture):
            if not isinstance(inputs[0][0], pathlib.Path):
                inputs[0][0] = pathlib.Path(inputs[0][0])
            first_file_name = inputs[0][0].name
        else:
            first_file_name = inputs[0][0].filename

    output_file = executor.output_dir / output_filename_func(self, first_file_name, fcl, label, executor.name_salt, lar_opts)

    if executor.stage_in_db(self.stage_id_str):
        executor._skip_counter += 1
        return [[output_file], [], '']

    if output_file.is_file():
        executor._skip_counter += 1
        return [[output_file], [], '']

    executor._stage_counter += 1
    output_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = ' && '.join([
        f'mkdir -p {run_dir} && cd {run_dir}',
        lar_cmd_func(self, fcl, input_files_user, output_file, lar_opts)
    ])

    if parent_cmd:
        cmd = ' && '.join([parent_cmd, cmd])

    dummy_input = None
    if last_file is not None:
        dummy_input = last_file[0][0]
    input_arg = [str(fcl), dummy_input] + input_files + depends

    if self.combine:
        # don't submit work, just forward commands to the next task
        return [[output_file], input_files, cmd]

    # metadata & fcl manipulation
    mg_cmd = '\n'.join([
        fcl_cmd_func(self, fcl),
        meta.run_cmd(output_file.name + '.json', os.path.basename(fcl), check_exists=False)
    ])

    future = future_func(
        workdir = str(run_dir),
        stdout = str(run_dir / output_file.name.replace(".root", ".out")),
        stderr = str(run_dir / output_file.name.replace(".root", ".err")),
        template = template,
        cmd = cmd,
        larsoft_opts = executor.larsoft_opts,
        inputs = input_arg,
        outputs = [File(str(output_file))],
        pre_job_hook = mg_cmd
    )

    transfer_ids(self, future.outputs[0])

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return [future.outputs, [], '']



# -------
#
# RUNFUNCS
# 
# -------


mc_runfunc_sbnd=functools.partial(larsoft_runfunc)

def build_larsoft_cmd_sbnd_data(stage: Stage, fcl, inputs: List=None, output_file: pathlib.Path=pathlib.Path('sbn_parsl_out.root'), lar_args={}) -> str:
    """Build larsoft command with input & output flags"""
    # caf stage does not get an output argument
    output_file_arg_str = ''
    if stage.stage_type != DefaultStageTypes.CAF:
        output_file_arg_str = f'--output={str(output_file)}'

    # specify output stream for SBND decode stage
    if stage.stage_type == DefaultStageTypes.DECODE:
        output_file_arg_str = f'--output=out1:{str(output_file)}'

    input_file_arg_str = ''
    if inputs:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' if not isinstance(file, parsl.app.futures.DataFuture) else f'-s {str(file.filepath)}' for file in inputs])

    # stages after gen: don't limit number of events; use all events from all input files
    if stage.stage_type != DefaultStageTypes.GEN:
        nevts = f' --nevts=-1'
    else:
        nevts = f' --nevts={lar_args["nevts"]}'

    nskip = ''
    try:
        nskip = f' --nskip={lar_args["nskip"]}'
    except KeyError:
        pass

    return f'lar -c {fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}'

def output_filepath_sbnd_data(stage: Stage, first_file_name: str, fcl: pathlib.Path, label: str='', salt='', lar_opts={}):
    """Pick an output file name based on input filename."""
    if stage.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        output_filename = '-'.join([
            str(stage.stage_type.name), os.path.basename(first_file_name),
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.flat.caf.root'

    return pathlib.PurePosixPath(label, stage.stage_type.name, f'{stage.workflow_id // 1000:06d}', \
            f'{stage.workflow_id // 100:06d}', output_filename)

data_runfunc_sbnd=functools.partial(larsoft_runfunc, lar_cmd_func=build_larsoft_cmd_sbnd_data, output_filename_func=output_filepath_sbnd_data)


# icarus: different caf name
def output_filepath_icarus_data(stage: Stage, first_file_name: str, fcl: pathlib.Path, label: str='', salt='', lar_opts={}):
    """Pick an output file name based on input filename."""
    nskip = 0
    try:
        nskip = lar_opts['nskip']
    except KeyError:
        pass
    if stage.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        output_filename = '-'.join([
            str(stage.stage_type.name), f'{nskip:03d}', os.path.basename(first_file_name),
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.Blind.OKTOLOOK.flat.caf.root'

    return pathlib.PurePosixPath(label, stage.stage_type.name, f'{stage.workflow_id // 1000:06d}', \
            f'{stage.workflow_id // 100:06d}', output_filename)

def build_modify_fcl_cmd_icarus(stage: Stage, fcl):
    """generate bash commands that modify fcl"""
    fcl_cmd = ''
    fcl_name = os.path.basename(fcl)
    if stage.stage_type.name == 'overlay':
        run_number = 1 + (stage.workflow_id // 100)
        subrun_number = stage.workflow_id % 100
        fcl_cmd = '\n'.join([
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFilesIcarus/\\"" >> {os.path.basename(fcl)}''',
            f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\\" ]" >> {os.path.basename(fcl)}''',
            f'''echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {os.path.basename(fcl)}''',
        ])

    return fcl_cmd

mc_runfunc_icarus=functools.partial(larsoft_runfunc, fcl_cmd_func=build_modify_fcl_cmd_icarus)
data_runfunc_icarus=functools.partial(larsoft_runfunc, output_filename_func=output_filepath_icarus_data)
