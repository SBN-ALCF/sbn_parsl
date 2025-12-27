"""
Components module which contains runfunc & helper functions for common workflows
New runfuncs can be build by passing different helpers, here: _components_, to the runfunc
Each component takes a "RunContext" object as an argument that contains
information about the stage and user settings,
"""
import sys, os
import pathlib
import functools
import itertools
from typing import Dict, List
from dataclasses import dataclass, field

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbn_parsl.workflow import StageType, Stage, DefaultStageTypes, WorkflowExecutor
from sbn_parsl.metadata import MetadataGenerator

from sbn_parsl.utils import hash_name


@bash_app(cache=False)
def fcl_future(workdir, stdout, stderr, template, cmd, larsoft_opts, inputs=[], outputs=[], pre_job_hook='', post_job_hook='', parsl_resource_specification={}):
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


@dataclass
class RunContext:
    """
    Context object passed to different components during runfunc.
    Output file is set during the runfunc but may be used by other components
    For this reason it doesn't need to be initialized at context creation, but
    must be set before use later
    LAr args are executor larsoft options after any stage overrides
    """
    stage: Stage
    input_files: List[pathlib.Path] = field(default_factory=list)
    out_dir: pathlib.Path = pathlib.Path("")
    fcl: pathlib.Path = None
    lar_args: Dict = None
    salt: str = ''
    label: str = ''
    output_file: pathlib.Path = pathlib.Path("")
    meta: MetadataGenerator = None

    """
    @property
    def output_file(self) -> pathlib.Path:
        if not self._output_file_is_set:
            raise RuntimeError("Error: Component tried to access output file before it was set during stage execution.")
        return self._output_file

    @output_file.setter
    def output_file(self, v: pathlib.Path):
        self._output_file_is_set = True
        self._output_file = v
    """
    

def output_filepath(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on a stage and its input."""
    if context.stage.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        _label = context.label
        if context.label != '':
            _label = context.label + '-'

        output_filename = ''.join([
            str(context.stage.stage_type.name), '-', _label,
            hash_name(context.fcl.name + context.salt + context.stage.stage_id_str),
            ".root"
        ])
    else:
        output_filename = os.path.splitext(context.input_files[0].name)[0] + '.flat.caf.root'

    return context.out_dir / pathlib.Path(context.label, context.stage.stage_type.name, \
            f'{context.stage.workflow_id // 1000:06d}', \
            f'{context.stage.workflow_id // 100:06d}', output_filename)



def build_larsoft_cmd(context: RunContext) -> str:
    """Build larsoft command with input & output flags"""
    # caf stage does not get an output argument
    output_file_arg_str = ''
    if context.stage.stage_type != DefaultStageTypes.CAF:
        output_file_arg_str = f'--output={str(context.output_file)}'

    print(context.input_files)
    input_file_arg_str = ''
    if context.input_files:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' for file in context.input_files])

    # stages after gen: don't limit number of events; use all events from all input files
    nevts = f' --nevts=-1'
    if context.stage.stage_type == DefaultStageTypes.GEN:
        nevts = f' --nevts={context.lar_args["nevts"]}'

    nskip = ''
    try:
        nskip = f' --nskip={context.lar_args["nskip"]}'
    except KeyError:
        pass

    return f'lar -c {context.fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}'


def _transfer_ids(stage: Stage, future):
    """add some custom member functions to track the parent workflow when these stages complete"""
    future.workflow_id = stage.workflow_id
    future.stage_id = stage.stage_id_str
    future.final = stage.final


def build_modify_fcl_cmd(context: RunContext) -> str:
    """generate bash commands that modify fcl"""
    fcl_cmd = ''
    fcl_name = context.fcl.name
    if context.stage.stage_type == DefaultStageTypes.GEN:
        run_number = 1 + (context.stage.workflow_id // 100)
        subrun_number = context.stage.workflow_id % 100
        fcl_cmd = '\n'.join([
            f'echo "" >> {fcl_name}',
            f'echo "source.firstRun: {run_number}" >> {fcl_name}',
            f'echo "source.firstSubRun: {subrun_number}" >> {fcl_name}',
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFiles/\\"" >> {fcl_name}''',
            f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\\" ]" >> {fcl_name}''',
            f'''echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {fcl_name}''',
        ])

    return fcl_cmd


def larsoft_runfunc(self, fcl, inputs, run_dir, template, executor, meta=None, label='', last_file=None, \
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

    lar_opts = executor.larsoft_opts.copy()
    lar_opts.update(kwargs)

    # first stage for file workflows will have a string or path as input
    # put it in a general form that can be passed to the next stage
    if not isinstance(inputs, list):
        inputs = [[inputs], [], '']
    else:
        if len(inputs) == 1:
            inputs = [inputs, [], '']


    input_files = list(itertools.chain.from_iterable(inputs[0::3]))
    # if we pass in pathlib Paths, parsl will complain that it can't memoize
    # them, so they need to be strings or datafutures
    input_files = [str(f) if isinstance(f, pathlib.Path) else f for f in input_files]

    depends = list(itertools.chain.from_iterable(inputs[1::3]))
    parent_cmd = '\n'.join(pc for pc in inputs[2::3] if pc != '')

    # create a context object for our components
    # convert datafutures so user-code doesn't have to handle Parsl types
    # combined stages: Write output files to /tmp on Aurora instead of flare
    # slightly better IO & avoids keeping them on disk
    # TODO this mkdir must be run on the worker!
    context = RunContext(
        stage=self,
        input_files=[pathlib.PurePosixPath(f.filename) \
                if isinstance(f, parsl.app.futures.DataFuture) \
                else pathlib.PurePosixPath(f) for f in input_files if f is not None],
        out_dir = executor.output_dir if not self.combine else pathlib.Path('/tmp'),
        fcl=pathlib.PurePosixPath(fcl),
        label=label,
        lar_args=lar_opts,
        salt=executor.name_salt,
        meta=meta,
    )

    first_file_name = ''
    if self.stage_type != DefaultStageTypes.GEN:
        if not isinstance(inputs[0][0], parsl.app.futures.DataFuture):
            if not isinstance(inputs[0][0], pathlib.Path):
                inputs[0][0] = pathlib.PurePosixPath(inputs[0][0])
            first_file_name = inputs[0][0].name
        else:
            first_file_name = inputs[0][0].filename

    # output_file = executor.output_dir / output_filename_func(self, first_file_name, fcl, label, executor.name_salt, lar_opts)
    context.output_file = output_filename_func(context)

    if executor.stage_in_db(self.stage_id_str):
        executor._skip_counter += 1
        return [[context.output_file], [], '']

    if context.output_file.is_file():
        executor._skip_counter += 1
        return [[context.output_file], [], '']

    executor._stage_counter += 1

    # clean any input files that are in /tmp after this stage completes
    rm_cmd = '\n'.join([f'rm {f}' for f in context.input_files if str(f).startswith('/tmp/')])

    cmd = '\n'.join([
        f'mkdir -p {run_dir}',
        f'mkdir -p {context.output_file.parent}',
        f'cd {run_dir}',
        lar_cmd_func(context),
        rm_cmd
    ])
    # lar_cmd_func(self, fcl, input_files_user, output_file, lar_opts)

    if parent_cmd:
        cmd = '\n'.join([parent_cmd, cmd])

    dummy_input = []
    if last_file is not None:
        # dummy input is a list containing a parsl datafuture
        dummy_input = last_file[0]
    input_arg = [str(fcl)] + dummy_input + input_files + depends

    # metadata & fcl manipulation
    mg_cmd = fcl_cmd_func(context)
    if meta is not None:
        mg_cmd += '\n' + \
                meta.run_cmd(context.output_file.name + '.json', os.path.basename(fcl), check_exists=False)

    if self.combine:
        # don't submit work, just forward commands to the next task
        return [[context.output_file], dummy_input + input_files + depends, mg_cmd + '\n' + cmd]

    # this tells Parsl to prioritize stages from a single workflow before
    # stages from later workflows. E.g., if two workflows each have stages A,
    # B, C, then on one worker, the run order will be A1 B1 C1 A2 B2 C2 instead
    # of A1 A2 B1 B2 C1 C2. There are two reasons: First, this means we get
    # final outputs ("C" stages) earlier, which can be helpful for debugging.
    # Second is performance: If we finish an entire workflow, we no longer have
    # to keep its tasks in memory, and restarts of the main program will be
    # faster thanks to workflow caching.  Note this covers a slightly different
    # case than the workflow generator order, which submits tasks with no
    # dependencies first to avoid having tasks in memory that can't run. Here,
    # we specifically deal with the case where all tasks are submitted and parsl has
    # a choice between stages of different depths to submit
    resource_spec = {'priority': self.workflow_id}

    future = future_func(
        workdir = str(run_dir),
        stdout = str(run_dir / context.output_file.name.replace(".root", ".out")),
        stderr = str(run_dir / context.output_file.name.replace(".root", ".err")),
        template = template,
        cmd = cmd,
        larsoft_opts = executor.larsoft_opts,
        inputs = input_arg,
        outputs = [File(str(context.output_file))],
        pre_job_hook = mg_cmd,
        parsl_resource_specification=resource_spec
    )

    _transfer_ids(self, future.outputs[0])

    # this modifies the list passed in by WorkflowExecutor
    executor.futures.append(future.outputs[0])

    return [future.outputs, [], '']



# -------
#
# RUNFUNCS
# 
# -------

def build_modify_fcl_cmd_sbnd_mc(context: RunContext) -> str:
    """Normal MC fcl command + superaMC renaming"""
    fcl_cmd = build_modify_fcl_cmd(context)
    fcl_name = context.fcl.name
    if context.stage.stage_type == DefaultStageTypes.RECO1:
        # find the first component in the output file path with "reco1" & replace with "larcv"
        larcv_dir = pathlib.PurePosixPath(*[p if p != 'reco1' else 'larcv' for p in context.output_file.parent.parts])
        larcv_filename = larcv_dir / f"larcv_{context.output_file.name}"

        fcl_cmd = '\n'.join([
            f'mkdir -p {str(larcv_dir)}',
            fcl_cmd,
            f'''echo "physics.analyzers.supera.out_filename: \\"{str(larcv_filename)}\\"" >> {fcl_name}''',
            f'''echo "physics.analyzers.supera.unique_filename: false" >> {fcl_name}'''
        ])

    return fcl_cmd

def build_larsoft_cmd_drop_reco2(context: RunContext) -> str:
    """variation of larsoft command to add -T flag for calibration ntuple
    output path when reco2 is skipped. Assumes reco1 is kept (not written to /tmp)"""
    # caf stage does not get an output argument
    lar_cmd = build_larsoft_cmd(context)
    if context.stage.stage_type == DefaultStageTypes.RECO2:
        calib_dir = pathlib.PurePosixPath(*[p if p != 'reco1' else 'calib_ntuple' for p in context.input_files[0].parent.parts])
        calib_filename = calib_dir / f"hists_{context.input_files[0].name}"
        lar_cmd = ' '.join([
            lar_cmd,
            f'-T {str(calib_filename)}'
        ])
        lar_cmd = '\n'.join([
            'rm -f standard_reco2_sbnd.fcl',
            f'mkdir -p {str(calib_dir)}',
            lar_cmd
        ])

    return lar_cmd

mc_runfunc_sbnd=functools.partial(larsoft_runfunc, lar_cmd_func=build_larsoft_cmd_drop_reco2, fcl_cmd_func=build_modify_fcl_cmd_sbnd_mc)

def build_larsoft_cmd_sbnd_data(context: RunContext) -> str:
    """Build larsoft command with input & output flags"""
    # caf stage does not get an output argument
    output_file_arg_str = ''
    if context.stage.stage_type != DefaultStageTypes.CAF:
        output_file_arg_str = f'--output={str(context.output_file)}'

    # specify output stream for SBND decode stage
    if context.stage.stage_type == DefaultStageTypes.DECODE:
        output_file_arg_str = f'--output=out1:{str(context.output_file)}'

    input_file_arg_str = ''
    if context.input_files:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' for file in context.input_files])

    # stages after gen: don't limit number of events; use all events from all input files
    if context.stage.stage_type != DefaultStageTypes.GEN:
        nevts = f' --nevts=-1'
    else:
        nevts = f' --nevts={context.lar_args["nevts"]}'

    nskip = ''
    try:
        nskip = f' --nskip={context.lar_args["nskip"]}'
    except KeyError:
        pass

    return f'lar -c {context.fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}'


def output_filepath_sbnd_data(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on input filename."""
    if context.stage.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        output_filename = '-'.join([
            str(context.stage.stage_type.name), context.input_files[0].name,
        ])
    else:
        output_filename = os.path.splitext(context.input_files[0].name)[0] + '.flat.caf.root'

    return context.out_dir / pathlib.Path(context.label, context.stage.stage_type.name, \
            f'{context.stage.workflow_id // 1000:06d}', \
            f'{context.stage.workflow_id // 100:06d}', output_filename)

data_runfunc_sbnd=functools.partial(larsoft_runfunc, lar_cmd_func=build_larsoft_cmd_sbnd_data, output_filename_func=output_filepath_sbnd_data)


# icarus: different caf name
def output_filepath_icarus_data(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on input filename."""
    nskip = 0
    try:
        nskip = context.lar_opts['nskip']
    except KeyError:
        pass
    if context.stage.stage_type != DefaultStageTypes.CAF:
        # from string or posixpath input
        output_filename = '-'.join([
            str(stage.stage_type.name), f'{nskip:03d}', os.path.basename(first_file_name),
        ])
    else:
        output_filename = os.path.splitext(os.path.basename(first_file_name))[0] + '.Blind.OKTOLOOK.flat.caf.root'

    return context.out_dir / pathlib.Path(label, stage.stage_type.name, \
            f'{stage.workflow_id // 1000:06d}', \
            f'{stage.workflow_id // 100:06d}', output_filename)

def output_filepath_icarus_mc(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on input filename (for overlay MC)."""
    nskip = 0
    try:
        nskip = context.lar_args['nskip']
    except KeyError:
        pass
    if context.stage.stage_type != DefaultStageTypes.CAF:
        # add, e.g., "-030-" to filename if we are only processing part of it
        skip_str = f'{nskip:03d}' if context.lar_args['nevts'] > 0 else ''
        # from string or posixpath input
        output_filename = '-'.join(filter(None, [
            str(context.stage.stage_type.name), skip_str, context.input_files[0].name,
        ]))
    else:
        output_filename = os.path.splitext(context.input_files[0].name)[0] + '.flat.caf.root'

    return context.out_dir / pathlib.Path(context.stage.stage_type.name, \
            f'{context.stage.workflow_id // 1000:06d}', \
            f'{context.stage.workflow_id // 100:06d}', output_filename)

def build_modify_fcl_cmd_icarus(context: RunContext):
    """generate bash commands that modify fcl"""
    fcl_cmd = ''
    fcl_name = context.fcl.name
    if context.stage.stage_type.name == 'overlay':
        run_number = 1 + (context.stage.workflow_id // 100)
        subrun_number = context.stage.workflow_id % 100
        fcl_cmd = '\n'.join([
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFilesIcarus/\\"" >> {fcl_name}''',
            f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\\" ]" >> {fcl_name}''',
            f'''echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {fcl_name}''',
        ])
    elif context.stage.stage_type == DefaultStageTypes.STAGE1:
        # find the first component in the output file path with "reco1" & replace with "larcv"
        larcv_dir = pathlib.PurePosixPath(*[p if p != 'stage1' else 'larcv' for p in context.output_file.parent.parts])
        larcv_filename = larcv_dir / f"larcv_{context.output_file.name}"

        fcl_cmd = '\n'.join([
            f'mkdir -p {str(larcv_dir)}',
            fcl_cmd,
            f'''echo "physics.analyzers.superaMC.out_filename: \\"{str(larcv_filename)}\\"" >> {fcl_name}''',
            f'''echo "physics.analyzers.superaMC.unique_filename: false" >> {fcl_name}'''
        ])

    return fcl_cmd


def build_larsoft_cmd_icarus_overlay_mc(context: RunContext) -> str:
    """Special larsoft command for icarus overlay MC: first stage is not "gen"
    but we still want to use nevts and nskip flags"""
    if context.stage.stage_type.name == 'overlay':
        nevts = f' --nevts=-1'
        try:
            nevts = f' --nevts={context.lar_args["nevts"]}'
        except KeyError:
            pass

        nskip = ''
        try:
            nskip = f' --nskip={context.lar_args["nskip"]}'
        except KeyError:
            pass
        output_file_arg_str = ''
        if context.stage.stage_type != DefaultStageTypes.CAF:
            output_file_arg_str = f'--output={str(context.output_file)}'

        input_file_arg_str = ''
        if context.input_files:
            input_file_arg_str = \
                ' '.join([f'-s {str(file)}' for file in context.input_files])

        return f'lar -c {context.fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}'
    elif context.stage.stage_type == DefaultStageTypes.STAGE1:
        # make a calib_ntuple directory
        calib_dir = pathlib.PurePosixPath(*[p if p != 'stage0' else 'calib_ntuple' for p in context.input_files[0].parent.parts])
        calib_filename = calib_dir / f"hists_{context.input_files[0].name}"
        lar_cmd = ' '.join([
            build_larsoft_cmd(context),
            f'-T {str(calib_filename)}'
        ])
        return '\n'.join([
            f'mkdir -p {str(calib_dir)}',
            lar_cmd
        ])
    
    # default case
    return build_larsoft_cmd(context)


mc_runfunc_icarus=functools.partial(larsoft_runfunc, lar_cmd_func=build_larsoft_cmd_icarus_overlay_mc, \
                                    output_filename_func=output_filepath_icarus_mc, fcl_cmd_func=build_modify_fcl_cmd_icarus)
data_runfunc_icarus=functools.partial(larsoft_runfunc, output_filename_func=output_filepath_icarus_data)
