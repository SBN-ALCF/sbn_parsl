"""
Components module which contains runfunc & helper functions for common workflows
New runfuncs can be build by passing different helpers, here: _components_, to the runfunc
Each component takes a "RunContext" object as an argument that contains
information about the stage and user settings,
"""

from typing import List, Optional
from dataclasses import dataclass, field
import pathlib
import os
import functools

import parsl
from parsl.data_provider.files import File
from parsl.app.app import bash_app

from sbn_parsl.workflow import (
    StageType,
    Stage,
    DefaultStageTypes,
    LArSoftExecutor,
    StageResult,
)
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.config import Config, LArSoftConfig

from sbn_parsl.utils import hash_name


@bash_app(cache=False)
def fcl_future(
    workdir,
    stdout,
    stderr,
    template,
    cmd,
    larsoft_opts,
    inputs=[],
    outputs=[],
    pre_job_hook="",
    post_job_hook="",
    parsl_resource_specification={},
):
    """Return formatted bash script which produces each future when executed."""
    return template.format(
        fhicl=inputs[0],
        workdir=workdir,
        output=outputs[0],
        input=inputs[1] if len(inputs) > 1 else "",
        cmd=cmd,
        **larsoft_opts,
        pre_job_hook=pre_job_hook,
        post_job_hook=post_job_hook,
    )


@dataclass
class RunContext:
    """
    State container passed between components during the execution of a stage.

    Encapsulates all information required to generate the final execution command,
    including paths, configurations, and stage-specific metadata.
    """

    stage: Stage
    cfg: Config
    input_files: List[pathlib.Path] = field(default_factory=list)
    out_dir: pathlib.Path = pathlib.Path("")
    fcl: pathlib.Path = None
    lar_args: LArSoftConfig = None
    salt: str = ""
    label: str = ""
    output_file: pathlib.Path = pathlib.Path("")
    meta: MetadataGenerator = None


def _get_output_dir(context: RunContext, use_label: bool = True) -> pathlib.Path:
    """
    Generates a structured output directory path.

    The path follows the pattern: [label/]<stage_name>/<workflow_batch>/<workflow_id>
    Batches are grouped by 1000s and 100s to avoid directory scaling issues.
    """
    parts = []
    if use_label and context.label:
        parts.append(context.label)
    parts.append(context.stage.stage_type.name)
    parts.append(f"{context.stage.workflow_id // 1000:06d}")
    parts.append(f"{context.stage.workflow_id // 100:06d}")
    return context.out_dir.joinpath(*parts)


def output_filepath_generic(
    context: RunContext,
    use_label: bool = True,
    include_skip: bool = False,
    blind_caf: bool = False,
    is_mc: bool = False,
) -> pathlib.Path:
    """
    Determines the full output file path based on experiment-specific conventions.

    Handles special cases for CAF files (blinding), MC first stages (hashing
    for uniqueness), and standard derivative stages (chaining filenames).
    """
    if context.stage.stage_type == DefaultStageTypes.CAF:
        caf_suffix = ".Blind.OKTOLOOK.flat.caf.root" if blind_caf else ".flat.caf.root"
        output_filename = os.path.splitext(context.input_files[0].name)[0] + caf_suffix
    elif is_mc and context.stage.is_first():
        _label = context.label + "-" if context.label else ""
        output_filename = "".join(
            [
                str(context.stage.stage_type.name),
                "-",
                _label,
                hash_name(context.fcl.name + context.salt + context.stage.stage_id_str),
                ".root",
            ]
        )
    else:
        parts = [str(context.stage.stage_type.name)]
        if include_skip and context.lar_args.nskip > 0 and context.lar_args.nevts > 0:
            parts.append(f"{context.lar_args.nskip:03d}")
        parts.append(context.input_files[0].name)
        output_filename = "-".join(parts)

    return _get_output_dir(context, use_label) / output_filename


output_filepath = functools.partial(output_filepath_generic, is_mc=True, use_label=True)


def build_larsoft_cmd(
    context: RunContext,
    decode_stream: str = "",
    calib_ntuple_stage: Optional[StageType] = None,
) -> str:
    """
    Constructs the standard 'lar' command line execution string.

    Handles input/output flags, event counts (nevts), skip offsets (nskip),
    and optional calibration ntuple redirection (-T).
    """
    output_file_arg_str = ""
    if context.stage.stage_type != DefaultStageTypes.CAF:
        stream_prefix = (
            f"{decode_stream}:"
            if (decode_stream and context.stage.stage_type == DefaultStageTypes.DECODE)
            else ""
        )
        output_file_arg_str = f"--output={stream_prefix}{str(context.output_file)}"

    input_file_arg_str = ""
    if context.input_files:
        input_file_arg_str = " ".join(
            [f"-s {str(file)}" for file in context.input_files]
        )

    # first stage: apply nevts
    nevts = " --nevts=-1"
    if context.stage.is_first():
        nevts = f" --nevts={context.lar_args.nevts}"

    nskip = ""
    if context.lar_args.nskip > 0:
        nskip = f" --nskip={context.lar_args.nskip}"

    calib_str = ""
    mkdir_cmd = ""
    if calib_ntuple_stage and context.stage.stage_type == calib_ntuple_stage:
        parent_name = (
            context.stage.parent_type.name if context.stage.parent_type else "unknown"
        )
        parent_dir = pathlib.PurePosixPath(
            *[
                p if p != parent_name else "calib_ntuple"
                for p in context.input_files[0].parent.parts
            ]
        )
        calib_filename = parent_dir / f"hists_{context.input_files[0].name}"
        calib_str = f" -T {str(calib_filename)}"
        mkdir_cmd = f"mkdir -p {str(parent_dir)}\n"

    cmd = f"lar -c {context.fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}{calib_str} --tmpdir=/tmp"
    return mkdir_cmd + cmd


def build_modify_fcl_cmd(context: RunContext) -> str:
    """
    Generates bash commands to append experiment-specific overrides to a FHiCL file.

    For GEN stages, this includes run/subrun/event numbering and flux path
    configurations required for reproducibility.
    """
    fcl_cmd = ""
    fcl_name = context.fcl.name
    if context.stage.stage_type == DefaultStageTypes.GEN:
        run_number = 1 + (context.stage.workflow_id // 100)
        # Try to get first_run from lar_args if it was added as extra, though not in dataclass yet
        # For now, use 0 as default
        subrun_number = context.stage.workflow_id % 100
        event_number = 1 + (context.lar_args.nevts * context.stage.stage_id[-1])

        # Use site-specific simulation_inputs if not provided in larsoft config
        sim_inputs = context.cfg.larsoft.simulation_inputs

        fcl_cmd = "\n".join(
            [
                f'echo "" >> {fcl_name}',
                f'echo "source.firstRun: {run_number}" >> {fcl_name}',
                f'echo "source.firstSubRun: {subrun_number}" >> {fcl_name}',
                f'echo "source.firstEvent: {event_number}" >> {fcl_name}',
                f'''echo "physics.producers.generator.FluxSearchPaths: \\"{sim_inputs}/{context.lar_args.flux_path}/\\"" >> {fcl_name}''',
                f'''echo "physics.producers.generator.FluxFiles: [ \\"{context.lar_args.flux_files}\\" ]" >> {fcl_name}''',
                f'''echo "physics.producers.generator.FluxType:  \\"{context.lar_args.flux_type}\\"" >> {fcl_name}''',
                f'''echo "physics.producers.corsika.ShowerInputFiles: [ \\"{sim_inputs}/CorsikaDBFiles/p_showers_*.db\\" ]" >> {fcl_name}''',
                f"""echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> {fcl_name}""",
            ]
        )

    return fcl_cmd


def _transfer_ids(stage: Stage, future):
    """add some custom member functions to track the parent workflow when these stages complete"""
    future.workflow_id = stage.workflow_id
    future.stage_id = stage.stage_id_str
    future.final = stage.final


def larsoft_runfunc(
    self,
    fcl,
    parent_result: StageResult,
    run_dir,
    template,
    executor: LArSoftExecutor,
    meta=None,
    label="",
    last_file=None,
    output_filename_func=output_filepath,
    lar_cmd_func=build_larsoft_cmd,
    fcl_cmd_func=build_modify_fcl_cmd,
    future_func=fcl_future,
    **kwargs,
) -> StageResult:
    """
    Core orchestrator that converts a Stage into a Parsl task.

    This function is responsible for:
    1. Resolving input and output file paths.
    2. Checking the SQLite cache for existing results to enable fast restarts.
    3. Building the execution environment (mkdir, cd, environment variables).
    4. Orchestrating FHiCL modifications and metadata generation.
    5. Submitting the task via a `bash_app` (defined by `future_func`).
    6. Handling dependencies, including "combined" stages where multiple
       commands are executed in a single shell script.

    Args:
        self: The Stage object being executed.
        fcl: Path to the FHiCL file for this stage.
        parent_result: Output and dependency information from predecessors.
        run_dir: Directory where the task's logs and temporary files live.
        template: The bash script template for the Parsl app.
        executor: The active LArSoftExecutor managing the workflow.
        meta: Optional MetadataGenerator for FNAL/SAM metadata.
        label: Optional string label for directory organization.
        last_file: Optional output from a previous workflow to chain dependencies.
        output_filename_func: Logic for naming the resulting root file.
        lar_cmd_func: Logic for building the 'lar' CLI command.
        fcl_cmd_func: Logic for modifying the FHiCL file.
        future_func: The Parsl bash_app to use for submission.

    Returns:
        A StageResult containing the new DataFutures or local file paths.
    """

    # Create a copy of larsoft config to apply overrides if any
    lar_cfg = executor.cfg.larsoft
    # Note: currently dataclass overrides via kwargs is tricky.
    # For now, let's assume kwargs are for legacy or specific overrides we handle.

    overlay_str = ""
    if lar_cfg.overlays:
        overlay_str = " ".join(
            ["--overlay", ",".join(f"{overlay}:ro" for overlay in lar_cfg.overlays)]
        )

    input_files = parent_result.outputs
    input_files = [str(f) if isinstance(f, pathlib.Path) else f for f in input_files]

    depends = parent_result.dependencies
    parent_cmd = parent_result.command

    context = RunContext(
        stage=self,
        cfg=executor.cfg,
        input_files=[
            pathlib.PurePosixPath(f.filename)
            if isinstance(f, parsl.app.futures.DataFuture)
            else pathlib.PurePosixPath(f)
            for f in input_files
            if f is not None
        ],
        out_dir=executor.output_dir if not self.combine else pathlib.Path("/tmp"),
        fcl=pathlib.PurePosixPath(fcl),
        label=label,
        lar_args=lar_cfg,
        salt=executor.name_salt,
        meta=meta,
    )

    context.output_file = output_filename_func(context)

    if (
        executor.stage_in_db(
            self.stage_id_str, require_success=executor.cfg.run.require_success
        )
        and not executor.dry_run
    ):
        executor._skip_counter += 1
        return StageResult(outputs=[context.output_file])

    # clean any input files that are in /tmp after this stage completes
    rm_cmd = "\n".join(
        [f"rm {f}" for f in context.input_files if str(f).startswith("/tmp/")]
    )

    cmd = "\n".join(
        [
            f"mkdir -p {run_dir}",
            f"mkdir -p {context.output_file.parent}",
            f"cd {run_dir}",
            lar_cmd_func(context),
            rm_cmd,
        ]
    )

    if parent_cmd:
        cmd = "\n".join([parent_cmd, cmd])

    dummy_input = []
    if last_file is not None:
        dummy_input = last_file[0]
    if hasattr(context.stage, "dependency"):
        if context.stage.dependency is not None:
            dummy_input += context.stage.dependency.output_files[0]
    input_arg = [str(fcl)] + dummy_input + input_files + depends

    # metadata & fcl manipulation
    mg_cmd = fcl_cmd_func(context)
    if meta is not None:
        mg_cmd += "\n" + meta.run_cmd(
            context.output_file.name + ".json",
            os.path.basename(fcl),
            check_exists=False,
        )

    if executor.dry_run:
        print(f"--- DRY RUN: Stage {self.stage_type.name} [{self.stage_id_str}] ---")
        print(f"RUN_DIR: {run_dir}")
        print(f"OUTPUT:  {context.output_file}")
        print("COMMAND:")
        print(mg_cmd + "\n" + cmd)
        print("-" * 40)
        return StageResult(outputs=[context.output_file])

    if self.combine:
        return StageResult(
            outputs=[context.output_file],
            dependencies=dummy_input + input_files + depends,
            command=mg_cmd + "\n" + cmd,
        )

    resource_spec = {"priority": self.workflow_id}

    stdout = str(run_dir / context.output_file.name.replace(".root", ".out"))
    stderr = str(run_dir / context.output_file.name.replace(".root", ".err"))
    if context.output_file.suffix != ".root":
        stdout += ".out"
        stderr += ".err"

    # Convert Config to dict for template formatting if needed,
    # but old template used larsoft_opts dict.
    lar_dict = executor.cfg.larsoft.__dict__.copy()
    lar_dict["overlay_str"] = overlay_str
    # Include site container info
    lar_dict["container"] = executor.cfg.larsoft.container_path
    lar_dict["larsoft_top"] = executor.cfg.larsoft.larsoft_top

    future = future_func(
        workdir=str(run_dir),
        stdout=stdout,
        stderr=stderr,
        template=template,
        cmd=cmd,
        larsoft_opts=lar_dict,
        inputs=input_arg,
        outputs=[File(str(context.output_file))],
        pre_job_hook=mg_cmd,
        parsl_resource_specification=resource_spec,
    )

    _transfer_ids(self, future.outputs[0])
    executor.futures.add(future.outputs[0])
    executor._stage_counter += 1

    return StageResult(outputs=future.outputs)


# -------
#
# RUNFUNCS (Moved to sbn_parsl.experiments)
#
# -------

def output_filepath_sbnd_mc(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on a stage and its input.
    For GEN: Use random string. For later stages, use <stage name>-<input file>.root
    """
    if context.stage.stage_type == DefaultStageTypes.GEN:
        output_filename = _gen_filename(context)
    elif context.stage.stage_type == DefaultStageTypes.CAF:
        output_filename = _caf_filename(context)
    else:
        output_filename = context.stage.stage_type.name + '-' \
                + os.path.splitext(context.input_files[0].name)[0] + '.root'

    return _base_output_dir(context) / output_filename


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
            f'mkdir -p {str(calib_dir)}',
            lar_cmd
        ])

    return lar_cmd

mc_runfunc_sbnd=functools.partial(larsoft_runfunc,
                                  output_filename_func=output_filepath_sbnd_mc,
                                  lar_cmd_func=build_larsoft_cmd_drop_reco2,
                                  fcl_cmd_func=build_modify_fcl_cmd_sbnd_mc)

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
    if context.stage.stage_type == DefaultStageTypes.CAF:
        output_filename = _caf_filename(context)
    else:
        # from string or posixpath input
        output_filename = '-'.join([
            str(context.stage.stage_type.name), context.input_files[0].name,
        ])

    return _base_output_dir(context) / output_filename

data_runfunc_sbnd=functools.partial(larsoft_runfunc, lar_cmd_func=build_larsoft_cmd_sbnd_data, output_filename_func=output_filepath_sbnd_data)


# icarus: different caf name
def output_filepath_icarus_data(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on input filename."""
    nskip = context.lar_args.get('nskip', 0)
    if context.stage.stage_type == DefaultStageTypes.CAF:
        output_filename = os.path.splitext(context.input_files[0].name)[0] + '.Blind.OKTOLOOK.flat.caf.root'
    else:
        # from string or posixpath input
        output_filename = '-'.join([
            str(context.stage.stage_type.name), f'{nskip:03d}', context.input_files[0].name,
        ])

    return _base_output_dir(context) / output_filename

def output_filepath_icarus_mc(context: RunContext) -> pathlib.Path:
    """Pick an output file name based on input filename (for overlay MC)."""
    nskip = context.lar_args.get('nskip', 0)
    if context.stage.stage_type == DefaultStageTypes.CAF:
        output_filename = _caf_filename(context)
    else:
        # add, e.g., "-030-" to filename if we are only processing part of it
        skip_str = f'{nskip:03d}' if context.lar_args.get('nevts', -1) > 0 else ''
        # from string or posixpath input
        output_filename = '-'.join(filter(None, [
            str(context.stage.stage_type.name), skip_str, context.input_files[0].name,
        ]))

    # Icarus MC omits context.label in its directory structure
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
            f'''echo "physics.producers.generator.FluxSearchPaths: \\"{context.lar_args["simulation_inputs"]}/{context.lar_args["flux_path"]}/\\"" >> {fcl_name}''',
            f'''echo "physics.producers.generator.ShowerInputFiles: [" >> {fcl_name}''',

            f'''echo \\"{context.lar_args["simulation_inputs"]}/CORSIKA/standard/p_showers_*.db\\", >> {fcl_name}''',
            f'''echo \\"{context.lar_args["simulation_inputs"]}/CORSIKA/standard/He_showers_*.db\\", >> {fcl_name}''',
            f'''echo \\"{context.lar_args["simulation_inputs"]}/CORSIKA/standard/N_showers_*.db\\", >> {fcl_name}''',
            f'''echo \\"{context.lar_args["simulation_inputs"]}/CORSIKA/standard/Mg_showers_*.db\\", >> {fcl_name}''',
            f'''echo \\"{context.lar_args["simulation_inputs"]}/CORSIKA/standard/Fe_showers_*.db\\" >> {fcl_name}''',
            f'''echo "]" >> {fcl_name}''',
            f'''echo "physics.producers.generator.ShowerCopyType: \\"DIRECT\\"" >> {fcl_name}''',
        ])
    elif context.stage.stage_type == DefaultStageTypes.STAGE1:
        # find the first component in the output file path with "reco1" & replace with "larcv"
        larcv_dir = pathlib.PurePosixPath(*[p if p != 'stage1' else 'larcv' for p in context.output_file.parent.parts])
        larcv_filename = larcv_dir / f"larcv_{context.output_file.name}"

        larcv_dir_str = str(larcv_dir) # .replace("/lus", "/tmp")

        fcl_cmd = '\n'.join([
            f'mkdir -p {larcv_dir_str}',
            fcl_cmd,
            f'''echo "physics.analyzers.superaMC.out_filename: \\"{larcv_dir_str}/{larcv_filename.name}\\"" >> {fcl_name}''',
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
>>>>>>> Stashed changes
