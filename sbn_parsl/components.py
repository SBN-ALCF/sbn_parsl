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
    rootdir=""
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
        rootdir=rootdir
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


# Unblinded CAF stages write two files: the flat CAF (the stage's tracked
# output) under FLATCAF_DIR and the regular CAF under CAF_DIR, side by side
# in otherwise identical directory trees.
CAF_DIR = "caf"
FLATCAF_DIR = "flatcaf"
CAF_SUFFIX = ".caf.root"
FLATCAF_SUFFIX = ".flat.caf.root"


def _get_output_dir(
    context: RunContext, use_label: bool = True, stage_dir: Optional[str] = None
) -> pathlib.Path:
    """
    Generates a structured output directory path.

    The path follows the pattern: [label/]<stage_dir>/<workflow_batch>/<workflow_id>,
    where stage_dir defaults to the stage name.
    Batches are grouped by 1000s and 100s to avoid directory scaling issues.
    """
    parts = []
    if use_label and context.label:
        parts.append(context.label)
    parts.append(stage_dir or context.stage.stage_type.name)
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
        stem = os.path.splitext(context.input_files[0].name)[0]
        if blind_caf:
            # CAFMaker names the blinded files itself, in its cwd; the
            # template sweeps them all into the CAF_DIR tree
            return _get_output_dir(context, use_label) / (
                stem + ".Blind.OKTOLOOK" + FLATCAF_SUFFIX
            )
        return _get_output_dir(context, use_label, stage_dir=FLATCAF_DIR) / (
            stem + FLATCAF_SUFFIX
        )
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


# Per-task node-local buffer created by the templates (LOCAL_BUFFER_INIT). The
# variable is expanded by the task's host shell, including inside the
# container heredoc, so these paths can go straight into commands and fcls.
LOCAL_BUFFER = pathlib.PurePosixPath("$SBN_LOCAL_BUFFER")


def sibling_output_dir(context: RunContext, dirname: str) -> pathlib.PurePosixPath:
    """
    The final directory, under the run's output directory, that sits alongside
    this stage's own output directory but named dirname instead of the stage.

    E.g. <out>/reco1/000000/000000 -> <out>/larcv/000000/000000. Uses where the
    output belongs, not where it is written: a combined stage writes its output
    to /tmp, but its side outputs still belong under the output directory.
    """
    out_root = pathlib.PurePosixPath(context.cfg.run.output)
    rel = pathlib.PurePosixPath(str(context.output_file)).parent.relative_to(
        str(context.out_dir)
    )
    stage_name = context.stage.stage_type.name
    return out_root.joinpath(*[p if p != stage_name else dirname for p in rel.parts])


def final_output_dir(context: RunContext) -> pathlib.PurePosixPath:
    """This stage's own output directory under the run's output directory."""
    return sibling_output_dir(context, context.stage.stage_type.name)


def local_buffer_path(context: RunContext, final_path: pathlib.PurePath) -> pathlib.PurePosixPath:
    """Where to write final_path (under the output directory) so it is copied
    there when the task succeeds."""
    rel = pathlib.PurePosixPath(str(final_path)).relative_to(context.cfg.run.output)
    return LOCAL_BUFFER / "keep" / rel


def local_discard_path(filename: str) -> pathlib.PurePosixPath:
    """Where to send an output that must be written but not kept."""
    return LOCAL_BUFFER / "discard" / filename


def regular_caf_path(flat_caf: pathlib.PurePath) -> pathlib.PurePosixPath:
    """The regular CAF written alongside an unblinded flat CAF."""
    parent = pathlib.PurePosixPath(
        *[p if p != FLATCAF_DIR else CAF_DIR for p in flat_caf.parent.parts]
    )
    return parent / (flat_caf.name[: -len(FLATCAF_SUFFIX)] + CAF_SUFFIX)


def build_caf_filename_cmd(context: RunContext) -> str:
    """
    Point CAFMaker's two outputs at their final paths.

    Without this, CAFMaker derives both names from its first input and writes
    them to its cwd. Only applies to the unblinded layout: with blinding,
    CAFMaker strips the directory from explicit names when deriving the
    Blind/Prescale files, so it is left to name everything itself.
    """
    flat = pathlib.PurePosixPath(context.output_file)
    if (
        context.stage.stage_type != DefaultStageTypes.CAF
        or FLATCAF_DIR not in flat.parent.parts
    ):
        return ""

    caf = regular_caf_path(flat)
    fcl_name = context.fcl.name
    return "\n".join(
        [
            f"mkdir -p {caf.parent}",
            f"mkdir -p {flat.parent}",
            f'''echo "physics.producers.cafmaker.CAFFilename: \\"{caf}\\"" >> {fcl_name}''',
            f'''echo "physics.producers.cafmaker.FlatCAFFilename: \\"{flat}\\"" >> {fcl_name}''',
        ]
    )


def build_larsoft_cmd(
    context: RunContext,
    decode_stream: str = "",
    calib_ntuple_stage: Optional[StageType] = None,
    save_calib_ntuple: bool = True,
    save_hists: bool = False,
) -> str:
    """
    Constructs the standard 'lar' command line execution string.

    Handles input/output flags, event counts (nevts), skip offsets (nskip),
    and the TFileService file (-T): the calibration ntuple on calib_ntuple_stage,
    a histogram file on every other stage.
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

    # -T is always given: otherwise the fcl's TFileService file lands in the
    # run dir, which every stage of a combined task shares, and the template's
    # end-of-task sweep moves it into the final stage's output directory
    if calib_ntuple_stage and context.stage.stage_type == calib_ntuple_stage:
        tfile_name = f"hists_{context.input_files[0].name}"
        if save_calib_ntuple:
            tfile = local_buffer_path(
                context, sibling_output_dir(context, "calib_ntuple") / tfile_name
            )
        else:
            tfile = local_discard_path(tfile_name)
    else:
        # a combined stage's output is scratch, so are its histograms
        tfile_name = f"hists_{context.output_file.name}"
        if save_hists and not context.stage.combine:
            tfile = local_buffer_path(context, final_output_dir(context) / tfile_name)
        else:
            tfile = local_discard_path(tfile_name)
    tfile_str = f" -T {tfile}"
    mkdir_cmd = f"mkdir -p {tfile.parent}\n"

    cmd = f"lar -c {context.fcl} {input_file_arg_str} {output_file_arg_str}{nevts}{nskip}{tfile_str} --tmpdir=/tmp"
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
        run_number = 1 + (context.stage.workflow_id // 100) + context.lar_args.first_run
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

    # Optional submit-time filesystem check, for when the cache database is
    # believed to have missed completed stages. Combined stages are excluded:
    # their output lives in /tmp on a worker, so a stale local file would be
    # meaningless, and skipping one would drop its command from the child's
    # script and leave the child with an input that was never produced.
    if (
        executor.cfg.run.check_existing_outputs
        and not executor.dry_run
        and not self.combine
        and context.output_file.is_file()
    ):
        executor._skip_counter += 1
        executor._file_skip_counter += 1
        return StageResult(outputs=[context.output_file])

    # clean any combined-stage scratch inputs in /tmp after this stage
    # completes. Inputs under the output directory are real stage outputs and
    # must survive, even when that directory is itself in /tmp (a DAOS dfuse
    # mount lives at /tmp/<pool>/<cont>).
    out_root = pathlib.PurePosixPath(executor.output_dir)
    rm_cmd = "\n".join(
        [
            f"rm {f}"
            for f in context.input_files
            if str(f).startswith("/tmp/") and not f.is_relative_to(out_root)
        ]
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

    # fcl manipulation first, then metadata injection
    mg_cmd = fcl_cmd_func(context)
    caf_cmd = build_caf_filename_cmd(context)
    if caf_cmd:
        mg_cmd += "\n" + caf_cmd
    # a combined stage's output is deleted once its child has read it, so
    # there is nothing to describe
    if meta is not None and not self.combine:
        json_file = local_buffer_path(
            context, final_output_dir(context) / (context.output_file.name + ".json")
        )
        mg_cmd += f"\nmkdir -p {json_file.parent}\n" + meta.run_cmd(
            str(json_file),
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

    resource_spec = {"priority": executor.task_priority(self)}

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
        rootdir=str(context.cfg.run.output)
    )

    _transfer_ids(self, future.outputs[0])
    executor.register_future(future.outputs[0])
    executor._stage_counter += 1

    return StageResult(outputs=future.outputs)



