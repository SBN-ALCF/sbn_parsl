#!/usr/bin/env python

# Funnel workflow for rare-signal ("exclusive") samples: files_per_filter GEN
# jobs feed a single filter job, which writes the selected events to one file
# per signal stream. Each stream then runs G4 -> Detsim -> Reco1 -> Reco2 -> CAF
# on its own, under <output>/<stream>/.

import sys
import pathlib
import functools

import parsl

from sbn_parsl.workflow import (
    StageType,
    StageResult,
    Stage,
    Workflow,
    LArSoftExecutor,
    DefaultStageTypes,
)
from sbn_parsl.templates import CMD_TEMPLATE_CONTAINER
from sbn_parsl.components import (
    larsoft_runfunc,
    output_filepath,
    RunContext,
    final_output_dir,
    local_buffer_path,
)
from sbn_parsl.experiments.sbnd import mc_runfunc_sbnd, build_modify_fcl_cmd_sbnd_mc
from sbn_parsl.app import entry_point
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.config import Config
from sbn_parsl.utils import get_subrun_dir, get_caf_dir


FILTER = StageType("filter")

STAGE_ORDER = [
    DefaultStageTypes.GEN,
    FILTER,
    DefaultStageTypes.G4,
    DefaultStageTypes.DETSIM,
    DefaultStageTypes.RECO1,
    DefaultStageTypes.RECO2,
    DefaultStageTypes.CAF,
]


def stream_path(path, stream: str) -> pathlib.PurePosixPath:
    """The per-stream file written alongside the filter's tracked output:
    filter-X.root -> filter-X_<stream>.root"""
    p = pathlib.PurePosixPath(str(path))
    return p.with_name(f"{p.stem}_{stream}{p.suffix}")


def build_larsoft_cmd_filter(context: RunContext, streams) -> str:
    """
    Run the filter over every GEN file in the funnel, writing each stream's
    output module (out<stream>) to its own file next to the tracked output.

    The tracked output itself is an empty placeholder, touched only once lar
    succeeds; downstream G4 stages read the stream files instead. The GEN
    inputs are deleted after a successful filter -- see gen_runfunc for how a
    restart copes with that.
    """
    if context.stage.stage_type != FILTER:
        raise RuntimeError("Tried to use the filter lar command on a non-filter stage")

    output_args = " ".join(
        f"--output=out{stream}:{stream_path(context.output_file, stream)}"
        for stream in streams
    )
    input_args = " ".join(f"-s {f}" for f in context.input_files)
    nskip = f" --nskip={context.lar_args.nskip}" if context.lar_args.nskip > 0 else ""
    tfile = local_buffer_path(
        context, final_output_dir(context) / f"hists_{context.output_file.name}"
    )
    rm_inputs = "rm -f " + " ".join(str(f) for f in context.input_files)

    return "\n".join(
        [
            f"mkdir -p {tfile.parent}",
            f"lar -c {context.fcl} {input_args} {output_args} --nevts=-1{nskip} -T {tfile} --tmpdir=/tmp"
            f" && touch {context.output_file} && {rm_inputs}",
        ]
    )


def gen_runfunc(self, fcl, parent_result, run_dir, executor, filter_stage, **kwargs):
    """
    GEN runfunc that reruns a cached GEN whose output has been deleted.

    A successful filter deletes its GEN inputs. If the run dies after a filter
    finishes but before its success reaches the cache database, a restart finds
    the GENs cached but the filter not, and would otherwise rerun the filter on
    files that no longer exist. So: when a GEN is cached but its filter is not,
    check that the GEN file is really there, and if not, mark it failed so it is
    regenerated. When the filter is cached the GEN files are meant to be gone,
    and nothing is checked; on a first run nothing is cached and nothing is
    checked either.

    Marking the GEN failed only forces a rerun with run.require_success = true
    (the default); without it a failed stage still counts as cached.
    """
    require_success = executor.cfg.run.require_success
    if (
        not executor.dry_run
        and executor.stage_in_db(self.stage_id_str, require_success=require_success)
        and not executor.stage_in_db(filter_stage.stage_id_str, require_success=require_success)
    ):
        context = RunContext(
            stage=self,
            cfg=executor.cfg,
            out_dir=executor.output_dir,
            fcl=pathlib.PurePosixPath(fcl),
            lar_args=executor.cfg.larsoft,
            salt=executor.name_salt,
            label=kwargs.get("label", ""),
        )
        if not pathlib.Path(output_filepath(context)).is_file():
            # Written directly rather than through the database thread's queue,
            # because the cache check in larsoft_runfunc below must already see
            # it. Nothing else writes this stage's row right now: it is only
            # written again when the rerun task completes.
            executor.mark_stage_in_db(self.stage_id_str, status=1)

    return mc_runfunc_sbnd(self, fcl, parent_result, run_dir, executor=executor, **kwargs)


def g4_stream_runfunc(self, fcl, parent_result, run_dir, stream, **kwargs):
    """
    G4 for one stream: read that stream's filter file instead of the filter's
    tracked placeholder, while still waiting on the placeholder's future.
    """
    outputs = []
    dependencies = list(parent_result.dependencies)
    for f in parent_result.outputs:
        if isinstance(f, parsl.app.futures.DataFuture):
            dependencies.append(f)
            f = f.filename
        outputs.append(str(stream_path(f, stream)))

    stream_result = StageResult(
        outputs=outputs, dependencies=dependencies, command=parent_result.command
    )
    return mc_runfunc_sbnd(self, fcl, stream_result, run_dir, label=stream, **kwargs)


class FilterGenExecutor(LArSoftExecutor):
    """Execute a Gen (x N) -> Filter -> [G4 -> Detsim -> Reco1 -> Reco2 -> CAF] per stream workflow."""

    def __init__(self, cfg: Config):
        super().__init__(cfg)
        self.meta = None
        if cfg.larsoft.metadata.exe:
            self.meta = MetadataGenerator(cfg, self.fcls, defer_check=True)

        self.stage_order = [StageType.from_str(key) for key in self.fcls.keys()]
        if self.stage_order != STAGE_ORDER:
            raise ValueError(
                "[workflow.fcls] must list exactly these stages, in this order: "
                + ", ".join(s.name for s in STAGE_ORDER)
            )

        self.streams = list(cfg.workflow.streams)
        self.files_per_filter = cfg.workflow.files_per_filter

        common = dict(executor=self, template=CMD_TEMPLATE_CONTAINER)
        self.gen_runfunc = functools.partial(gen_runfunc, meta=None, **common)
        self.filter_runfunc = functools.partial(
            larsoft_runfunc,
            output_filename_func=output_filepath,
            lar_cmd_func=functools.partial(build_larsoft_cmd_filter, streams=self.streams),
            fcl_cmd_func=build_modify_fcl_cmd_sbnd_mc,
            meta=None,
            **common,
        )
        self.g4_runfuncs = {
            stream: functools.partial(g4_stream_runfunc, stream=stream, meta=None, **common)
            for stream in self.streams
        }
        # detsim, reco1 and reco2 inherit the CAF stage's runfunc
        self.stream_runfuncs = {
            stream: functools.partial(mc_runfunc_sbnd, label=stream, meta=self.meta, **common)
            for stream in self.streams
        }

    def setup_single_workflow(self, iteration: int, file_slice=None, last_file=None):
        workflow = Workflow(self.stage_order, default_fcls=self.fcls)

        s_filter = Stage(FILTER)
        s_filter.runfunc = self.filter_runfunc
        s_filter.run_dir = self.get_run_dir(iteration)

        for stream in self.streams:
            stream_dir = self.output_dir / stream

            s_caf = Stage(DefaultStageTypes.CAF)
            s_caf.runfunc = self.stream_runfuncs[stream]
            s_caf.run_dir = get_caf_dir(stream_dir, iteration)

            s_reco2 = Stage(DefaultStageTypes.RECO2)
            s_reco2.run_dir = get_subrun_dir(stream_dir, iteration)
            s_reco1 = Stage(DefaultStageTypes.RECO1)
            s_detsim = Stage(DefaultStageTypes.DETSIM)
            s_g4 = Stage(DefaultStageTypes.G4)
            s_g4.runfunc = self.g4_runfuncs[stream]

            s_caf.add_parents(s_reco2)
            s_reco2.add_parents(s_reco1)
            s_reco1.add_parents(s_detsim)
            s_detsim.add_parents(s_g4)
            # the same filter stage feeds every stream's G4
            s_g4.add_parents(s_filter)

            # G4 runs in the detsim task, reco2 in the CAF task
            s_g4.combine = True
            s_reco2.combine = True

            workflow.add_final_stage(s_caf)

        gen_runfunc_ = functools.partial(self.gen_runfunc, filter_stage=s_filter)
        for i in range(self.files_per_filter):
            s_gen = Stage(DefaultStageTypes.GEN)
            s_gen.runfunc = gen_runfunc_
            s_gen.run_dir = self.get_run_dir(iteration) / f"gen_{i:03d}"
            s_filter.add_parents(s_gen)

        return workflow


if __name__ == "__main__":
    entry_point(sys.argv, FilterGenExecutor)
