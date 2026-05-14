import pathlib
import functools
from sbn_parsl.workflow import DefaultStageTypes
from sbn_parsl.components import (
    larsoft_runfunc,
    build_larsoft_cmd,
    build_modify_fcl_cmd,
    output_filepath,
    output_filepath_generic,
    RunContext,
)
from sbn_parsl.experiments.registry import sbnd_registry


@sbnd_registry.register_fcl_modifier("mc")
def build_modify_fcl_cmd_sbnd_mc(context: RunContext) -> str:
    """Normal MC fcl command + superaMC renaming"""
    fcl_cmd = build_modify_fcl_cmd(context)
    fcl_name = context.fcl.name
    if context.stage.stage_type == DefaultStageTypes.RECO1:
        # find the first component in the output file path with "reco1" & replace with "larcv"
        larcv_dir = pathlib.PurePosixPath(
            *[p if p != "reco1" else "larcv" for p in context.output_file.parent.parts]
        )
        larcv_filename = larcv_dir / f"larcv_{context.output_file.name}"

        fcl_cmd = "\n".join(
            [
                f"mkdir -p {str(larcv_dir)}",
                fcl_cmd,
                f'''echo "physics.analyzers.supera.out_filename: \\"{str(larcv_filename)}\\"" >> {fcl_name}''',
                f"""echo "physics.analyzers.supera.unique_filename: false" >> {fcl_name}""",
            ]
        )

    return fcl_cmd


mc_runfunc_sbnd = functools.partial(
    larsoft_runfunc,
    output_filename_func=output_filepath,
    lar_cmd_func=functools.partial(
        build_larsoft_cmd, calib_ntuple_stage=DefaultStageTypes.RECO2
    ),
    fcl_cmd_func=build_modify_fcl_cmd_sbnd_mc,
)
sbnd_registry.register_runfunc("mc", mc_runfunc_sbnd)

data_runfunc_sbnd = functools.partial(
    larsoft_runfunc,
    lar_cmd_func=functools.partial(build_larsoft_cmd, decode_stream="out1"),
    output_filename_func=functools.partial(output_filepath_generic, is_mc=False),
)
sbnd_registry.register_runfunc("data", data_runfunc_sbnd)
