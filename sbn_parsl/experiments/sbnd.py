import functools
from sbn_parsl.workflow import DefaultStageTypes
from sbn_parsl.components import (
    larsoft_runfunc,
    build_larsoft_cmd,
    build_modify_fcl_cmd,
    output_filepath,
    output_filepath_generic,
    RunContext,
    sibling_output_dir,
    local_buffer_path,
    local_discard_path,
)
from sbn_parsl.experiments.registry import sbnd_registry


@sbnd_registry.register_fcl_modifier("mc")
def build_modify_fcl_cmd_sbnd_mc(context: RunContext, save_larcv: bool = True) -> str:
    """Normal MC fcl command + supera output naming"""
    fcl_cmd = build_modify_fcl_cmd(context)
    fcl_name = context.fcl.name
    if context.stage.stage_type == DefaultStageTypes.RECO1:
        # written to the node-local buffer, then copied to <out>/larcv/...
        larcv_name = f"larcv_{context.output_file.name}"
        if save_larcv:
            larcv_filename = local_buffer_path(
                context, sibling_output_dir(context, "larcv") / larcv_name
            )
        else:
            larcv_filename = local_discard_path(larcv_name)

        fcl_cmd = "\n".join(
            [
                f"mkdir -p {larcv_filename.parent}",
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
