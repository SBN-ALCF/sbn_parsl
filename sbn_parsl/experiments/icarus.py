import pathlib
import functools
from sbn_parsl.workflow import DefaultStageTypes
from sbn_parsl.components import (
    larsoft_runfunc,
    build_larsoft_cmd,
    output_filepath_generic,
    RunContext,
)
from sbn_parsl.experiments.registry import icarus_registry


@icarus_registry.register_fcl_modifier("mc")
def build_modify_fcl_cmd_icarus(context: RunContext):
    """generate bash commands that modify fcl"""
    fcl_cmd = ""
    fcl_name = context.fcl.name
    sim_inputs = context.cfg.larsoft.simulation_inputs

    if context.stage.stage_type.name == "overlay":
        fcl_cmd = "\n".join(
            [
                f'''echo "physics.producers.generator.FluxSearchPaths: \\"{sim_inputs}/{context.lar_args.flux_path}/\\"" >> {fcl_name}''',
                f"""echo "physics.producers.generator.ShowerInputFiles: [" >> {fcl_name}""",
                f'''echo \\"{sim_inputs}/CORSIKA/standard/p_showers_*.db\\", >> {fcl_name}''',
                f'''echo \\"{sim_inputs}/CORSIKA/standard/He_showers_*.db\\", >> {fcl_name}''',
                f'''echo \\"{sim_inputs}/CORSIKA/standard/N_showers_*.db\\", >> {fcl_name}''',
                f'''echo \\"{sim_inputs}/CORSIKA/standard/Mg_showers_*.db\\", >> {fcl_name}''',
                f'''echo \\"{sim_inputs}/CORSIKA/standard/Fe_showers_*.db\\" >> {fcl_name}''',
                f"""echo "]" >> {fcl_name}""",
                f"""echo "physics.producers.generator.ShowerCopyType: \\"DIRECT\\"" >> {fcl_name}""",
            ]
        )
    elif context.stage.stage_type == DefaultStageTypes.STAGE1:
        # find the first component in the output file path with "reco1" & replace with "larcv"
        larcv_dir = pathlib.PurePosixPath(
            *[p if p != "stage1" else "larcv" for p in context.output_file.parent.parts]
        )
        larcv_filename = larcv_dir / f"larcv_{context.output_file.name}"

        larcv_dir_str = str(larcv_dir)

        fcl_cmd = "\n".join(
            [
                f"mkdir -p {larcv_dir_str}",
                fcl_cmd,
                f'''echo "physics.analyzers.superaMC.out_filename: \\"{larcv_dir_str}/{larcv_filename.name}\\"" >> {fcl_name}''',
                f"""echo "physics.analyzers.superaMC.unique_filename: false" >> {fcl_name}""",
            ]
        )

    return fcl_cmd


mc_runfunc_icarus = functools.partial(
    larsoft_runfunc,
    lar_cmd_func=functools.partial(
        build_larsoft_cmd, calib_ntuple_stage=DefaultStageTypes.STAGE1
    ),
    output_filename_func=functools.partial(
        output_filepath_generic, is_mc=True, use_label=False, include_skip=True
    ),
    fcl_cmd_func=build_modify_fcl_cmd_icarus,
)
icarus_registry.register_runfunc("mc", mc_runfunc_icarus)

data_runfunc_icarus = functools.partial(
    larsoft_runfunc,
    output_filename_func=functools.partial(
        output_filepath_generic, is_mc=False, include_skip=True, blind_caf=True
    ),
)
icarus_registry.register_runfunc("data", data_runfunc_icarus)
