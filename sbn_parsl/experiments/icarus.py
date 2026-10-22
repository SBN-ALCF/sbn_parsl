import functools
from sbn_parsl.workflow import DefaultStageTypes
from sbn_parsl.components import (
    larsoft_runfunc,
    build_larsoft_cmd,
    output_filepath_generic,
    RunContext,
    sibling_output_dir,
    local_buffer_path,
    local_discard_path,
)
from sbn_parsl.experiments.registry import icarus_registry


# @icarus_registry.register_fcl_modifier("mc")
def build_modify_fcl_cmd_icarus(context: RunContext, is_mc: bool=False, save_larcv: bool=True):
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
    elif context.stage.stage_type.name == 'pot':
        fcl_cmd = '\n'.join([
            f'''echo "physics.producers.bnbinfo.URL: \\"http://localhost:8000/ifbeam\\"" >> {fcl_name}''',
        ])
    elif context.stage.stage_type == DefaultStageTypes.STAGE1:
        # written to the node-local buffer, then copied to <out>/larcv/...
        larcv_name = f"larcv_{context.output_file.name}"
        if save_larcv:
            larcv_filename = local_buffer_path(
                context, sibling_output_dir(context, "larcv") / larcv_name
            )
        else:
            larcv_filename = local_discard_path(larcv_name)

        larcv_dir_str = str(larcv_filename.parent)

        supera_module_name = 'supera'
        if is_mc:
            supera_module_name = 'superaMC'
        fcl_cmd = "\n".join(
            [
                f"mkdir -p {larcv_dir_str}",
                fcl_cmd,
                f"""echo "physics.analyzers.{supera_module_name}.out_filename: \\"{larcv_dir_str}/{larcv_filename.name}\\"" >> {fcl_name}""",
                f"""echo "physics.analyzers.{supera_module_name}.unique_filename: false" >> {fcl_name}""",
                f"""echo "outputs.out1.outputCommands: [ @sequence::outputs.out1.outputCommands," >> {fcl_name}""",
                f"""echo "\\"drop recob::SpacePoints_cluster3DCryo*_*_*\\"," >> {fcl_name}""",
                f"""echo "\\"drop recob::Hitrecob::SpacePointvoidart::Assns_cluster3DCryo*_*_*\\" ]" >> {fcl_name}"""
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
    fcl_cmd_func=functools.partial(build_modify_fcl_cmd_icarus, is_mc=True),
)
icarus_registry.register_runfunc("mc", mc_runfunc_icarus)


data_runfunc_icarus = functools.partial(
    larsoft_runfunc,
    lar_cmd_func=functools.partial(
        build_larsoft_cmd, calib_ntuple_stage=DefaultStageTypes.STAGE1
    ),
    output_filename_func=functools.partial(
        output_filepath_generic, is_mc=False, include_skip=True, blind_caf=True
    ),
    fcl_cmd_func=functools.partial(build_modify_fcl_cmd_icarus, is_mc=False),
)
icarus_registry.register_runfunc("data", data_runfunc_icarus)


overlay_runfunc_icarus = functools.partial(
    larsoft_runfunc,
    lar_cmd_func=functools.partial(
        build_larsoft_cmd, calib_ntuple_stage=DefaultStageTypes.STAGE1
    ),
    output_filename_func=functools.partial(
        output_filepath_generic, is_mc=False, use_label=False, include_skip=True
    ),
    fcl_cmd_func=functools.partial(build_modify_fcl_cmd_icarus, is_mc=True),
)
icarus_registry.register_runfunc("overlay", mc_runfunc_icarus)
