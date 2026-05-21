import pathlib
from unittest.mock import MagicMock

from sbn_parsl.workflow import DefaultStageTypes, Stage
from sbn_parsl.components import (
    build_larsoft_cmd,
    output_filepath,
    build_modify_fcl_cmd,
    RunContext,
)
from sbn_parsl.config import Config, LArSoftConfig, SiteConfig, RunConfig


def get_mock_cfg():
    cfg = MagicMock(spec=Config)
    cfg.site = MagicMock(spec=SiteConfig)
    cfg.larsoft = MagicMock(spec=LArSoftConfig)
    cfg.larsoft.simulation_inputs = (
        "/lus/flare/projects/neutrinoGPU/simulation_inputs_striped"
    )
    cfg.larsoft.container_path = "/path/to/container"
    cfg.larsoft.larsoft_top = "/path/to/larsoft"
    cfg.run = MagicMock(spec=RunConfig)
    cfg.run.require_success = True
    return cfg


def test_lar_cmd_gen():
    s = Stage(DefaultStageTypes.GEN, stage_order=[DefaultStageTypes.GEN])
    s._is_finalized = True  # Bypass ancestry check

    cfg = get_mock_cfg()
    lar_args = LArSoftConfig(experiment="sbnd", version="v1", qual="e26:prof", nevts=1)

    rc = RunContext(
        stage=s,
        cfg=cfg,
        fcl=pathlib.Path("gen.fcl"),
        input_files=[pathlib.Path("input.root")],
        output_file=pathlib.Path("output.root"),
        lar_args=lar_args,
    )
    assert (
        build_larsoft_cmd(rc)
        == "lar -c gen.fcl -s input.root --output=output.root --nevts=1 --tmpdir=/tmp"
    )


def test_output_filepath():
    s = Stage(
        DefaultStageTypes.RECO2,
        stage_order=[DefaultStageTypes.RECO1, DefaultStageTypes.RECO2],
    )

    # normally these are set by the workflow
    s.workflow_id = 0
    s.stage_id = (0,)
    s._is_finalized = True

    cfg = get_mock_cfg()
    lar_args = LArSoftConfig(experiment="sbnd", version="v1", qual="e26:prof")

    rc = RunContext(
        stage=s,
        cfg=cfg,
        fcl=pathlib.Path("reco2.fcl"),
        input_files=[pathlib.Path("reco1.root")],
        output_file=pathlib.Path(""),
        label="var",
        lar_args=lar_args,
    )

    assert output_filepath(rc) == pathlib.PurePosixPath(
        "var/reco2/000000/000000/reco2-reco1.root"
    )


def test_build_modify_fcl_cmd():
    s = Stage(DefaultStageTypes.GEN)

    # normally these are set by the workflow
    s.workflow_id = 0
    s.stage_id = (0,)
    s._is_finalized = True

    cfg = get_mock_cfg()
    lar_args = LArSoftConfig(
        experiment="sbnd",
        version="v1",
        qual="e26:prof",
        nevts=1,
        flux_path="fluxFiles/bnb/G4BNB/v1.1.1/fhc/a",
    )

    rc = RunContext(stage=s, cfg=cfg, fcl=pathlib.Path("gen.fcl"), lar_args=lar_args)

    # Need to verify the output matches what the new generic function or sbnd function does.
    # We are testing build_modify_fcl_cmd (base function)
    expected = "\n".join(
        [
            'echo "" >> gen.fcl',
            'echo "source.firstRun: 1" >> gen.fcl',
            'echo "source.firstSubRun: 0" >> gen.fcl',
            'echo "source.firstEvent: 1" >> gen.fcl',
            """echo "physics.producers.generator.FluxSearchPaths: \\"/lus/flare/projects/neutrinoGPU/simulation_inputs_striped/fluxFiles/bnb/G4BNB/v1.1.1/fhc/a/\\"" >> gen.fcl""",
            """echo "physics.producers.generator.FluxFiles: [ \\"NuBeam_production_BooNE_50m_I174000A_*.dk2nu.root\\" ]" >> gen.fcl""",
            """echo "physics.producers.generator.FluxType:  \\"dk2nu\\"" >> gen.fcl""",
            """echo "physics.producers.corsika.ShowerInputFiles: [ \\"/lus/flare/projects/neutrinoGPU/simulation_inputs_striped/CorsikaDBFiles/p_showers_*.db\\" ]" >> gen.fcl""",
            """echo "physics.producers.corsika.ShowerCopyType: \\"DIRECT\\"" >> gen.fcl""",
        ]
    )

    assert build_modify_fcl_cmd(rc) == expected
