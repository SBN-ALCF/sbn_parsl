import pytest
import pathlib

from sbn_parsl.workflow import DefaultStageTypes, Stage
from sbn_parsl.components import build_larsoft_cmd, output_filepath, \
        build_modify_fcl_cmd, RunContext


def test_lar_cmd_gen():
    s = Stage(DefaultStageTypes.GEN)
    rc = RunContext(
            stage=s, fcl='gen.fcl', input_files=['input.root'], output_file='output.root',
            lar_args={'nevts': 1}
    )
    assert build_larsoft_cmd(rc) == \
            'lar -c gen.fcl -s input.root --output=output.root --nevts=1'

def test_output_filepath():
    s = Stage(DefaultStageTypes.RECO2)

    # normally these are set by the workflow
    s.workflow_id = 0
    s.stage_id = (0,)

    rc = RunContext(
            stage=s, fcl=pathlib.Path('reco2.fcl'), input_files=['reco1.root'], output_file=None,
            label='var'
    )

    assert output_filepath(rc) == \
            pathlib.PurePosixPath('var/reco2/000000/000000/reco2-var-a80a-af8f-3107-9248.root')

def test_build_modify_fcl_cmd():
    s = Stage(DefaultStageTypes.GEN)

    # normally these are set by the workflow
    s.workflow_id = 0
    s.stage_id = (0,)

    rc = RunContext(
            stage=s, fcl=pathlib.Path('gen.fcl')
    )

    assert build_modify_fcl_cmd(rc) == \
            r'''echo "source.firstRun: 1" >> gen.fcl
echo "source.firstSubRun: 0" >> gen.fcl
echo "physics.producers.generator.FluxSearchPaths: \"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFiles/\"" >> gen.fcl
echo "physics.producers.corsika.ShowerInputFiles: [ \"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\" ]" >> gen.fcl
echo "physics.producers.corsika.ShowerCopyType: \"DIRECT\"" >> gen.fcl'''
