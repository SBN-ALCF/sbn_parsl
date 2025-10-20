import pytest
import pathlib

from sbnd_parsl.workflow import DefaultStageTypes, Stage
from sbnd_parsl.components import build_larsoft_cmd, output_filepath, \
        build_modify_fcl_cmd


def test_lar_cmd_gen():
    s = Stage(DefaultStageTypes.GEN)
    assert build_larsoft_cmd(s, 'gen.fcl', ['input.root'], 'output.root', {'nevts': 1}) == \
            'lar -c gen.fcl -s input.root --output=output.root --nevts=1'

def test_output_filepath():
    s = Stage(DefaultStageTypes.RECO2)

    # normally these are set by the workflow
    s.workflow_id = 0
    s.stage_id = (0,)

    assert output_filepath(s, 'reco1.root', 'reco2.fcl', 'var') == \
            pathlib.PurePosixPath('var/reco2/000000/000000/reco2-var-a80a-af8f-3107-9248.root')

def test_build_modify_fcl_cmd():
    s = Stage(DefaultStageTypes.GEN)

    # normally these are set by the workflow
    s.workflow_id = 0
    s.stage_id = (0,)

    assert build_modify_fcl_cmd(s, 'gen.fcl') == \
            r'''echo "source.firstRun: 1" >> gen.fcl
echo "source.firstSubRun: 0" >> gen.fcl
echo "physics.producers.generator.FluxSearchPaths: \"/lus/flare/projects/neutrinoGPU/simulation_inputs/FluxFiles/\"" >> gen.fcl
echo "physics.producers.corsika.ShowerInputFiles: [ \"/lus/flare/projects/neutrinoGPU/simulation_inputs/CorsikaDBFiles/p_showers_*.db\" ]" >> gen.fcl
echo "physics.producers.corsika.ShowerCopyType: \"DIRECT\"" >> gen.fcl'''
