import pytest

from sbnd_parsl.workflow import Workflow, Stage, StageType, DefaultStageTypes, run_stage
from sbnd_parsl.workflow import NoStageOrderException, NoFclFileException


def test_stage_init():
    s = Stage(DefaultStageTypes.GEN)
    # should warn, but proceed
    s = Stage(StageType('MyType'))

def test_stage_add_parent_no_order():
    # raises: can't add a parent if stage order is not specified
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]
    s1 = Stage(DefaultStageTypes.G4)
    s2 = Stage(DefaultStageTypes.GEN)
    s1.add_parents(s2)

    with pytest.raises(NoStageOrderException):
        s1._finalize()

def test_stage_add_parent_last_stage():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]
    workflow = Workflow(stage_order)

    s1 = Stage(DefaultStageTypes.G4)
    workflow.add_final_stage(s1)

    s2 = Stage(DefaultStageTypes.GEN)
    print(s1.stage_type, stage_order)
    
    # OK: s2 is the first stage in the order, so we don't need to know
    # the fcl files for its parents
    s1.add_parents(s2)
    workflow._finalize()

def test_stage_add_parent_not_last_stage():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM]
    workflow = Workflow(stage_order)

    s1 = Stage(DefaultStageTypes.DETSIM)
    workflow.add_final_stage(s1)

    s2 = Stage(DefaultStageTypes.G4)
    
    # Bad: s2 is not the first stage in the order, so we might have to generate
    # its parent. Fcl dict argument is required here
    s1.add_parents(s2)
    with pytest.raises(NoFclFileException):
        workflow._finalize()

def test_stage_add_parent_not_last_stage2():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM]
    s1 = Stage(DefaultStageTypes.DETSIM, stage_order=stage_order)
    s2 = Stage(DefaultStageTypes.G4)
    fcls = {DefaultStageTypes.GEN: 'gen.fcl', DefaultStageTypes.G4: 'g4.fcl', DefaultStageTypes.DETSIM: 'detsim.fcl'}
    
    # OK: s2 is not the first stage in the order, but we provide fcl dict
    s1.add_parents(s2, fcls)

def test_run_stage_no_fcl():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM]
    s1 = Stage(DefaultStageTypes.DETSIM, stage_order=stage_order)
    with pytest.raises(NoFclFileException):
        next(run_stage(s1))

def test_run_stage():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM]
    fcls = {DefaultStageTypes.GEN: 'gen.fcl', DefaultStageTypes.G4: 'g4.fcl', DefaultStageTypes.DETSIM: 'detsim.fcl'}
    workflow = Workflow(stage_order, default_fcls=fcls)
    s1 = Stage(DefaultStageTypes.DETSIM)
    workflow.add_final_stage(s1)
    workflow._finalize()
    while True:
        try:
            next(workflow.get_next_task())
        except StopIteration:
            break

def test_combine():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.DETSIM]
    fcls = {DefaultStageTypes.GEN: 'gen.fcl', DefaultStageTypes.G4: 'g4.fcl', DefaultStageTypes.DETSIM: 'detsim.fcl'}
    workflow = Workflow(stage_order, fcls)

    s1 = Stage(DefaultStageTypes.DETSIM)
    workflow.add_final_stage(s1)

    s2 = Stage(DefaultStageTypes.G4)
    s3 = Stage(DefaultStageTypes.GEN)

    # combine: when we call next() below, we should get all stages executed
    # instead of 1 per next() call since all stages are marked as combine
    s1.combine = True
    s2.combine = True
    s3.combine = True

    s2.add_parents(s3, fcls)
    s1.add_parents(s2, fcls)
    workflow._finalize()
    runs = 0
    while True:
        try:
            print('start loop')
            next(workflow.get_next_task())
            runs += 1
        except StopIteration:
            break

    assert runs == 1
