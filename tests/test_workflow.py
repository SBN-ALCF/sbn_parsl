import pytest

from sbn_parsl.workflow import Workflow, Stage, StageType, DefaultStageTypes, run_stage
from sbn_parsl.workflow import NoStageOrderException, NoFclFileException


def test_stage_init():
    Stage(DefaultStageTypes.GEN)
    # should warn, but proceed
    Stage(StageType("MyType"))


def test_stage_add_parent_no_order():
    # raises: can't add a parent if stage order is not specified
    s1 = Stage(DefaultStageTypes.G4, fcl="g4.fcl")
    s2 = Stage(DefaultStageTypes.GEN, fcl="gen.fcl")
    s1.add_parents(s2)

    with pytest.raises(NoStageOrderException):
        s1._finalize()


def test_stage_add_parent_last_stage():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]
    workflow = Workflow(stage_order)

    s1 = Stage(DefaultStageTypes.G4, fcl="g4.fcl")
    workflow.add_final_stage(s1)

    s2 = Stage(DefaultStageTypes.GEN, fcl="gen.fcl")

    # OK: s2 is the first stage in the order, so we don't need to know
    # the fcl files for its parents
    s1.add_parents(s2)
    workflow._finalize()


def test_stage_add_parent_not_last_stage():
    stage_order = [
        DefaultStageTypes.GEN,
        DefaultStageTypes.G4,
        DefaultStageTypes.DETSIM,
    ]
    workflow = Workflow(stage_order)

    s1 = Stage(DefaultStageTypes.DETSIM, fcl="detsim.fcl")
    workflow.add_final_stage(s1)

    s2 = Stage(DefaultStageTypes.G4, fcl="g4.fcl")

    # Bad: s2 is not the first stage in the order, so we might have to generate
    # its parent. Fcl dict argument is required here (now supplied by Workflow default_fcls)
    s1.add_parents(s2)
    workflow._finalize()
    with pytest.raises(NoFclFileException):
        next(workflow.get_next_task())


def test_stage_add_parent_not_last_stage2():
    stage_order = [
        DefaultStageTypes.GEN,
        DefaultStageTypes.G4,
        DefaultStageTypes.DETSIM,
    ]
    s1 = Stage(DefaultStageTypes.DETSIM, stage_order=stage_order, fcl="detsim.fcl")
    s2 = Stage(DefaultStageTypes.G4, fcl="g4.fcl")
    fcls = {
        DefaultStageTypes.GEN: "gen.fcl",
        DefaultStageTypes.G4: "g4.fcl",
        DefaultStageTypes.DETSIM: "detsim.fcl",
    }

    # OK: s2 is not the first stage in the order, but we provide fcl dict during finalize
    s1.add_parents(s2)
    s1.stage_id = tuple()
    s1._finalize(fcls)


def test_run_stage_no_fcl():
    stage_order = [
        DefaultStageTypes.GEN,
        DefaultStageTypes.G4,
        DefaultStageTypes.DETSIM,
    ]
    s1 = Stage(DefaultStageTypes.DETSIM, stage_order=stage_order)
    s1.stage_id = tuple()
    with pytest.raises(NoFclFileException):
        s1._finalize()
        next(run_stage(s1))


def test_run_stage():
    stage_order = [
        DefaultStageTypes.GEN,
        DefaultStageTypes.G4,
        DefaultStageTypes.DETSIM,
    ]
    fcls = {
        DefaultStageTypes.GEN: "gen.fcl",
        DefaultStageTypes.G4: "g4.fcl",
        DefaultStageTypes.DETSIM: "detsim.fcl",
    }
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
    stage_order = [
        DefaultStageTypes.GEN,
        DefaultStageTypes.G4,
        DefaultStageTypes.DETSIM,
    ]
    fcls = {
        DefaultStageTypes.GEN: "gen.fcl",
        DefaultStageTypes.G4: "g4.fcl",
        DefaultStageTypes.DETSIM: "detsim.fcl",
    }
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

    s2.add_parents(s3)
    s1.add_parents(s2)
    workflow._finalize()
    runs = 0
    while True:
        try:
            next(workflow.get_next_task())
            runs += 1
        except StopIteration:
            break

    # run_stage yields once per task submitted. Every stage here is combined, so
    # nothing is submitted at all and the single advance is the super stage
    # reporting the workflow finished.
    assert runs == 1


def test_is_first():
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]
    workflow = Workflow(stage_order)

    s1 = Stage(DefaultStageTypes.G4, fcl="g4.fcl")
    workflow.add_final_stage(s1)

    s2 = Stage(DefaultStageTypes.GEN, fcl="gen.fcl")
    s1.add_parents(s2)

    # Before finalize, is_first raises RuntimeError
    with pytest.raises(RuntimeError):
        s2.is_first()

    workflow._finalize()

    assert s2.is_first() is True
    assert s1.is_first() is False


def _futures_per_advance(combined):
    """
    Drive a gen->g4->detsim->reco1->reco2->caf chain and report how many tasks
    each next() on the workflow submits, with `combined` stages marked combine.
    """
    from sbn_parsl.workflow import StageResult

    order = [
        DefaultStageTypes.GEN,
        DefaultStageTypes.G4,
        DefaultStageTypes.DETSIM,
        DefaultStageTypes.RECO1,
        DefaultStageTypes.RECO2,
        DefaultStageTypes.CAF,
    ]
    submitted = []

    def runfunc(self, fcl, parent_result, output_dir):
        out = f"{self.stage_type.name}.root"
        if self.combine:
            # a combined stage hands its command to its child, it submits nothing
            return StageResult(outputs=[out], command="cmd")
        submitted.append(self.stage_type.name)
        return StageResult(outputs=[out])

    workflow = Workflow(
        order, {k: f"{k.name}.fcl" for k in order}, runfunc=runfunc
    )
    caf = Stage(DefaultStageTypes.CAF, fcl="caf.fcl")
    caf.runfunc = runfunc
    workflow.add_final_stage(caf)

    stages = {"caf": caf}
    prev = caf
    for stage_type, name in (
        (DefaultStageTypes.RECO2, "reco2"),
        (DefaultStageTypes.RECO1, "reco1"),
        (DefaultStageTypes.DETSIM, "detsim"),
        (DefaultStageTypes.G4, "g4"),
        (DefaultStageTypes.GEN, "gen"),
    ):
        s = Stage(stage_type)
        prev.add_parents(s)
        stages[name] = s
        prev = s
    for name in combined:
        stages[name].combine = True
    workflow._finalize()

    per_advance = []
    while True:
        before = len(submitted)
        try:
            next(workflow.get_next_task())
        except StopIteration:
            break
        per_advance.append(len(submitted) - before)
    return per_advance, submitted


@pytest.mark.parametrize(
    "combined",
    [(), ("gen",), ("g4",), ("detsim",), ("reco1",), ("reco2",),
     ("g4", "reco2"), ("gen", "g4"), ("detsim", "reco2")],
)
def test_one_task_submitted_per_advance(combined):
    """
    run_stage must yield once per submitted task, whatever the combine layout.

    Guarding the ancestor yield on combine instead made a combined stage swallow
    its whole ancestry's yields, so a single advance submitted every stage
    beneath it. That inflated the futures the executor holds per workflow and
    left most of them blocked on dependencies, capping worker occupancy.
    """
    per_advance, submitted = _futures_per_advance(combined)
    assert max(per_advance) <= 1, (
        f"combine={combined} submitted {max(per_advance)} tasks in one advance: "
        f"{per_advance}"
    )
    # combined stages fold into their child, so they never submit
    expected = [s for s in ("gen", "g4", "detsim", "reco1", "reco2", "caf")
                if s not in combined]
    assert submitted == expected


@pytest.mark.parametrize("combined", [(), ("gen",), ("g4",), ("g4", "reco2")])
def test_no_advance_is_wasted(combined):
    """
    Only the final advance may submit nothing -- that one is the super stage
    reporting the workflow finished. A combined stage submits nothing, so it
    must not yield for its own run either, or the executor burns a trip through
    its round-robin on a workflow that handed it no task.
    """
    per_advance, _ = _futures_per_advance(combined)
    assert per_advance[-1] == 0, "expected a trailing super-stage advance"
    assert 0 not in per_advance[:-1], (
        f"combine={combined} wasted an advance: {per_advance}"
    )
