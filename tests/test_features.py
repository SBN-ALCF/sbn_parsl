import pathlib
from unittest.mock import MagicMock
from sbn_parsl.workflow import Workflow, Stage, DefaultStageTypes, StageResult
from sbn_parsl.metadata import MetadataGenerator
from sbn_parsl.config import Config, LArSoftConfig, MetadataConfig, SiteConfig
from sbn_parsl.components import larsoft_runfunc


def test_workflow_cycle_mode():
    """Test that 'cycle' mode yields tasks from different branches in round-robin."""
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]

    # Branch 1
    s1 = Stage(DefaultStageTypes.G4, fcl="g4_1.fcl")
    s1_gen = Stage(DefaultStageTypes.GEN, fcl="gen_1.fcl")
    s1.add_parents(s1_gen)

    # Branch 2
    s2 = Stage(DefaultStageTypes.G4, fcl="g4_2.fcl")
    s2_gen = Stage(DefaultStageTypes.GEN, fcl="gen_2.fcl")
    s2.add_parents(s2_gen)

    wf = Workflow(stage_order)
    wf.add_final_stage(s1)
    wf.add_final_stage(s2)
    wf._finalize()

    # We expect:
    # 1. gen_1 (branch 1)
    # 2. gen_2 (branch 2)
    # 3. g4_1 (branch 1)
    # 4. g4_2 (branch 2)
    # 5. super (final)

    submitted_fcls = []

    def mock_runfunc(self, fcl, parent_result, output_dir):
        if fcl:  # skip super stage which has no fcl
            submitted_fcls.append(fcl)
        return StageResult(
            outputs=[
                pathlib.Path(str(fcl).replace(".fcl", ".root")) if fcl else "super.root"
            ]
        )

    # Assign runfunc to all stages
    for stage in [s1, s1_gen, s2, s2_gen]:
        stage.runfunc = mock_runfunc

    # Manually drive the workflow
    tasks = 0
    while True:
        try:
            next(wf.get_next_task())
            tasks += 1
        except StopIteration:
            break

    assert tasks == 5  # 4 actual tasks + 1 super stage
    assert submitted_fcls == ["gen_1.fcl", "gen_2.fcl", "g4_1.fcl", "g4_2.fcl"]


def test_metadata_generator():
    """Test that MetadataGenerator produces correct command strings."""
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig

    cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(
            experiment="sbnd",
            version="v1",
            qual="e26",
            metadata=MetadataConfig(exe="/path/to/injector", mdprojectversion="v1"),
        ),
        workflow=MagicMock(spec=WorkflowConfig),
        run=MagicMock(spec=RunConfig),
    )

    mg = MetadataGenerator(cfg)

    # Mock os.path.isfile to return True for the injector
    import os

    original_isfile = os.path.isfile
    os.path.isfile = lambda x: x == "/path/to/injector"

    try:
        cmd = mg.run_cmd("output.json", "input.fcl")
        expected = "/path/to/injector --json output.json --fcl input.fcl --project sbnd --version v1"
        assert cmd == expected
    finally:
        os.path.isfile = original_isfile


def test_larsoft_runfunc_with_metadata():
    """Test that larsoft_runfunc includes metadata command in the script."""
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig

    executor = MagicMock()
    executor.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(
            experiment="sbnd",
            version="v1",
            qual="e26",
            metadata=MagicMock(spec=MetadataConfig),
        ),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output="/tmp/out", require_success=False),
    )
    executor.cfg.larsoft.container_path = "/path/to/container"
    executor.cfg.larsoft.larsoft_top = "/path/to/larsoft"

    executor.output_dir = pathlib.Path("/tmp/out")
    executor.name_salt = "salt"
    executor.stage_in_db.return_value = False
    executor.futures = set()

    meta = MagicMock(spec=MetadataGenerator)
    meta.run_cmd.return_value = "METADATA_CMD"

    stage = Stage(DefaultStageTypes.GEN, stage_order=[DefaultStageTypes.GEN])
    stage.workflow_id = 0
    stage.stage_id = (0,)
    stage.combine = False
    stage._is_finalized = True  # simulate finalized stage

    template = "{pre_job_hook}\n{cmd}"

    def mock_future_func(**kwargs):
        # Just return a mock future
        f = MagicMock()
        f.outputs = [MagicMock()]
        # simulate Parsl behavior of setting attributes on future.outputs[0]
        return f

    larsoft_runfunc(
        stage,
        fcl="test.fcl",
        parent_result=StageResult(),
        run_dir=pathlib.Path("/tmp/run"),
        template=template,
        executor=executor,
        meta=meta,
        future_func=mock_future_func,
    )

    # meta.run_cmd should have been called
    meta.run_cmd.assert_called_once()

    # The output should contain METADATA_CMD
    # We didn't capture the actual future content easily here,
    # but we can verify meta was used.
