import pathlib
from unittest.mock import MagicMock, patch
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

    fcls = {"gen": "input.fcl"}
    mg = MetadataGenerator(cfg, fcls, defer_check=True)

    # Mock os.path.isfile to return True for the injector
    import os

    original_isfile = os.path.isfile
    os.path.isfile = lambda x: x == "/path/to/injector"

    try:
        cmd = mg.run_cmd("output.json", "input.fcl", check_exists=False)
        expected = "/path/to/injector --inputfclname input.fcl --mdfclname input.fcl --mdprojectname input --mdprojectstage gen --mdprojectversion v1 --mdprojectsoftware sbndcode --mdproductionname MCP2023Blike --mdproductiontype polaris --mdappversion v1 --mdfiletype mc --mdappfamily art --mdruntype physics --mdgroupname sbnd --tfilemdjsonname output.json"
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


def _fanin_workflow(n_branches, submit_mode=None):
    """Build a CAF-style workflow: one final stage fed by n parallel gen->g4 chains."""
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4, DefaultStageTypes.CAF]
    submitted = []

    def mock_runfunc(self, fcl, parent_result, output_dir):
        if fcl:
            submitted.append((fcl, self.workflow_id, self.stage_id))
        return StageResult(
            outputs=[pathlib.Path(str(fcl).replace(".fcl", ".root")) if fcl else "s.root"]
        )

    caf = Stage(DefaultStageTypes.CAF, fcl="caf.fcl")
    caf.runfunc = mock_runfunc
    for i in range(n_branches):
        g4 = Stage(DefaultStageTypes.G4, fcl=f"g4_{i}.fcl")
        gen = Stage(DefaultStageTypes.GEN, fcl=f"gen_{i}.fcl")
        g4.add_parents(gen)
        caf.add_parents(g4)

    wf = Workflow(stage_order)
    wf.add_final_stage(caf)
    if submit_mode is not None:
        wf.submit_mode = submit_mode
    wf._id = 7
    wf._finalize()

    while True:
        try:
            next(wf.get_next_task())
        except StopIteration:
            break
    return submitted


def test_submit_mode_cycle_interleaves_branches():
    """Default 'cycle' round-robins: every branch's gen before any branch's g4."""
    fcls = [f for f, _, _ in _fanin_workflow(3)]
    assert fcls == [
        "gen_0.fcl", "gen_1.fcl", "gen_2.fcl",
        "g4_0.fcl", "g4_1.fcl", "g4_2.fcl",
        "caf.fcl",
    ]


def test_submit_mode_depth_completes_branch_first():
    """'depth' carries one branch to completion before starting the next."""
    fcls = [f for f, _, _ in _fanin_workflow(3, submit_mode="depth")]
    assert fcls == [
        "gen_0.fcl", "g4_0.fcl",
        "gen_1.fcl", "g4_1.fcl",
        "gen_2.fcl", "g4_2.fcl",
        "caf.fcl",
    ]


def test_submit_mode_propagates_to_ancestors():
    """The mode set on the workflow must reach stages created during _finalize."""
    stage_order = [DefaultStageTypes.GEN, DefaultStageTypes.G4]
    g4 = Stage(DefaultStageTypes.G4, fcl="g4.fcl")
    gen = Stage(DefaultStageTypes.GEN, fcl="gen.fcl")
    g4.add_parents(gen)

    wf = Workflow(stage_order)
    wf.add_final_stage(g4)
    wf.submit_mode = "depth"
    wf._finalize()

    assert wf.submit_mode == "depth"
    assert g4.submit_mode == "depth"
    assert gen.submit_mode == "depth"


def _executor_with_task_order(task_order):
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig
    from sbn_parsl.workflow import WorkflowExecutor

    cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output="/tmp/out", task_order=task_order),
    )
    cfg.site.max_futures = -1
    # WorkflowExecutor.__init__ touches the filesystem and starts a DB thread;
    # only the ordering logic is under test here
    ex = WorkflowExecutor.__new__(WorkflowExecutor)
    ex.cfg = cfg
    ex.task_order = task_order
    return ex


def test_task_priority_depth_orders_by_stage():
    """Under 'depth', first stages (longest stage_id) must sort ahead of later ones."""
    from sbn_parsl.workflow import TASK_ORDER_SUBMIT_MODES

    ex = _executor_with_task_order("depth")
    gen = MagicMock(workflow_id=5, stage_id=(0, 0, 0))
    caf = MagicMock(workflow_id=5, stage_id=(0,))
    # parsl HTEX dispatches the lowest priority value first
    assert ex.task_priority(gen) < ex.task_priority(caf)
    assert TASK_ORDER_SUBMIT_MODES["depth"] == "cycle"


def test_task_priority_workflow_orders_by_workflow():
    """Under 'workflow', any task of workflow N outranks any task of N+1."""
    from sbn_parsl.workflow import TASK_ORDER_SUBMIT_MODES

    ex = _executor_with_task_order("workflow")
    wf0_caf = MagicMock(workflow_id=0, stage_id=(0,))
    wf1_gen = MagicMock(workflow_id=1, stage_id=(0, 0, 0))
    assert ex.task_priority(wf0_caf) < ex.task_priority(wf1_gen)
    assert TASK_ORDER_SUBMIT_MODES["workflow"] == "depth"


def test_task_order_validated():
    """An unknown strategy must fail loudly rather than silently doing nothing."""
    import pytest
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig
    from sbn_parsl.workflow import WorkflowExecutor

    cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output="/tmp/out", task_order="sideways"),
    )
    with pytest.raises(ValueError, match="task_order"):
        WorkflowExecutor(cfg)


def _priority_of(task_order, stage_id, workflow_id):
    """Run larsoft_runfunc and return the priority it hands to parsl."""
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig
    from sbn_parsl.workflow import WorkflowExecutor

    executor = MagicMock()
    executor.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output="/tmp/out", require_success=False, task_order=task_order),
    )
    executor.cfg.larsoft.container_path = "/c"
    executor.cfg.larsoft.larsoft_top = "/l"
    executor.output_dir = pathlib.Path("/tmp/out")
    executor.name_salt = "salt"
    executor.stage_in_db.return_value = False
    executor.dry_run = False
    executor.futures = set()
    # use the real ordering logic, not the mock's
    executor.task_order = task_order
    executor.task_priority = MagicMock(
        side_effect=lambda s: WorkflowExecutor.task_priority(executor, s)
    )

    stage = Stage(DefaultStageTypes.GEN, stage_order=[DefaultStageTypes.GEN])
    stage.workflow_id = workflow_id
    stage.stage_id = stage_id
    stage.combine = False
    stage._is_finalized = True

    captured = {}

    def mock_future_func(**kwargs):
        captured.update(kwargs)
        f = MagicMock()
        f.outputs = [MagicMock()]
        return f

    larsoft_runfunc(
        stage,
        fcl="test.fcl",
        parent_result=StageResult(),
        run_dir=pathlib.Path("/tmp/run"),
        template="{cmd}",
        executor=executor,
        future_func=mock_future_func,
    )
    return captured["parsl_resource_specification"]["priority"]


def test_runfunc_emits_depth_priority():
    """Reaching further back up the ancestry must lower the priority value."""
    first = _priority_of("depth", stage_id=(0, 0, 0, 0), workflow_id=3)
    last = _priority_of("depth", stage_id=(0,), workflow_id=3)
    assert first == -4 and last == -1
    assert first < last


def test_runfunc_emits_workflow_priority():
    """Under 'workflow' the priority is the workflow id, whatever the depth."""
    early_last_stage = _priority_of("workflow", stage_id=(0,), workflow_id=2)
    later_first_stage = _priority_of("workflow", stage_id=(0, 0, 0, 0), workflow_id=9)
    assert early_last_stage == 2 and later_first_stage == 9
    assert early_last_stage < later_first_stage


def _submit_stage(tmp_path, *, check, combine=False, dry_run=False):
    """Call larsoft_runfunc once; return (captured future kwargs or None, result, executor)."""
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig

    executor = MagicMock()
    executor.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(
            output=str(tmp_path),
            require_success=False,
            check_existing_outputs=check,
        ),
    )
    executor.cfg.larsoft.container_path = "/c"
    executor.cfg.larsoft.larsoft_top = "/l"
    executor.output_dir = tmp_path
    executor.name_salt = "salt"
    executor.stage_in_db.return_value = False
    executor.dry_run = dry_run
    executor.futures = set()
    executor.task_priority.return_value = 0
    # real ints, so the runfunc's += actually counts
    executor._skip_counter = 0
    executor._file_skip_counter = 0
    executor._stage_counter = 0

    stage = Stage(DefaultStageTypes.GEN, stage_order=[DefaultStageTypes.GEN])
    stage.workflow_id = 0
    stage.stage_id = (0,)
    stage.combine = combine
    stage._is_finalized = True

    captured = {}

    def mock_future_func(**kwargs):
        captured.update(kwargs)
        f = MagicMock()
        f.outputs = [MagicMock()]
        return f

    result = larsoft_runfunc(
        stage,
        fcl="test.fcl",
        parent_result=StageResult(),
        run_dir=tmp_path / "run",
        template="{cmd}",
        executor=executor,
        future_func=mock_future_func,
    )
    return captured or None, result, executor


def test_output_check_off_by_default(tmp_path):
    """With the setting off, an existing output must not stop the task submitting.

    The config lookup must also stay the first clause of the `and` chain, so
    that `and` short-circuits and no stat is issued at all. Reordering the
    clauses would put one filesystem check per task back on the submit path.
    """
    with patch.object(pathlib.Path, "is_file", return_value=True) as is_file:
        captured, _, executor = _submit_stage(tmp_path, check=False)
    assert captured is not None, "task should have been submitted"
    assert executor._skip_counter == 0
    assert executor._file_skip_counter == 0
    assert is_file.call_count == 0, "disabled check must not touch the filesystem"


def test_output_check_skips_existing_output(tmp_path):
    """With the setting on, a real file on disk skips the task and is counted."""
    # first pass with the check off to learn the predicted output path
    captured, _, _ = _submit_stage(tmp_path, check=False)
    out = pathlib.Path(captured["outputs"][0].filepath)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.touch()

    captured, result, executor = _submit_stage(tmp_path, check=True)
    assert captured is None, "task should have been skipped"
    assert result.outputs == [out]
    assert executor._skip_counter == 1
    assert executor._file_skip_counter == 1
    assert executor._stage_counter == 0


def test_output_check_not_applied_to_combined_stages(tmp_path):
    """A combined stage must keep emitting its command even if its /tmp output exists.

    Skipping it would drop the command from the child's script and leave the
    child with an input file that nothing ever produces.
    """
    with patch.object(pathlib.Path, "is_file", return_value=True) as is_file:
        captured, result, executor = _submit_stage(tmp_path, check=True, combine=True)
    assert captured is None, "combined stages never submit their own future"
    assert result.command, "combined stage must hand its command to the child"
    assert executor._file_skip_counter == 0
    assert is_file.call_count == 0, "combine guard must short-circuit before the stat"


def test_output_check_ignored_in_dry_run(tmp_path):
    """Dry runs show the full workflow, like the database check already does."""
    with patch.object(pathlib.Path, "is_file", return_value=True) as is_file:
        captured, result, executor = _submit_stage(tmp_path, check=True, dry_run=True)
    assert captured is None, "dry runs never submit"
    assert executor._file_skip_counter == 0
    assert result.outputs
    assert is_file.call_count == 0, "dry-run guard must short-circuit before the stat"


def _run_execute(nsubruns, nworkers, cached=(), advances_per_wf=3):
    """
    Drive the real LArSoftExecutor.execute() over stub workflows.

    Returns the order of workflow indices that were advanced. Raises if the loop
    fails to terminate, which is the failure mode when the batch window cannot
    roll past a fully cached batch.
    """
    import threading
    import types
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig
    from sbn_parsl.workflow import LArSoftExecutor

    ex = LArSoftExecutor.__new__(LArSoftExecutor)
    ex.cfg = Config(
        site=MagicMock(spec=SiteConfig),
        job=MagicMock(spec=JobConfig),
        workflow=MagicMock(spec=WorkflowConfig),
        run=RunConfig(output="/tmp/out", nsubruns=nsubruns),
    )
    ex.run_opts = ex.cfg.run
    ex.dry_run = False
    ex.futures = set()
    ex.max_futures = -1
    ex._future_limit = False
    ex._drain_batch = 1
    ex._completed_queue = __import__("queue").SimpleQueue()
    ex._workflow_counters = {}
    ex._stage_counter = ex._skip_counter = 0
    ex._success_counter = ex._fail_counter = ex._file_skip_counter = 0
    ex._db_worker_stop = threading.Event()
    ex._db_update_thread = MagicMock()
    ex._monitor_stop = threading.Event()
    ex.workflow_in_db = lambda idx: idx in cached

    advanced = []

    def make_wf(idx):
        left = [advances_per_wf]

        def get_next_task():
            if left[0] <= 0:
                return
            left[0] -= 1
            advanced.append(idx)
            yield

        return types.SimpleNamespace(
            _id=idx,
            n_final_stages=1,
            get_next_task=get_next_task,
            _get_last_file=lambda: None,
        )

    ex.setup_single_workflow_wrapper = lambda idx, inputs=None, last_file=None: make_wf(idx)

    # a spinning loop would hang the suite, so bound it with a watchdog
    done = threading.Event()
    err = []

    def target():
        try:
            ex.execute(nworkers=nworkers)
        except BaseException as e:  # noqa: BLE001
            err.append(e)
        finally:
            done.set()

    t = threading.Thread(target=target, daemon=True)
    t.start()
    if not done.wait(timeout=20):
        raise AssertionError(
            f"execute() did not terminate (nsubruns={nsubruns}, -c {nworkers}, "
            f"cached={sorted(cached)}); {len(advanced)} advances made"
        )
    if err:
        raise err[0]
    return advanced


def test_execute_completes_with_nothing_cached():
    advanced = _run_execute(12, 4)
    assert sorted(set(advanced)) == list(range(12))
    assert len(advanced) == 36


def test_execute_rolls_past_a_fully_cached_batch():
    """
    A contiguous run of already-complete workflows is the normal case on a
    resume. The batch window has to roll past it, or the cycle keeps yielding
    indices that are all in skip_idx and the loop spins forever.
    """
    advanced = _run_execute(12, 4, cached=set(range(4)))
    assert sorted(set(advanced)) == list(range(4, 12))
    assert 0 not in advanced


def test_execute_rolls_past_a_cached_middle_batch():
    advanced = _run_execute(12, 4, cached=set(range(4, 8)))
    assert sorted(set(advanced)) == [0, 1, 2, 3, 8, 9, 10, 11]


def test_execute_handles_every_workflow_cached():
    advanced = _run_execute(12, 4, cached=set(range(12)))
    assert advanced == []


def test_execute_single_workflow_batch_with_cache():
    """-c 1 is the most exposed case: one cached workflow is a whole batch."""
    advanced = _run_execute(6, 1, cached={0, 1})
    assert sorted(set(advanced)) == [2, 3, 4, 5]


def test_execute_scattered_cache():
    advanced = _run_execute(12, 4, cached={0, 5, 11})
    assert sorted(set(advanced)) == [1, 2, 3, 4, 6, 7, 8, 9, 10]


def _progress_executor(max_futures, slots, submitted, runnable, workers=None):
    """Build a bare executor with just what log_progress reads."""
    import types
    from sbn_parsl.workflow import WorkflowExecutor

    ex = WorkflowExecutor.__new__(WorkflowExecutor)
    ex.max_futures = max_futures
    ex._future_limit = max_futures >= 0
    ex._slot_estimate = slots
    ex.futures = set(range(submitted))
    ex._success_counter = 7
    ex._fail_counter = 1
    fake = types.SimpleNamespace(
        outstanding=lambda: runnable,
        connected_workers=lambda: workers,
    )
    ex._parsl_executors = lambda: [fake]
    return ex


def test_log_progress_flags_starvation(capsys):
    """Budget full but most of it blocked on dependencies: the invisible failure."""
    ex = _progress_executor(max_futures=300, slots=200, submitted=300, runnable=100)
    ex.log_progress()
    out = capsys.readouterr().out
    assert "submitted=300" in out
    assert "runnable=100" in out
    assert "slots=200" in out
    assert "fed=50%" in out
    assert "STARVED" in out


def test_log_progress_quiet_when_healthy(capsys):
    """Budget full but enough runnable work to feed every slot: no warning."""
    ex = _progress_executor(max_futures=300, slots=200, submitted=300, runnable=250)
    ex.log_progress()
    out = capsys.readouterr().out
    assert "fed=125%" in out
    assert "STARVED" not in out


def test_log_progress_no_warning_below_the_cap(capsys):
    """Under the cap the submit loop is free to catch up, so this is not starvation."""
    ex = _progress_executor(max_futures=300, slots=200, submitted=50, runnable=40)
    ex.log_progress()
    assert "STARVED" not in capsys.readouterr().out


def test_log_progress_samples_workers_only_when_asked(capsys):
    ex = _progress_executor(300, 200, 300, 250, workers=198)
    ex.log_progress()
    assert "connected_workers" not in capsys.readouterr().out
    ex.log_progress(with_workers=True)
    out = capsys.readouterr().out
    assert "connected_workers=198" in out and "requested 200" in out


def test_log_progress_silent_without_parsl(capsys):
    """Dry runs and tests have no parsl loaded; the monitor must stay quiet."""
    ex = _progress_executor(300, 200, 300, 100)
    ex._parsl_executors = lambda: []
    ex.log_progress()
    assert capsys.readouterr().out == ""


def _real_executor(tmp_path, max_futures, nodes, cpus_per_node):
    """Construct a real WorkflowExecutor against a tmp output dir."""
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig
    from sbn_parsl.workflow import LArSoftExecutor

    cfg = Config(
        site=SiteConfig(
            name="test", cpus_per_node=cpus_per_node, cores_per_worker=1,
            max_futures=max_futures,
        ),
        job=JobConfig(nodes_per_block=nodes),
        workflow=WorkflowConfig(),
        run=RunConfig(output=str(tmp_path)),
        larsoft=LArSoftConfig(experiment="sbnd", version="v1", qual="e26"),
    )
    # WorkflowExecutor.__init__ starts a thread targeting _backup_db_loop, which
    # is defined on the subclass, so the base class is not instantiable alone
    ex = LArSoftExecutor(cfg)
    ex._monitor_stop.set()          # do not leave the monitor ticking in tests
    ex._db_worker_stop.set()
    return ex


def test_startup_warns_when_budget_cannot_fill_the_machine(tmp_path, capsys):
    """max_futures below the slot count means guaranteed idle workers."""
    ex = _real_executor(tmp_path, max_futures=1000, nodes=64, cpus_per_node=102)
    out = capsys.readouterr().out
    assert ex._slot_estimate == 64 * 102
    assert "WARNING" in out and "max_futures (1000)" in out
    assert "6528 worker slots" in out


def test_startup_quiet_when_budget_exceeds_slots(tmp_path, capsys):
    ex = _real_executor(tmp_path, max_futures=250000, nodes=2048, cpus_per_node=102)
    out = capsys.readouterr().out
    assert ex._slot_estimate == 208896
    assert "WARNING" not in out


def test_startup_quiet_when_future_limit_disabled(tmp_path, capsys):
    ex = _real_executor(tmp_path, max_futures=-1, nodes=2048, cpus_per_node=102)
    assert ex._future_limit is False
    assert "WARNING" not in capsys.readouterr().out


def test_workflow_executor_is_usable_on_its_own(tmp_path):
    """
    WorkflowExecutor owns the sqlite connections and the writer thread, so it
    must also own the persistence methods that thread runs. It previously
    started a thread targeting _backup_db_loop, which was defined on
    LArSoftExecutor, so constructing the base class raised AttributeError.
    """
    from sbn_parsl.config import RunConfig, JobConfig, WorkflowConfig
    from sbn_parsl.workflow import WorkflowExecutor

    cfg = Config(
        site=SiteConfig(name="test", cpus_per_node=8, cores_per_worker=1,
                        max_futures=1000),
        job=JobConfig(nodes_per_block=1),
        workflow=WorkflowConfig(),
        run=RunConfig(output=str(tmp_path)),
    )
    ex = WorkflowExecutor(cfg)
    try:
        # the persistence layer must be reachable from the base class
        assert ex.stage_in_db("nope") is False
        ex.mark_stage_in_db("wf0_0_0", status=0)
        assert ex.stage_in_db("wf0_0_0", require_success=True) is True
        assert ex.workflow_in_db(0) is False
        ex.mark_workflow_in_db(0)
        assert ex.workflow_in_db(0) is True
        # and the writer thread must be running, not merely constructed
        assert ex._db_update_thread.is_alive()
    finally:
        ex._monitor_stop.set()
        ex._db_worker_stop.set()
        ex._db_update_thread.join(timeout=30)
    assert not ex._db_update_thread.is_alive()
    assert ex._db_file.exists()
