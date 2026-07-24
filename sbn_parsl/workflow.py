#!/usr/bin/env python3
"""
Classes for organizing LArSoft workflows for SBND

Reconstructed events either start as raw data or generated MC. Raw data must be
decoded, and generated MC vectors must be simulated at the Geant4 and the
detector response level,

Raw -> Decode
Gen -> G4 -> Detsim

After these steps, data and MC are handled in the same way,
Decode -> Reco1 -> Reco2 -> CAF
Detsim -> Reco1 -> Reco2 -> CAF

For simulating detector systematic variation samples, we can also "scrub" Reco1
MC files to get back to the same generated event:
Reco1 -> Scrub (Gen) -> G4 -> Detsim -> Reco1 -> Reco2 -> CAF

CAF files can also have multiple input Reco2 files
Reco2 + Reco2 + ... -> CAF

These classes implement this structure and generate jobs based on what you have
and what you want, automatically filling in intermediate steps as needed.
"""

import os
import time
import threading
import queue
from types import MethodType
import itertools
import pathlib
from collections import deque
from concurrent.futures import as_completed
import sqlite3
import logging
from enum import Flag, auto
from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Optional, Callable, Any

# don't print error message if job times out
from parsl.executors.high_throughput.errors import ManagerLost as ManagerLostError

from sbn_parsl.config import Config


@dataclass
class StageResult:
    """
    Encapsulates the outputs and dependencies of a workflow stage.

    This object is passed down to child stages so they know where to find their inputs
    and what commands need to be prefixed (e.g., when combining stages).
    """

    outputs: List[Any] = field(default_factory=list)
    dependencies: List[Any] = field(default_factory=list)
    command: str = ""


logger = logging.getLogger(__name__)


class NoInputFileException(Exception):
    """Raised when a stage requires an input file but none was provided."""

    pass


class NoFclFileException(Exception):
    """Raised when a stage requires a FHiCL file but none was provided or found in defaults."""

    pass


class WorkflowException(Exception):
    """General exception for workflow-related errors."""

    pass


class StageAncestorException(Exception):
    """Raised when there is an error in the stage ancestry graph (e.g., invalid parent type)."""

    pass


class NoStageOrderException(Exception):
    """Raised when a stage operation is attempted without a defined stage order."""

    pass


class StageProperty(Flag):
    """
    Bitwise flags representing special behavioral properties of a StageType.

    Attributes:
        NONE: Default properties.
        NO_FCL: Stage does not require a FHiCL file (e.g., SPINE).
        NO_PARENT: Stage is an entry point and has no predecessors.
        NO_INPUT: Stage does not require input files (e.g., GEN).
        _SUPER: Special property for the workflow-terminating super stage.
    """

    NONE = 0
    NO_FCL = auto()
    NO_PARENT = auto()
    NO_INPUT = auto()
    _SUPER = auto()


class StageType:
    """
    Defines the categorical identity and properties of a specific stage.
    Registers itself automatically upon instantiation so it can be looked up
    by string name, supporting both DefaultStageTypes and custom user types.
    """

    _registry: Dict[str, "StageType"] = {}

    def __init__(self, name: str, props: Optional[StageProperty] = StageProperty.NONE):
        """
        Initializes a StageType and registers it.

        Args:
            name: The unique string identifier for the stage type.
            props: Behavioral flags for this stage type.
        """
        self._name = name
        self._props = props
        StageType._registry[name.lower()] = self

    @property
    def properties(self):
        """Returns the StageProperty flags for this type."""
        return self._props

    @property
    def name(self):
        """Returns the string name of this stage type."""
        return self._name

    def __eq__(self, other):
        if isinstance(other, StageType):
            return self._name == other.name and self._props == other.properties
        return NotImplemented

    def __hash__(self):
        """Override so StageTypes can be used as dictionary keys."""
        return hash(self._name)

    @classmethod
    def from_str(cls, name: str) -> "StageType":
        """
        Return the StageType instance with the given name.

        Args:
            name: The string name to look up.

        Returns:
            The matching StageType instance.

        Raises:
            ValueError: If the name is not found in the registry.
        """
        name_lower = name.lower()
        if name_lower in cls._registry:
            return cls._registry[name_lower]
        raise ValueError(
            f"StageType '{name}' not found in registry. "
            f"Available: {list(cls._registry.keys())}"
        )


class DefaultStageTypes:
    """Provide some commonly used standard StageTypes."""

    GEN = StageType("gen", StageProperty.NO_INPUT | StageProperty.NO_PARENT)
    SPINE = StageType("spine", StageProperty.NO_FCL)

    # these are just common stage names, no special properties
    G4 = StageType("g4")
    DETSIM = StageType("detsim")
    RECO1 = StageType("reco1")
    RECO2 = StageType("reco2")
    STAGE0 = StageType("stage0")
    STAGE1 = StageType("stage1")
    DECODE = StageType("decode")
    CAF = StageType("caf")
    SCRUB = StageType("scrub")


# special stage that triggers end-of-workflow actions
_SUPER = StageType("super", StageProperty._SUPER | StageProperty.NO_FCL)


def default_runfunc(
    stage_self, fcl, parent_result: StageResult, output_dir
) -> StageResult:
    """
    Default function called when each stage is run.
    Constructs a basic 'lar -c' command.

    Args:
        stage_self: The Stage object being executed.
        fcl: Path to the FHiCL file to use.
        parent_result: The StageResult from the parent stage(s).
        output_dir: Directory where outputs should be written.

    Returns:
        A StageResult object containing the output root file path.
    """
    input_file_arg_str = ""
    if parent_result.outputs:
        input_file_arg_str = " ".join(
            [f"-s {str(file)}" for file in parent_result.outputs]
        )

    output_filename = os.path.basename(fcl).replace(".fcl", ".root")
    if output_dir is None:
        output_dir = pathlib.Path(".")
    output_file = output_dir / pathlib.Path(output_filename)
    output_file_arg_str = f"--output {str(output_file)}"
    logger.info(f"lar -c {fcl} {input_file_arg_str} {output_file_arg_str}")
    return StageResult(outputs=[output_file])


class Stage:
    """
    A single task or logical step within a computational workflow.
    Tracks ancestry, dependencies, files, and handles execution through `run()`.

    Attributes:
        stage_type: The categorical identity of the stage.
        fcl: Path to the FHiCL file for this stage.
        runfunc: Callable that executes the stage logic.
        run_dir: Directory where the stage is executed.
        workflow_id: ID of the parent workflow.
        stage_id: Unique tuple identifier within the workflow.
        final: Whether this is a final stage in the workflow.
    """

    def __init__(
        self,
        stage_type: StageType,
        fcl: Optional[str] = None,
        runfunc: Optional[Callable] = None,
        stage_order: Optional[List[StageType]] = None,
    ):
        """
        Initializes a Stage.

        Args:
            stage_type: The StageType or string name of the stage.
            fcl: Optional path to the FHiCL file.
            runfunc: Optional callable to execute the stage.
            stage_order: Optional list defining the sequence of stage types.
        """

        if isinstance(stage_type, str):
            stage_type = StageType.from_str(stage_type)

        self._stage_type: StageType = stage_type
        self.fcl = fcl
        self.runfunc = runfunc
        self.run_dir = None

        self.stage_order = stage_order

        self._complete = False
        self._parent_results: List[StageResult] = []
        self._output_result: Optional[StageResult] = None
        self._parents_iterators = deque()
        self._combine = False

        self._workflow_last_file = None

        self._temp_parent_stages = []
        self._fcls = None
        self.workflow_id: int = None
        self.stage_id: Tuple[int] = None
        self.final: bool = False
        self._is_finalized: bool = False

    @property
    def stage_type(self) -> StageType:
        """Returns the StageType of this stage."""
        return self._stage_type

    def is_first(self) -> bool:
        """
        Determines if this stage is the first in its workflow branch.

        Returns:
            True if the stage has no parent type, False otherwise.

        Raises:
            RuntimeError: If called before the stage is finalized.
        """
        if not self._is_finalized:
            raise RuntimeError(
                "Cannot determine if stage is first before it has been finalized."
            )
        return self.parent_type is None

    @property
    def output_files(self) -> List:
        """
        Returns the list of output files produced by this stage.
        Triggers `run()` if the stage has not been executed yet.
        """
        if self._output_result is None:
            print(
                f"Warning: Running stage of type {self.stage_type} via output_files method"
            )
            self.run()
        return self._output_result.outputs

    @property
    def output_result(self) -> StageResult:
        """
        Returns the StageResult object produced by this stage.
        Triggers `run()` if the stage has not been executed yet.
        """
        if self._output_result is None:
            self.run()
        return self._output_result

    @property
    def input_files(self) -> Optional[List]:
        """Returns a flattened list of output files from all parent stages."""
        if not self._parent_results:
            return None
        return [f for r in self._parent_results for f in r.outputs]

    @property
    def parent_type(self) -> Optional[StageType]:
        """
        Return the stage type of the stage before this one in the stage order.

        Returns:
            The parent StageType, or None if this is the first stage.

        Raises:
            NoStageOrderException: If stage_order is not defined.
            StageAncestorException: If the stage type is not found in stage_order.
        """
        if self.stage_order is None:
            raise NoStageOrderException(
                f"No stage order set for stage of type {self.stage_type.name}. Either add this stage to a Workflow or set stage_order at initialization."
            )

        idx = self.stage_order.index(self.stage_type)
        if idx == 0:
            return None

        parent_idx = idx - 1
        if parent_idx < 0:
            raise StageAncestorException(
                f"No ancestor for {self.stage_type} in list {self.stage_order}"
            )

        return self.stage_order[parent_idx]

    def has_parents(self) -> bool:
        """Checks if the stage has any parent iterators still active."""
        return len(self._parents_iterators) > 0

    def parents(self, _type: StageType):
        """Returns the set of unique parent Stage objects matching a given type."""
        return set(s[0] for s in self._parents_iterators if s[0].stage_type == _type)

    @property
    def complete(self) -> bool:
        """Returns True if the stage has finished execution."""
        return self._complete

    def add_input_file(self, file) -> None:
        """
        Manually adds an input file or StageResult as a dependency.

        Args:
            file: A filename string or a StageResult object.
        """
        if isinstance(file, StageResult):
            self._parent_results.append(file)
        else:
            self._parent_results.append(StageResult(outputs=[file]))

    def add_parent_result(self, result: StageResult) -> None:
        """
        Adds a StageResult from a parent stage.

        Args:
            result: The StageResult object to add.
        """
        self._parent_results.append(result)

    @property
    def combine(self) -> bool:
        """Whether to combine this stage's command with its parents' in a single script."""
        return self._combine

    @combine.setter
    def combine(self, val: bool) -> None:
        self._combine = val

    @property
    def stage_id_str(self) -> str:
        """Returns a string representation of the unique stage ID."""
        return f"{self.workflow_id}_" + "_".join(str(c) for c in self.stage_id)

    def run(self, rerun: bool = False) -> None:
        """
        Executes the stage logic using the assigned `runfunc`.

        Args:
            rerun: If True, forces execution even if already complete.

        Raises:
            RuntimeError: If parents are still pending or other logic errors.
            NoFclFileException: If a FHiCL file is required but missing.
            NoInputFileException: If inputs are required but missing.
            TypeError: If runfunc does not return a StageResult.
        """
        if self._output_result is not None and not rerun:
            return

        if self._output_result is None and self.complete:
            return

        if self.has_parents():
            raise RuntimeError(
                f"Attempt to run stage {self._stage_type} while it still holds references to its parents"
            )

        if StageProperty._SUPER in self._stage_type.properties:
            print(
                f"Congratulations, you ran all the stages in workflow {self.workflow_id}!"
            )
            self._complete = True
            return

        if self.fcl is None and StageProperty.NO_FCL not in self._stage_type.properties:
            raise NoFclFileException(
                f"Attempt to run stage {self._stage_type.name} with no fcl provided and no default"
            )

        if StageProperty.NO_INPUT in self._stage_type.properties:
            pass
        else:
            if not self._parent_results:
                raise NoInputFileException(
                    f"Tried to run stage of type {self._stage_type.name} which requires at least one input file, but it was not set."
                )

        self._complete = True
        func = MethodType(self.runfunc, self)

        inputs = []
        depends = []
        cmds = []
        for r in self._parent_results:
            inputs.extend(r.outputs)
            depends.extend(r.dependencies)
            if r.command:
                cmds.append(r.command)

        parent_cmd = "\n".join(cmds)
        parent_result = StageResult(
            outputs=inputs, dependencies=depends, command=parent_cmd
        )

        result = func(self.fcl, parent_result, self.run_dir)

        if not isinstance(result, StageResult):
            raise TypeError(
                f"Runfunc for stage {self.stage_type.name} must return a StageResult object. Got {type(result)}."
            )

        self._output_result = result

    def add_parents(self, stages: Any) -> None:
        """
        Queues parent stages for addition during finalization.

        Args:
            stages: A single Stage or list of Stage objects to add as parents.
        """
        if not isinstance(stages, list):
            stages = [stages]
        self._temp_parent_stages.extend(stages)

    def _finalize(self, fcls: Optional[Dict] = None):
        """
        Internal method to build the ancestry tree and resolve FHiCL files.

        Args:
            fcls: Dictionary mapping StageTypes to default FHiCL files.
        """

        if fcls is not None:
            self._fcls = fcls
        elif self._fcls is None:
            self._fcls = {}

        if self.fcl is None and StageProperty.NO_FCL not in self.stage_type.properties:
            if self._fcls:
                try:
                    self.fcl = self._fcls[self.stage_type]
                except KeyError:
                    self.fcl = self._fcls.get(self.stage_type.name)
            if self.fcl is None:
                raise NoFclFileException(
                    "Tried to run a stage with no fcl file. Either set the stage's fcl file first, or provide it via Workflow default_fcls."
                )

        for s in self._temp_parent_stages:
            self._add_parents(s)
            s._finalize(self._fcls)

        self._temp_parent_stages = []
        self._is_finalized = True

    def _add_parents(self, stages: Any) -> None:
        """
        Internal method to link parent stages and initialize their metadata.

        Args:
            stages: A single Stage or list of Stage objects.
        """
        if not isinstance(stages, list):
            stages = [stages]

        for s in stages:
            # The _SUPER stage can have any stage type from the stage_order as a parent,
            # allowing the workflow to terminate at any point.
            is_super = StageProperty._SUPER in self._stage_type.properties
            if not is_super and s.stage_type != self.parent_type:
                raise StageAncestorException(
                    f"Tried to add stage of type {s.stage_type.name} as a parent to a stage with type {self.stage_type.name}"
                )
            elif is_super and s.stage_type not in self.stage_order:
                raise StageAncestorException(
                    f"Tried to add stage of type {s.stage_type.name} as a parent to the workflow's super stage, but {s.stage_type.name} is not in the workflow's stage order."
                )

            if s.workflow_id is None:
                s.workflow_id = self.workflow_id
            if s.stage_id is None:
                s.stage_id = self.stage_id + (len(self._parents_iterators),)
            if StageProperty._SUPER in self._stage_type.properties:
                s.final = True
            if s.stage_order is None:
                s.stage_order = self.stage_order

            if s.parent_type is not None and not self._fcls and s.fcl is None:
                raise NoFclFileException(
                    "Must specify fcl file dictionary argument when adding a parent stage if the parent is not the first stage."
                )

            if s.run_dir is None:
                s.run_dir = self.run_dir
            if s.runfunc is None:
                s.runfunc = self.runfunc

            self._parents_iterators.append((s, run_stage(s)))

    def get_next_task(self, mode="cycle"):
        """
        Yields execution control to parent stages until they complete.

        Args:
            mode: "cycle" to alternate between parent branches, or "depth"
                  to finish one branch before starting another.
        """
        while self._parents_iterators:
            # remove
            parent, iterator = self._parents_iterators.popleft()
            try:
                next(iterator)
                # put back if not done. Either at the back of the deque or in place
                if mode == "cycle":
                    self._parents_iterators.append((parent, iterator))
                else:
                    self._parents_iterators.appendleft((parent, iterator))
                yield
            except StopIteration:
                self.add_parent_result(parent.output_result)

                if StageProperty._SUPER in self.stage_type.properties:
                    self._workflow_last_file = parent.output_result.outputs


def run_stage(stage: Stage):
    """
    Generator function that executes a stage and its ancestry recursively.

    This function yields Control to the caller each time a new task is
    ready for submission. It ensures that parent stages are fully
    traversed before the child stage executes.

    Args:
        stage: The Stage object to run.

    Raises:
        NoFclFileException: If a FHiCL file is required but missing.
    """

    if stage.complete:
        return

    if stage.fcl is None and StageProperty.NO_FCL not in stage.stage_type.properties:
        raise NoFclFileException(
            "Tried to run a stage with no fcl file. Either set the stage's fcl file first, or pass in a dictionary to Workflow."
        )

    if stage.runfunc is None:
        logger.warning(
            f"No runfunc specified for stage with type {stage.stage_type}. Adding default runfunc"
        )
        stage.runfunc = default_runfunc

    if StageProperty.NO_PARENT in stage.stage_type.properties:
        stage.run()
        yield

    if stage.input_files is not None and not stage.has_parents():
        stage.run()
        yield

    if stage.complete:
        return

    if not stage.has_parents():
        parent_stage = Stage(stage.parent_type)
        stage.add_parents(parent_stage)
        stage._finalize(stage._fcls)

    while stage.has_parents():
        try:
            next(stage.get_next_task())
            if not stage.combine:
                yield
        except StopIteration:
            pass

    stage.run()
    yield


class Workflow:
    """
    A logical collection of `Stage` objects representing a full end-to-end execution path.
    The Workflow class automatically builds the ancestry graph, filling in any gaps
    between the requested final stages and the initial inputs using the provided `stage_order`.
    """

    @staticmethod
    def default_runfunc(
        stage_self, fcl, parent_result: StageResult, output_dir
    ) -> StageResult:
        """
        Default function called when each stage is run.

        Args:
            stage_self: The Stage object.
            fcl: Path to the FHiCL file.
            parent_result: Result from parent stage.
            output_dir: Output directory.

        Returns:
            A StageResult containing the predicted output file.
        """
        input_file_arg_str = ""
        if parent_result.outputs:
            input_file_arg_str = " ".join(
                [f"-s {str(file)}" for file in parent_result.outputs]
            )

        output_filename = os.path.basename(fcl).replace(".fcl", ".root")
        output_file = output_dir / pathlib.Path(output_filename)
        output_file_arg_str = f"--output {str(output_file)}"
        print(f"lar -c {fcl} {input_file_arg_str} {output_file_arg_str}")
        return StageResult(outputs=[output_file])

    def __init__(
        self,
        stage_order: List[StageType],
        default_fcls: Optional[Dict] = None,
        run_dir: pathlib.Path = pathlib.Path(),
        runfunc: Optional[Callable] = None,
    ):
        """
        Initializes a Workflow.

        Args:
            stage_order: List of StageTypes defining the valid ancestry path.
            default_fcls: Mapping of StageType to default FHiCL files.
            run_dir: Directory where tasks will execute.
            runfunc: Default execution function for all stages in the workflow.
        """

        # ID is set by the workflow executor
        self._id = None
        self._n_final_stages = 0

        self._stage_order = stage_order

        self.default_fcls = {}
        if default_fcls is not None:
            for k, v in default_fcls.items():
                if not isinstance(k, StageType):
                    self.default_fcls[StageType.from_str(k)] = v
                else:
                    self.default_fcls[k] = v

        self._run_dir = run_dir
        self._default_runfunc = runfunc
        if self._default_runfunc is None:
            self._default_runfunc = Workflow.default_runfunc
        self._stage = Stage(_SUPER)

        self._stage.run_dir = self._run_dir
        self._stage.runfunc = self._default_runfunc
        self._stage.stage_order = self._stage_order + [_SUPER]
        self._final_stages = []

    def add_final_stage(self, stage: Stage):
        """
        Adds a leaf node to the workflow graph.

        Args:
            stage: The final Stage object to be executed.
        """
        self._final_stages.append(stage)

    def _finalize(self):
        """Internal method to link the super stage to final stages and set IDs."""
        # keep track of where this stage came from
        # super stage always gets Stage ID as the empty string
        self._stage.workflow_id = self._id

        # Do not set combine flag for super stage. If set, the entire workflow
        # will run before the run_stage generator yields, effectively
        # submitting all tasks from the workflow at once. This could be a
        # useful feature, but by default, we want the generator to yield one
        # task from each subrun before cycling back to this workflow
        self._stage.combine = False

        self._stage.stage_id = tuple()

        for s in self._final_stages:
            self._stage.add_parents(s)

            # counter is used by the workflow executor to determine if all the
            # final stages of this workflow have completed
            self._n_final_stages += 1
        self._stage._finalize(self.default_fcls)

        # no need to keep this around
        del self._final_stages

    @property
    def n_final_stages(self):
        """Returns the number of final stages in the workflow."""
        return self._n_final_stages

    def get_next_task(self):
        """
        Generator that yields when a new task is ready for submission.
        """
        try:
            next(run_stage(self._stage))
            yield
        except StopIteration:
            pass

    def _get_last_file(self):
        """Returns the outputs of the last executed stage."""
        return self._stage._workflow_last_file


class WorkflowExecutor:
    """
    Class to wrap settings and run multiple workflow objects.
    Manages the task submission loop, SQLite caching, and resource limiting.

    Attributes:
        cfg: The Config object containing all project settings.
        futures: A set of active Parsl futures.
        max_futures: The maximum number of concurrent futures allowed.
    """

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.run_opts = cfg.run

        self.runinfo_dir = pathlib.Path(cfg.run.runinfo or cfg.run.output)
        self.output_dir = pathlib.Path(cfg.run.output)

        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            if cfg.run.daos:
                daos_container_dir = str(self.output_dir)
                username = os.getenv("USER")
                daos_container_dir = pathlib.Path(
                    daos_container_dir.replace("/tmp", f"/tmp/{username}")
                )
                print(f"{daos_container_dir=}")
                daos_container_dir.mkdir(parents=True, exist_ok=True)
            else:
                raise

        self.max_futures = cfg.site.max_futures
        self._future_limit = True
        if self.max_futures < 0:
            self._future_limit = False

        # for lots of futures (>10k), set is much faster than list)
        self.futures = set()

        self.workflow_opts = cfg.workflow
        self._workflow_counters = {}

        # optionally track the number of submitted stages. Runfunc should modify these
        self._stage_counter = 0
        self._skip_counter = 0
        self._success_counter = 0
        self._fail_counter = 0

        # completed stages tracking for dynamic prioritization
        self._completion_counter = 0
        self._completed_stages_info = {}

        # file tracking with sqlite
        self._db_update_thread = threading.Thread(
            target=self._backup_db_loop, daemon=True
        )
        self._db_worker_stop = threading.Event()
        self._db_event_queue = queue.Queue()
        self._db_lock = threading.Lock()

        self._db_batch_max_size = 5000
        self._db_batch_max_wait = 5.0
        self._db_backup_interval = 300.0
        self._db_update_thread.start()

        # DB file is unique to settings used for the workflow, modulo the job
        # settings and number of subruns which could change on re-runs
        db_suffix = cfg.get_science_hash()

        self._db_file = (
            self.output_dir / "runinfo" / "cmd" / f"file_cache_{db_suffix}.db"
        )
        print(f"Cache will be saved to {self._db_file}")
        self._db_file.parent.mkdir(parents=True, exist_ok=True)

        # Mark that the workflow has actually started executing
        launched_marker = self.runinfo_dir / ".launched"
        launched_marker.touch(exist_ok=True)

        self._disk_db = sqlite3.connect(str(self._db_file), check_same_thread=False)
        self._mem_db = sqlite3.connect(":memory:", check_same_thread=False)
        self._disk_db.backup(self._mem_db)
        self._cursor = self._mem_db.cursor()

        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS stages (
                stage_id TEXT PRIMARY KEY,
                status UNSIGNED INT
            )
        """)

        # this tracks fully completed workflows where all tasks were
        # successful. If the workflow's ID is in this database, we can skip
        # running it completely
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS workflows (
                id UNSIGNED INT PRIMARY KEY
            )
        """)


class LArSoftExecutor(WorkflowExecutor):
    """
    Subclass of WorkflowExecutor specifically for LArSoft-based workflows.
    Handles the parsing of FCL mappings and LArSoft environment configuration.
    """

    def __init__(self, cfg: Config):
        super().__init__(cfg)
        if not hasattr(cfg, "larsoft"):
            raise ValueError(
                "LArSoftExecutor requires a Config object with 'larsoft' settings initialized."
            )

        self.larsoft_opts = cfg.larsoft
        self.name_salt = str(self.output_dir)
        self.fcl_dir = None
        self.fcls = cfg.workflow.fcls
        self.dry_run = False

    def get_run_dir(self, iteration: int) -> pathlib.Path:
        """Default directory structure for standard subruns."""
        from sbn_parsl.utils import get_subrun_dir

        return get_subrun_dir(self.output_dir, iteration)

    def get_caf_dir(self, iteration: int) -> pathlib.Path:
        """Default directory structure for CAF outputs."""
        from sbn_parsl.utils import get_caf_dir

        return get_caf_dir(self.output_dir, iteration)

    def file_generator(self):
        if self.run_opts.file_list:
            with open(self.run_opts.file_list, "r") as f:
                for line in f.readlines():
                    yield pathlib.Path(line.strip())

    def execute(self, nworkers: int = -1, dry_run: bool = False):
        """
        Run many copies of a single workflow. yield each time a stage is
        executed for efficient task submission, i.e., we'll get the first tasks
        from each workflow first, instead of all the tasks from workflow 0,
        then all the tasks from workflow 1, etc.  Use itertools.cycle() to keep
        looping over all workflows until all tasks are submitted.
        If nworkers > 0, tasks will be gotten from <nworkers> workflows first
        before cycling over other workflows
        """
        self.dry_run = dry_run
        nsubruns = self.run_opts.nsubruns
        if self.dry_run:
            print(f"DRY RUN: Processing first {min(nsubruns, 3)} subruns...")
            nsubruns = min(nsubruns, 3)

        file_generator = None
        by_file = False
        if self.run_opts.files_per_subrun is not None:
            # each subrun processes a slice of files
            file_generator = self.file_generator()
            by_file = True

        # generator madness...
        # we'll cycle over indices until all tasks are submitted, taking one
        # task from each subrun at a time. This ensures we get the parsl
        # futures in the "correct" order: Futures without dependencies first,
        # then futures with dependencies later

        wfs = [None] * nsubruns
        skip_idx = set()
        idx_cycle = itertools.cycle(range(nsubruns))

        # another layer: Instead of cycling over all subruns, cycle in batches
        # with a batch size = to the number of workers. This ensures the workers
        # always have tasks to start but also don't have to wait for all subruns
        # to complete their first stage before moving onto their later stages
        if nworkers > 0:
            nworkers = min(nworkers, nsubruns)
            idx_cycle = itertools.cycle(range(nworkers))
        else:
            nworkers = nsubruns

        last_files = [None] * nworkers

        while len(skip_idx) < nsubruns:
            idx = next(idx_cycle)
            if idx in skip_idx:
                continue
            # print(f'waiting for workflows to submit tasks ({len(skip_idx)})')

            if wfs[idx] is None:
                # get a list of files
                file_slice = None
                if by_file:
                    file_slice = list(
                        itertools.islice(file_generator, self.run_opts.files_per_subrun)
                    )
                    if not file_slice:
                        skip_idx.add(idx)
                        continue

                # critically, do this after we slice for files so that we
                # don't break the file generator order for the next
                # workflow
                if not self.dry_run and self.workflow_in_db(idx):
                    print(f"Skip workflow at index={idx}, found in db")
                    skip_idx.add(idx)
                    continue

                # wrapper calls the user's setup_single_workflow and sets its ID
                this_wf = self.setup_single_workflow_wrapper(
                    idx, file_slice, last_files[idx % nworkers]
                )
                # user can return None from setup_single_workflow to skip based on inputs
                if this_wf is None:
                    skip_idx.add(idx)
                    continue

                wfs[idx] = this_wf
                self._workflow_counters[wfs[idx]._id] = {
                    "done": 0,
                    "nfinal": wfs[idx].n_final_stages,
                }

            # rate-limit the number of concurrent futures to avoid using too
            # much memory on login nodes (set to negative number to disable)
            if not self.dry_run:
                while len(self.futures) >= self.max_futures and self._future_limit:
                    min_done = min(1, len(self.futures) - self.max_futures)
                    print(f"Waiting: Current futures={len(self.futures)}")
                    self.get_task_results(min_done=min_done, min_time=1)

            try:
                next(wfs[idx].get_next_task())
            except StopIteration:
                skip_idx.add(idx)
                done_workflows = len(skip_idx)
                last_files[idx % nworkers] = wfs[idx]._get_last_file()
                if done_workflows % nworkers == 0:
                    idx_cycle = itertools.cycle(
                        range(done_workflows, min(nsubruns, done_workflows + nworkers))
                    )

                # let garbage collection happen
                wfs[idx] = None

        if not self.dry_run:
            while len(self.futures) > 0:
                print(f"All tasks submitted, draining {len(self.futures)} tasks")
                self.get_task_results(min_done=1, min_time=5)

        self._db_worker_stop.set()
        self._db_update_thread.join()
        print("Done")
        if not self.dry_run:
            print(f"(submitted/skipped) = ({self._stage_counter}/{self._skip_counter})")
            print(f"(success/fail) = ({self._success_counter}/{self._fail_counter})")

    def get_task_results(self, min_done: int = 1, min_time=None):
        """
        Wait for task to finish. Require min_done tasks and min_time time elapsed before returning
        """
        start_time = time.time()

        ndone = 0
        npass = 0
        nfail = 0

        # helper to check if we can return
        def conditions_met():
            now = time.time()
            if ndone < min_done:
                return False

            # require minimum number of seconds
            if min_time is not None:
                if now - start_time < min_time:
                    return False

            return True

        for f in as_completed(list(self.futures)):
            self.futures.discard(f)
            ndone += 1

            # Record completion metadata for dynamic prioritization
            stage_id = getattr(f, "stage_id", None)
            if stage_id:
                self._completion_counter += 1
                
                # Determine parent execution runtime
                parent = getattr(f, "parent", f)
                task_record = getattr(parent, "task_record", None)
                runtime = 0.0
                if task_record:
                    start_t = task_record.get('try_time_launched')
                    end_t = task_record.get('try_time_returned')
                    if start_t and end_t:
                        runtime = (end_t - start_t).total_seconds()
                
                self._completed_stages_info[stage_id] = {
                    'completion_order': self._completion_counter,
                    'timestamp': time.time(),
                    'workflow_id': getattr(f, "workflow_id", None),
                    'runtime': runtime
                }
                print(f"[STAGE COMPLETED] Stage {stage_id} completed. Runtime: {runtime:.2f} seconds", flush=True)

            success = False
            try:
                f.result()
                success = True
                self._db_event_queue.put(
                    {"type": "stage", "stage_id": f.stage_id, "status": 0}
                )
                npass += 1
                self._success_counter += 1
            except Exception as e:
                # ignore "manager lost" errors. Don't write these to DB
                if not isinstance(e, ManagerLostError):
                    print(f"[FAILED] task {f.tid} {f.filepath} ({e})")
                    self._db_event_queue.put(
                        {"type": "stage", "stage_id": f.stage_id, "status": 1}
                    )
                nfail += 1
                self._fail_counter += 1

            # check if we can mark the workflow as complete
            if success:
                # try/except for backwards compatibility
                try:
                    if f.final:
                        self._workflow_counters[f.workflow_id]["done"] += 1
                        if (
                            self._workflow_counters[f.workflow_id]["done"]
                            == self._workflow_counters[f.workflow_id]["nfinal"]
                        ):
                            # mark in DB that this workflow is fully finished
                            print(f"Workflow {f.workflow_id} completed successfully!")
                            self._db_event_queue.put(
                                {"type": "workflow", "workflow_id": f.workflow_id}
                            )
                except AttributeError:
                    print(
                        "Future is missing workflow_id attribute required for caching. Please set in the runfunc!"
                    )

            if conditions_met():
                break

        print(f"Futures [SUCCESS]/[FAILED]: {npass}/{nfail}")

    def _backup_db_loop(self):
        pending_stage_updates = []
        pending_workflow_ids = []

        last_flush_time = time.time()
        last_backup_time = [time.time()]

        def _flush_db_batches():
            """Perform batched DB writes for pending events."""
            nonlocal pending_stage_updates
            nonlocal pending_workflow_ids
            if pending_stage_updates:
                self.mark_stages_in_db(pending_stage_updates)
            if pending_workflow_ids:
                self.mark_workflows_in_db(pending_workflow_ids)
            pending_stage_updates.clear()
            pending_workflow_ids.clear()

        while True:
            timeout = self._db_batch_max_wait
            try:
                evt = self._db_event_queue.get(timeout=timeout)
            except queue.Empty:
                evt = None

            if evt is not None:
                if evt["type"] == "stage":
                    pending_stage_updates.append((evt["stage_id"], evt["status"]))
                elif evt["type"] == "workflow":
                    pending_workflow_ids.append(evt["workflow_id"])
                else:
                    raise RuntimeError(f"Got unsupported database event {evt['type']}")
            elif self._db_worker_stop.is_set():
                # Queue is empty and stop flag is set
                break

            now = time.time()
            should_flush = (pending_stage_updates or pending_workflow_ids) and (
                len(pending_stage_updates) >= self._db_batch_max_size
                or (now - last_flush_time) >= self._db_batch_max_wait
            )

            if should_flush:
                print(f"Writing {len(pending_stage_updates)} stage(s) to database")
                _flush_db_batches()
                last_flush_time = now

                self._maybe_backup_db(
                    force=False, last_backup_time_ref=last_backup_time
                )
                print("Done writing to database")

        # Final flush of any remaining batched updates
        _flush_db_batches()
        # Final sync to disk
        self.backup_db()

        self._mem_db.close()
        self._disk_db.close()

    def backup_db(self, nretries: int = 5):
        """Sync the in-memory database with the disk one.
        Sometimes fails on lustre similar to this: https://github.com/CGATOxford/CGATPipelines/issues/39
        """
        nretries = max(0, nretries)
        for i in range(nretries):
            try:
                self._mem_db.backup(self._disk_db)
                return
            except sqlite3.OperationalError as e:
                if i < nretries - 1:
                    print(f"Failed to sync database file! Retrying... ({i})")
                    time.sleep(10)
                    continue
                raise e

    def _maybe_backup_db(self, force: bool, last_backup_time_ref):
        """Call backup_db every _db_backup_interval seconds or if forced.

        last_backup_time_ref: single-element list [last_backup_time] so we can update it.
        """
        now = time.time()
        last_backup_time = last_backup_time_ref[0]
        if force or (now - last_backup_time) >= self._db_backup_interval:
            self.backup_db()
            last_backup_time_ref[0] = now

    def setup_single_workflow_wrapper(
        self, iteration: int, inputs=None, last_file=None
    ):
        """
        Wraps `setup_single_workflow` to automatically inject the workflow ID
        and finalize the workflow ancestry before execution.
        """
        wf = self.setup_single_workflow(iteration, inputs, last_file)
        if wf is not None:
            wf._id = iteration
            wf._finalize()
        return wf

    def setup_single_workflow(self, iteration: int, inputs=None, last_file=None):
        """
        User-implemented method to define the workflow stages and dependencies.
        """
        pass

    def stage_in_db(self, stage_id: str, require_success: bool = False) -> bool:
        """Checks the sqlite database to see if a stage has been previously completed."""
        with self._db_lock:
            result = self._cursor.execute(
                "SELECT status FROM stages WHERE stage_id=(?)", (stage_id,)
            ).fetchone()

        if result is None:
            return False

        if require_success:
            return result[0] == 0
        return True

    def workflow_in_db(self, id_) -> bool:
        """Checks the sqlite database to see if a workflow has been previously completed."""
        with self._db_lock:
            result = self._cursor.execute(
                "SELECT 1 FROM workflows WHERE id=(?)", (id_,)
            ).fetchone()
        return result is not None

    def mark_stage_in_db(self, stage_id, status: int = 0):
        """Add or update the stage status in the database."""
        self.mark_stages_in_db([(stage_id, status)])

    def mark_stages_in_db(self, stages):
        """Batch insert or update stage statuses in the database."""
        if not stages:
            return

        with self._db_lock:
            self._cursor.executemany(
                "INSERT OR REPLACE INTO stages (stage_id, status) VALUES (?, ?)",
                stages,
            )
            self._mem_db.commit()

    def mark_workflow_in_db(self, id_):
        """Mark a workflow as fully completed in the database."""
        self.mark_workflows_in_db([id_])

    def mark_workflows_in_db(self, ids):
        """Batch insert completed workflow IDs into the database."""
        if not ids:
            return
        rows = [(id_,) for id_ in ids]
        with self._db_lock:
            self._cursor.executemany(
                "INSERT OR REPLACE INTO workflows (id) VALUES (?)",
                rows,
            )
            self._mem_db.commit()


if __name__ == "__main__":
    # TODO demo
    pass
