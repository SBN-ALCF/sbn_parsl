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
import sys
import json
import time
from types import MethodType
import itertools
import pathlib
from collections import deque
from datetime import datetime
import sqlite3
import logging
logger = logging.getLogger(__name__)

from enum import Enum, Flag, auto
from typing import List, Tuple, Dict, Optional, Callable


class NoInputFileException(Exception):
    pass

class NoFclFileException(Exception):
    pass

class WorkflowException(Exception):
    pass

class StageAncestorException(Exception):
    pass

class NoStageOrderException(Exception):
    pass


class StageProperty(Flag):
    NONE = 0
    NO_FCL = auto()
    NO_PARENT = auto()
    NO_INPUT = auto()
    _SUPER = auto()


class StageType():
    def __init__(self, name: str, props: Optional[StageProperty]=StageProperty.NONE):
        self._name = name
        self._props = props

    @property
    def properties(self):
        return self._props

    @property
    def name(self):
        return self._name

    def __eq__(self, other):
        if isinstance(other, StageType):
            return self._name == other.name and self._props == other.properties
        return NotImplemented

    def __hash__(self):
        """Override so StageTypes can be used as dictionary keys."""
        return hash(self._name)


class DefaultStageTypes:
    """Provide some commonly used StageTypes."""
    GEN = StageType('gen', StageProperty.NO_INPUT | StageProperty.NO_PARENT)
    SPINE = StageType('spine', StageProperty.NO_FCL)

    # these are just common stage names, no special properties
    G4 = StageType('g4')
    DETSIM = StageType('detsim')
    RECO1 = StageType('reco1')
    RECO2 = StageType('reco2')
    STAGE0 = StageType('stage0')
    STAGE1 = StageType('stage1')
    DECODE = StageType('decode')
    CAF = StageType('caf')
    SCRUB = StageType('scrub')

    @staticmethod
    def from_str(name: str) -> 'StageType':
        """Return the StageType instance with the given name, or raise ValueError."""
        try:
            return getattr(DefaultStageTypes, name.upper())
        except AttributeError:
            raise ValueError(
                f"StageType '{name}' not found in DefaultStageTypes. "
                f"Available: {DefaultStageTypes._member_names()}"
            )

    @staticmethod
    def _member_names():
        return [k for k, v in DefaultStageTypes.__dict__.items() \
                if not k.startswith('_') and isinstance(v, StageType)]


# special stage that triggers end-of-workflow actions
_SUPER = StageType('super', StageProperty._SUPER | StageProperty.NO_FCL)


def default_runfunc(stage_self, fcl, input_files, output_dir) -> List[pathlib.Path]:
    """Default function called when each stage is run."""
    input_file_arg_str = ''
    if input_files is not None:
        input_file_arg_str = \
            ' '.join([f'-s {str(file)}' for file in input_files])

    output_filename = os.path.basename(fcl).replace(".fcl", ".root")
    if output_dir is None:
        output_dir = pathlib.Path('.')
    output_file = output_dir / pathlib.Path(output_filename)
    output_file_arg_str = f'--output {str(output_file)}'
    logger.info(f'lar -c {fcl} {input_file_arg_str} {output_file_arg_str}')
    return [output_file]


class Stage:
    def __init__(self, stage_type: StageType, fcl: Optional[str]=None,
                 runfunc: Optional[Callable]=None, stage_order: Optional[List[StageType]]=None):

        if isinstance(stage_type, str):
            stage_type = DefaultStageTypes.from_str(stage_type)
        # elif isinstance(stage_type, DefaultStageTypes):
        #     stage_type = stage_type.value

        self._stage_type: StageType = stage_type
        self.fcl = fcl
        self.runfunc = runfunc
        self.run_dir = None

        # override for custom stage order, otherwise this is set by the Workflow
        self.stage_order = stage_order

        self._complete = False
        self._input_files = None
        self._output_files = None
        self._parents_iterators = deque()
        self._combine = False

        # only relevant for _SUPER stage, hold a reference to the last output
        # (often Parsl datafuture) of the workflow so that it may be used as a
        # dummy input to the next workflow. This forces parsl to complete an
        # entire workflow before starting stages from the next. It is slightly
        # less efficient, but results in faster time to complete full workflows
        self._workflow_last_file = None

        # initialized automatically when added to workflow; user does not need
        # to set these
        # parent stages: Filled until the workflow starts, then is deleted
        # workflow_id: Which workflow created this stage
        # stage_id: Stage counter within the workflow
        # final: Is this the final stage of the workflow?
        self._temp_parent_stages = {}
        self.workflow_id: int = None
        self.stage_id: Tuple[int] = None
        self.final: bool = False

    @property
    def stage_type(self) -> StageType:
        return self._stage_type

    @property
    def output_files(self) -> List:
        if self._output_files is None:
            print(f'Warning: Running stage of type {self.stage_type} via output_files method')
            self.run()
        return self._output_files

    @property
    def input_files(self) -> List:
        return self._input_files

    @property
    def parent_type(self) -> Optional[StageType]:
        """Return the stage type of the stage before this one."""
        if self.stage_order is None:
            raise NoStageOrderException(f'No stage order set for stage of type {self.stage_type.name}. Either add this stage to a Workflow or set stage_order at initialization.')

        idx = self.stage_order.index(self.stage_type)
        if idx == 0:
            return None

        parent_idx = idx - 1
        if parent_idx < 0:
            raise StageAncestorException(f'No ancestor for {self.stage_type} in list {self.stage_order}')

        return self.stage_order[parent_idx]

    def has_parents(self) -> bool:
        return len(self._parents_iterators) > 0

    def parents(self, _type: StageType):
        return set(s[0] for s in self._parents_iterators if s[0].stage_type == _type)

    @property
    def complete(self) -> bool:
        return self._complete

    '''
    def check_complete(self, iteration: int) -> bool:
        """Manually check if this stage is complete based on an iteration number."""
        func = MethodType(self.outfunc, self)
        output_files = func(iteration)
        if output_filename.is_file():
            self._complete = True
            self._output_files = output_files
        return self.complete
    '''

    def add_input_file(self, file) -> None:
        if self._input_files is None:
            self._input_files = [file]
        else:
            self._input_files.append(file)

    @property
    def combine(self) -> bool:
        return self._combine

    @combine.setter
    def combine(self, val: bool) -> None:
        self._combine = val

    @property
    def stage_id_str(self) -> str:
        return f'{self.workflow_id}_' + '_'.join(str(c) for c in self.stage_id)

    def run(self, rerun: bool=False) -> None:
        """Produces the output file for this stage."""
        # if calling run method directly instead of asking for output files,
        # must specify rerun option to avoid calling runfunc multiple times!
        if self._output_files is not None and not rerun:
            return

        # make sure we delete references to the parents once they are run
        if self.has_parents():
            raise RuntimeError(f'Attempt to run stage {self._stage_type} while it still holds references to its parents')

        if StageProperty._SUPER in self._stage_type.properties:
            print('Congratulations, you ran all the stages!') 
            self._complete = True
            return

        if self.fcl is None and StageProperty.NO_FCL not in self._stage_type.properties:
            raise NoFclFileException(f'Attempt to run stage {self._stage_type.name} with no fcl provided and no default')

        if StageProperty.NO_INPUT in self._stage_type.properties:
            pass
        else:
            if self._input_files is None:
                raise NoInputFileException(f'Tried to run stage of type {self._stage_type.name} which requires at least one input file, but it was not set.')

        # bind the func to this object with MethodType so self resolves as if
        # it were a member function
        self._complete = True
        func = MethodType(self.runfunc, self)
        self._output_files = func(self.fcl, self._input_files, self.run_dir)

    def add_parents(self, stages: List, fcls: Optional[Dict]=None) -> None:
        """workflow calls finalize routine to actually add the parents, user
        code caches them prior to that call."""
        self._temp_parent_stages[stages] = fcls

    def _finalize(self):
        """Called by workflow for SUPER stage to allows workflow to control
        when the stages are actually added (e.g., after setting up IDs)"""
        for k, v in self._temp_parent_stages.items():
            self._add_parents(k, v)
            if not isinstance(k, list):
                k = [k]
            for s in k:
                s._finalize()
        self._temp_parent_stages = {}

    def _add_parents(self, stages: List, fcls: Optional[Dict]=None) -> None:
        """Add a list of known prior stages to this one."""
        if not isinstance(stages, list):
            stages = [stages]

        for s in stages:
            if s.stage_type != self.parent_type:
                raise StageAncestorException(f"Tried to add stage of type {s.stage_type.name} as a parent to a stage with type {self.stage_type.name}")
            if s.workflow_id is None:
                s.workflow_id = self.workflow_id
            if s.stage_id is None:
                # when adding to the SUPER stage, stage ID is 0, 1, 2, ... etc.
                # then adding parents to those stages would give IDs of 00, 01, 02, ..., 10, 11, 11, ..., etc.
                s.stage_id = self.stage_id + (len(self._parents_iterators),)
            if StageProperty._SUPER in self._stage_type.properties:
                s.final = True
            if s.stage_order is None:
                s.stage_order = self.stage_order

            if s.parent_type is not None and fcls is None:
                raise NoFclFileException(f'Must specify fcl file dictionary argument when adding a parent stage if the parent is not the first stage.')

            if s.run_dir is None:
                s.run_dir = self.run_dir
            if s.runfunc is None:
                s.runfunc = self.runfunc

            self._parents_iterators.append((s, run_stage(s, fcls)))

    def get_next_task(self, mode='cycle'):
        """
        Run the workflow by individually running the added stages. Can either
        cycle through end stages (grab one task from each stage at a time) or
        not (grab all tasks from first stage before continuing)
        """
        while self._parents_iterators:
            # remove 
            parent, iterator = self._parents_iterators.popleft()
            try:
                next(iterator)
                # put back if not done. Either at the back of the deque or in place
                if mode == 'cycle':
                    self._parents_iterators.append((parent, iterator))
                else:
                    self._parents_iterators.appendleft((parent, iterator))
                yield
            except StopIteration:
                # no "append" here: Let the iterator removed via popleft above
                # go out of scope, but grab the parent's inputs first
                for f in parent.output_files:
                    self.add_input_file(f)

                if StageProperty._SUPER in self.stage_type.properties:
                    self._workflow_last_file = parent.output_files


def run_stage(stage: Stage, fcls: Optional[Dict]=None):
    """Run an individual stage, recursing to parent stages as necessary.
    Note that this is a generator: Call next(Workflow.run_stage(stage)) to
    get the next task."""

    # this raises StopIteration
    if stage.complete:
        return

    if stage.fcl is None and StageProperty.NO_FCL not in stage.stage_type.properties:
        if not fcls:
            raise NoFclFileException(f"Tried to run a stage with no fcl file. Either set the stage's fcl file first, or pass in a dictionary to run_stage.")

        try:
            stage.fcl = fcls[stage.stage_type]
        except KeyError:
            stage.fcl = fcls[stage.stage_type.name]

    if stage.runfunc is None:
        logger.warning(f'No runfunc specified for stage with type {stage.stage_type}. Adding default runfunc')
        stage.runfunc = default_runfunc

    # some stage types should not have any parents
    if StageProperty.NO_PARENT in stage.stage_type.properties:
        stage.run()
        yield

    # if we have our inputs already, can run
    if stage.input_files is not None and not stage.has_parents():
        stage.run()
        yield

    # the above yields could result in the stage now being finished, so we have
    # to check again
    if stage.complete:
        return

    if not stage.has_parents():
        # no inputs and no parents -> create a parent stage
        parent_stage = Stage(stage.parent_type)
        stage.add_parents(parent_stage, fcls)
        stage._finalize()

    # run parent stages first
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
    Collection of stages and order to run the stages
    fills in the gaps between inputs and outputs
    """
    @staticmethod
    def default_runfunc(stage_self, fcl, input_files, output_dir) -> List[pathlib.Path]:
        """Default function called when each stage is run."""
        input_file_arg_str = ''
        if input_files is not None:
            input_file_arg_str = \
                ' '.join([f'-s {str(file)}' for file in input_files])

        output_filename = os.path.basename(fcl).replace(".fcl", ".root")
        output_file = output_dir / pathlib.Path(output_filename)
        output_file_arg_str = f'--output {str(output_file)}'
        print(f'lar -c {fcl} {input_file_arg_str} {output_file_arg_str}')
        return [output_file]

    def __init__(self, stage_order: List[StageType], default_fcls: Optional[Dict]=None, run_dir: pathlib.Path=pathlib.Path(), runfunc: Optional[Callable]=None):

        # ID is set by the workflow executor
        self._id = None
        self._n_final_stages = 0

        self._stage_order = stage_order

        self.default_fcls = {}
        if default_fcls is not None:
            for k, v in default_fcls.items():
                if not isinstance(k, StageType):
                    self.default_fcls[DefaultStageTypes.from_str(k)] = v
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
        """Add the final stage to the workflow."""
        self._final_stages.append(stage)

    def _finalize(self):
        """Let the workflow executor control when to call add_parents."""
        # keep track of where this stage came from
        # super stage always gets Stage ID as the empty string
        self._stage.workflow_id = self._id

        # this saves us one iteration in get_next_task. by marking combined,
        # when we submit the super stage, it will submit its parent on the
        # first call
        self._stage.combine = True

        self._stage.stage_id = tuple()

        for s in self._final_stages:
            self._stage.add_parents(s, self.default_fcls)

            # counter is used by the workflow executor to determine if all the
            # final stages of this workflow have completed
            self._n_final_stages += 1
        self._stage._finalize()

        # no need to keep this around
        del self._final_stages

    @property
    def n_final_stages(self):
        return self._n_final_stages

    '''
    def _resolve(self, stage=None, iteration):
        """
        Mark stages complete based on iteration number.
        While it is optimal for first-time runs to begin by running the stages
        with no dependencies, it becomes sub-optimal to re-run those stages if
        re-running the workflow from a  mostly complete state. This function
        marks stages with dependencies as complete if the "check_complete"
        function finds the correct output, e.g., a file with the expected name,
        therefore skipping submission of the earlier stages. If the user knows
        the workflow is a first-time run, this method should not be called.
        Otherwise it could save some time.
        """
        if stage is None:
            stage = self._stage

        for p in stage.parents():
            if not p.check_complete(iteration):
                self._resolve(p, iteration)
    '''

    def get_next_task(self):
        """
        Run the workflow by individually running the added stages. Can either
        cycle through end stages (grab one task from each stage at a time) or
        not (grab all tasks from first stage before continuing)
        """
        try:
            next(run_stage(self._stage, self.default_fcls))
            yield
        except StopIteration:
            pass

    def _get_last_file(self):
        return self._stage._workflow_last_file


class WorkflowExecutor: 
    """Class to wrap settings and run multiple workflow objects."""
    def __init__(self, settings: json):
        self.larsoft_opts = None
        try:
            self.larsoft_opts = settings['larsoft']
        except KeyError:
            pass

        self.run_opts = settings['run']
        self.output_dir = pathlib.Path(self.run_opts['output'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_futures = self.run_opts['max_futures']
        self.name_salt = str(settings['run']['seed']) + str(self.output_dir)

        self.fcl_dir = None
        self.fcls = {}
        try:
            self.fcl_dir = pathlib.Path(self.run_opts['fclpath'])
        except KeyError:
            pass
        self.fcls = settings['fcls']

        self.futures = []

        self.workflow_opts = settings['workflow']
        self._workflow_counters = {}

        # optionally track the number of submitted stages. Runfunc should modify these
        self._stage_counter = 0
        self._skip_counter = 0
        self._success_counter = 0
        self._fail_counter = 0

        # file tracking with sqlite as files are created, write them to the
        # database this allows us to check the database instead of the
        # filesystem on workflow restarts. While Parsl does this generically
        # for tasks, using the Parsl task cache requires actually submitting
        # the task, whereas this check can avoid submitting the task entirely
        db_file = self.output_dir / 'runinfo' / 'cmd' / 'file_cache.db'
        db_file.parent.mkdir(exist_ok=True)
        self._disk_db = sqlite3.connect(str(db_file))
        self._mem_db = sqlite3.connect(":memory:")
        self._disk_db.backup(self._mem_db)
        self._cursor = self._mem_db.cursor()

        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS stages (
                stage_id TEXT PRIMARY KEY
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


    def file_generator(self):
        with open(self.run_opts['file_list'], 'r') as f:
            for line in f.readlines():
                yield pathlib.Path(f)

    def execute(self, nworkers: int=-1):
        """
        Run many copies of a single workflow. yield each time a stage is
        executed for efficient task submission, i.e., we'll get the first tasks
        from each workflow first, instead of all the tasks from workflow 0,
        then all the tasks from workflow 1, etc.  Use itertools.cycle() to keep
        looping over all workflows until all tasks are submitted.
        If nworkers > 0, tasks will be gotten from <nworkers> workflows first
        before cycling over other workflows
        """

        nsubruns = self.run_opts['nsubruns']
        file_generator = None
        by_file = False
        if 'files_per_subrun' in self.run_opts:
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
                    file_slice = list(itertools.islice(file_generator, self.run_opts['files_per_subrun']))
                    if not file_slice:
                        skip_idx.add(idx)
                        continue

                # critically, do this after we slice for files so that we
                # don't break the file generator order for the next
                # workflow
                if self.workflow_in_db(idx):
                    print(f'Skip workflow at index={idx}, found in db')
                    skip_idx.add(idx)
                    continue

                # wrapper calls the user's setup_single_workflow and sets its ID
                wfs[idx] = self.setup_single_workflow_wrapper(idx, file_slice, last_files[idx % nworkers])
                self._workflow_counters[wfs[idx]._id] = {'done': 0, 'nfinal': wfs[idx].n_final_stages}

            # rate-limit the number of concurrent futures to avoid using too
            # much memory on login nodes
            while len(self.futures) > self.max_futures:
                self.get_task_results()
                # still too many?
                if len(self.futures) > self.max_futures:
                    print(f'Waiting: Current futures={len(self.futures)}')
                    time.sleep(10)

            try:
                next(wfs[idx].get_next_task())
            except StopIteration:
                skip_idx.add(idx)
                done_workflows = len(skip_idx)
                last_files[idx % nworkers] = wfs[idx]._get_last_file()
                if done_workflows % nworkers == 0:
                    idx_cycle = itertools.cycle(range(done_workflows, min(nsubruns, done_workflows + nworkers)))

                # let garbage collection happen
                wfs[idx] = None
        
        while len(self.futures) > 0:
            print(f'waiting for tasks to finish ({len(self.futures)})')
            self.get_task_results()
            time.sleep(10)

        self._mem_db.backup(self._disk_db)
        self._mem_db.close()
        self._disk_db.close()
        print('Done')
        print(f'(submitted/skipped) = ({self._stage_counter}/{self._skip_counter})')
        print(f'(success/fail) = ({self._success_counter}/{self._fail_counter})')

    def get_task_results(self):
        """Loop over all tasks & clear finished ones."""
        remaining_futures = []

        # these counters print the # of successes/failures since the last
        # update, separate from the total workflow count
        npass = 0
        nfail = 0
        for f in self.futures:
            if not f.done():
                remaining_futures.append(f)
                continue

            success = False
            try:
                f.result()
                success = True
                npass += 1
                self._success_counter += 1
            except Exception as e:
                print(f'[FAILED] task {f.tid} {f.filepath} ({e})')
                nfail += 1
                self._fail_counter += 1

            if success:
                self.mark_stage_in_db(f.stage_id)
                # try/except for backwards compatibility
                try:
                    if f.final:
                        self._workflow_counters[f.workflow_id]['done'] += 1
                        if self._workflow_counters[f.workflow_id]['done'] == \
                                self._workflow_counters[f.workflow_id]['nfinal']:
                            # mark in DB that this workflow is fully finished
                            print(f'Workflow {f.workflow_id} completed successfully!')
                            self.mark_workflow_in_db(f.workflow_id)
                except AttributeError as e:
                    print(f'Future is missing workflow_id attribute required for caching. Please set in the runfunc!')

        print(f'Futures [SUCCESS]/[FAILED]: {npass}/{nfail}')

        # sync the in-memory database with the disk one if we had any changes
        if npass > 0:
            self._mem_db.backup(self._disk_db)
            self.futures = remaining_futures

    def setup_single_workflow_wrapper(self, iteration: int, inputs=None, last_file=None):
        """Wrap setting the workflow ID so that the user doesn't have to do it."""
        wf = self.setup_single_workflow(iteration, inputs, last_file)
        wf._id = iteration
        wf._finalize()
        return wf

    def setup_single_workflow(self, iteration: int, inputs=None, last_file=None):
        # user should implement this
        pass

    def stage_in_db(self, stage_id: str) -> bool:
        """Check if the file is in the file database."""
        result = self._cursor.execute(
            "SELECT 1 FROM stages WHERE stage_id=(?)",
            (stage_id,)
        ).fetchone()
        return result is not None

    def workflow_in_db(self, id_) -> bool:
        """Check if the file is in the file database."""
        result = self._cursor.execute(
            "SELECT 1 FROM workflows WHERE id=(?)",
            (id_,)
        ).fetchone()
        return result is not None

    def mark_stage_in_db(self, stage_id):
        """Add or update the file in the database."""
        # str_filename = filename
        # if isinstance(filename, pathlib.Path):
        #     str_filename = str(filename.resolve())
        self._cursor.execute(
            "INSERT OR REPLACE INTO stages (stage_id) VALUES (?)",
            (stage_id,)
        )
        self._mem_db.commit()

    def mark_workflow_in_db(self, id_):
        """Add or update the file in the database."""
        self._cursor.execute(
            "INSERT OR REPLACE INTO workflows (id) VALUES (?)",
            (id_,)
        )
        self._mem_db.commit()

if __name__ == '__main__':
    # TODO demo
    pass
