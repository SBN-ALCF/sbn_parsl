"""
Hacks to Parsl DataFlowKernel (DFK) object to improve memory usage
Hack description:
 - my_update_memo: By default, Parsl stores the full AppFuture in the
   Memoizer's memo_lookup_table. This creates a ref to a task_record object
   which contains a circular ref to the AppFuture, among other Python objects.
   Below, we replace the AppFuture with a special "ResultFuture" that only
   contains the result & the task ID. This breaks the reference & allows the
   task_records to be freed.
 - my_check_memo: memo_lookup_table grows without bound by default. We modify
   the check_memo function to remove entries from the memo_lookup_table if no
   other tasks depend on them. This helps free the futures in the table.
"""

import logging

from concurrent.futures import Future
from types import MethodType

from parsl.dataflow.memoization import make_hash
from parsl.data_provider.staging import Staging
from parsl.data_provider.zip import ZipFileStaging
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPSeparateTaskStaging
from parsl.data_provider.http import HTTPSeparateTaskStaging
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    pass

logger = logging.getLogger("parsl.dataflow.memoization")

default_staging: List[Staging]
default_staging = [
    NoOpFileStaging(),
    FTPSeparateTaskStaging(),
    HTTPSeparateTaskStaging(),
    ZipFileStaging(),
]


class ResultFuture(Future):
    def __init__(self, task_id):
        super().__init__()
        self.tid = task_id


def my_update_memo(self, task) -> None:
    """
    update memo function that stores a copy of the future result, instead of
    the original future, in the memo_lookup_table. The original future result
    contains references to the parent AppFutures, preventing them from being
    garbage collected otherwise.
    """
    task_id = task["id"]
    r = task["app_fu"]

    if not self.memoize or not task["memoize"] or "hashsum" not in task:
        return

    if not isinstance(task["hashsum"], str):
        logger.error(
            "Attempting to update app cache entry but hashsum is not a string key"
        )
        return

    if task["hashsum"] in self.memo_lookup_table:
        logger.info(
            f"Replacing app cache entry {task['hashsum']} with result from task {task_id}"
        )
        self.memo_lookup_table[task["hashsum"]] = r
    else:
        logger.info(
            f"Storing app cache entry {task['hashsum']} with result from task {task_id}"
        )
        new_future = ResultFuture(task_id)
        new_future.set_result(r.result())
        self.memo_lookup_table[task["hashsum"]] = new_future


def my_check_memo(self, task):
    """
    check_memo function that removes tasks from the task record. This
    prevents the task record from growing in memory. Check happens once a task
    completes, and dependent tasks are removed from the record.

    This assumes that no two tasks have the same dependent task!!!
    """
    task_id = task["id"]

    if not self.memoize or not task["memoize"]:
        task["hashsum"] = None
        logger.debug("Task {} will not be memoized".format(task_id))
        return None

    hashsum = make_hash(task)
    logger.debug("Task {} has memoization hash {}".format(task_id, hashsum))
    result = None
    if hashsum in self.memo_lookup_table:
        result = self.memo_lookup_table[hashsum]
        logger.debug("Task %s using result from cache", task_id)
        logger.debug(
            "Clearing dependencies of task %d (%d)", task_id, len(task["depends"])
        )
        # find depends task hashes in memoizer & remove them
        for df in task["depends"]:
            task_obj = df.parent.parent.task_record
            hhash = self.make_hash(task_obj)
            for thash, f in self.memo_lookup_table.items():
                if thash == hhash:
                    logger.debug("removing task with hash %s from cache", thash)
                    del self.memo_lookup_table[thash]
                    break
    else:
        logger.info("Task %s had no result in cache", task_id)

    task["hashsum"] = hashsum

    assert isinstance(result, Future) or result is None
    return result


def my_launch_task(self, task_record):
    """
    Monkey-patched launch_task function that intercepts the launch process,
    retrieves completion info of parent stages, calculates a dynamic priority,
    and removes custom keys from the resource_specification to pass Parsl's validation.
    """
    spec = task_record.get('resource_specification', {})
    # Pop custom metadata keys so Parsl's executor validator doesn't raise exception
    stage_id = spec.pop('stage_id', None)
    parent_stage_ids = spec.pop('parent_stage_ids', [])

    # 1. Map this task's stage_id to its Parsl task ID and vice-versa
    if stage_id:
        if not hasattr(self, '_stage_id_to_task_id'):
            self._stage_id_to_task_id = {}
        self._stage_id_to_task_id[stage_id] = task_record['id']

        if not hasattr(self, '_task_id_to_stage_id'):
            self._task_id_to_stage_id = {}
        self._task_id_to_stage_id[task_record['id']] = stage_id

    # 2. Retrieve parent runtimes from DFK's completed stages runtimes
    if parent_stage_ids:
        parent_runtimes = []
        for pid in parent_stage_ids:
            # First check DFK completed runtimes
            if hasattr(self, '_completed_stages_runtime') and pid in self._completed_stages_runtime:
                parent_runtimes.append(self._completed_stages_runtime[pid])
            else:
                # Try fallback to workflow_executor._completed_stages_info (useful for testing/restarts)
                fallback_runtime = None
                if hasattr(self, 'workflow_executor'):
                    wfe = self.workflow_executor
                    completed_info = getattr(wfe, '_completed_stages_info', {})
                    if pid in completed_info:
                        fallback_runtime = completed_info[pid].get('runtime')

                if fallback_runtime is not None:
                    parent_runtimes.append(fallback_runtime)
                else:
                    available_keys = list(getattr(self, '_completed_stages_runtime', {}).keys())
                    raise KeyError(
                        f"CRITICAL ERROR: Parent stage ID '{pid}' runtime is missing when launching {task_record['id']} ({stage_id})!\n"
                        f"This means either the dependency tracking failed (and this task launched before {pid} finished), "
                        f"or {pid} was not added to _completed_stages_runtime properly.\n"
                        f"Available completed keys: {available_keys}\n"
                        f"Task record: {task_record}"
                    )

        # If we successfully resolved all parent runtimes:
        if parent_runtimes:
            # We add a large multiplier based on the stage_id depth so that earlier stages
            # (which have longer IDs) always take precedence over later stages.
            # Ties are broken by the parent runtime (longest task first)
            stage_depth = len(stage_id.split('_')) if stage_id else 0
            base_priority = stage_depth * 1000000
            
            # Note: Parsl's queue executes tasks with LARGER priority numbers first!
            # So we add the max parent runtime (longest task gets higher priority number)
            spec['priority'] = base_priority + int(max(parent_runtimes))
            print(f"[PRIORITY UPDATE] Task {task_record['id']} ({stage_id}) priority set to {spec['priority']} (parent runtimes: {parent_runtimes})", flush=True)

    return self._orig_launch_task(task_record)


def my_complete_task_result(self, task_record, new_state, result):
    """Intercept task success to record runtime before DFK's garbage collector wipes the task record."""
    import datetime

    task_id = task_record['id']
    stage_id = None
    if hasattr(self, '_task_id_to_stage_id'):
        stage_id = self._task_id_to_stage_id.get(task_id)

    if stage_id:
        start_t = task_record.get('try_time_launched')
        end_t = datetime.datetime.now()
        runtime = 0.0

        # If result is a dict containing precise runtime from python_app, use it
        if isinstance(result, dict) and 'runtime' in result:
            runtime = result['runtime']
        elif start_t and end_t:
            runtime = (end_t - start_t).total_seconds()

        if not hasattr(self, '_completed_stages_runtime'):
            self._completed_stages_runtime = {}
        self._completed_stages_runtime[stage_id] = runtime

    return self._orig_complete_task_result(task_record, new_state, result)


def my_complete_task_exception(self, task_record, new_state, exception):
    """Intercept task failure to record runtime before DFK's garbage collector wipes the task record."""
    import datetime

    task_id = task_record['id']
    stage_id = None
    if hasattr(self, '_task_id_to_stage_id'):
        stage_id = self._task_id_to_stage_id.get(task_id)

    if stage_id:
        start_t = task_record.get('try_time_launched')
        end_t = datetime.datetime.now()
        runtime = 0.0
        if start_t and end_t:
            runtime = (end_t - start_t).total_seconds()

        if not hasattr(self, '_completed_stages_runtime'):
            self._completed_stages_runtime = {}
        self._completed_stages_runtime[stage_id] = runtime

    return self._orig_complete_task_exception(task_record, new_state, exception)


def apply_hacks(dfk, update_memo=True, check_memo=False, dynamic_priority=True):
    """Overwrite functions in DataFlowKernel object."""
    if update_memo:
        func_update_memo = MethodType(my_update_memo, dfk.memoizer)
        dfk.memoizer.update_memo = func_update_memo

    if check_memo:
        func_check_memo = MethodType(my_check_memo, dfk.memoizer)
        dfk.memoizer.check_memo = func_check_memo

    if dynamic_priority:
        dfk._orig_launch_task = dfk.launch_task
        dfk.launch_task = MethodType(my_launch_task, dfk)

        dfk._orig_complete_task_result = dfk._complete_task_result
        dfk._complete_task_result = MethodType(my_complete_task_result, dfk)

        dfk._orig_complete_task_exception = dfk._complete_task_exception
        dfk._complete_task_exception = MethodType(my_complete_task_exception, dfk)

