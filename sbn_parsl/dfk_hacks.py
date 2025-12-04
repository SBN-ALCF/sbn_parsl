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

from parsl.app.futures import DataFuture
from parsl.data_provider.staging import Staging
from parsl.data_provider.files import File
from parsl.data_provider.zip import ZipFileStaging
from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPSeparateTaskStaging
from parsl.data_provider.http import HTTPSeparateTaskStaging
from typing import TYPE_CHECKING, Any, Callable, List, Optional
if TYPE_CHECKING:
    from parsl.dataflow.dflow import DataFlowKernel

logger = logging.getLogger('parsl.dataflow.memoization')

default_staging: List[Staging]
default_staging = [NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPSeparateTaskStaging(), ZipFileStaging()]


class ResultFuture(Future):
    def __init__(self, task_id):
        super().__init__()
        self.tid = task_id


def my_update_memo(self, task, r: Future=None) -> None:
    """
    update memo function that stores a copy of the future result, instead of
    the original future, in the memo_lookup_table. The original future result
    contains references to the parent AppFutures, preventing them from being
    garbage collected otherwise.
    """
    # for forwards compatibility, r is optional
    if r is None:
        r = task['app_fu']

    task_id = task['id']

    if not self.memoize or not task['memoize'] or 'hashsum' not in task:
        return

    if not isinstance(task['hashsum'], str):
        logger.error("Attempting to update app cache entry but hashsum is not a string key")
        return

    if task['hashsum'] in self.memo_lookup_table:
        logger.info(f"Replacing app cache entry {task['hashsum']} with result from task {task_id}")
    else:
        logger.info(f"Storing app cache entry {task['hashsum']} with result from task {task_id}")
        new_future = ResultFuture(task_id)
        new_future.set_result(r.result())
        self.memo_lookup_table[task['hashsum']] = new_future



def my_wipe_task(self, task_id: int) -> None:
    print('calling custom my_wipe_task')
    if self.config.garbage_collect:
        task = self.tasks[task_id]
        del task['depends']
        del task['app_fu']
        del self.tasks[task_id]


def my_check_memo(self, task):
    """
    check_memo function that removes tasks from the task record. This
    prevents the task record from growing in memory. Check happens once a task
    completes, and dependent tasks are removed from the record.

    This assumes that no two tasks have the same dependent task!!!
    """
    task_id = task['id']

    if not self.memoize or not task['memoize']:
        task['hashsum'] = None
        logger.debug("Task {} will not be memoized".format(task_id))
        return None

    hashsum = self.make_hash(task)
    logger.debug("Task {} has memoization hash {}".format(task_id, hashsum))
    result = None
    if hashsum in self.memo_lookup_table:
        result = self.memo_lookup_table[hashsum]
        logger.debug("Task %s using result from cache", task_id)
        logger.debug("Clearing dependencies of task %d (%d)", task_id, len(task['depends']))
        # find depends task hashes in memoizer & remove them
        for df in task['depends']:
            task_obj = df.parent.parent.task_record
            hhash = self.make_hash(task_obj)
            for thash, f in self.memo_lookup_table.items():
                if thash == hhash:
                    logger.debug('removing task with hash %s from cache', thash)
                    del self.memo_lookup_table[thash]
                    break
    else:
        logger.info("Task %s had no result in cache", task_id)

    task['hashsum'] = hashsum

    assert isinstance(result, Future) or result is None
    return result


def replace_task_stage_out_no_logging(self, file: File, func: Callable, executor: str) -> Callable:
    """This will give staging providers the chance to wrap (or replace entirely!) the task function."""
    executor_obj = self.dfk.executors[executor]
    if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
        storage_access = executor_obj.storage_access  # type: List[Staging]
    else:
        storage_access = default_staging

    for provider in storage_access:
        # logger.debug("stage_out checking Staging provider {}".format(provider))
        if provider.can_stage_out(file):
            newfunc = provider.replace_task_stage_out(self, executor, file, func)
            if newfunc:
                return newfunc
            else:
                return func

    logger.debug("reached end of staging provider list")
    # if we reach here, we haven't found a suitable staging mechanism
    raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))



def stage_out_no_logging(self, file: File, executor: str, app_fu: Future) -> Optional[Future]:
    """Transport the file from the local filesystem to the remote Globus endpoint.

    This function returns either a Future which should complete when the stageout
    is complete, or None, if no staging needs to be waited for.

    Args:
        - self
        - file (File) - file to stage out
        - executor (str) - Which executor the file is going to be staged out from.
        - app_fu (Future) - a future representing the main body of the task that should
                            complete before stageout begins.
    """
    executor_obj = self.dfk.executors[executor]
    if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
        storage_access = executor_obj.storage_access
    else:
        storage_access = default_staging

    for provider in storage_access:
        # logger.debug("stage_out checking Staging provider {}".format(provider))
        if provider.can_stage_out(file):
            return provider.stage_out(self, executor, file, app_fu)

    logger.debug("reached end of staging provider list")
    # if we reach here, we haven't found a suitable staging mechanism
    raise ValueError("Executor {} cannot stage out file {}".format(executor, repr(file)))

def stage_in_no_logging(self, file: File, input: Any, executor: str) -> Any:
    """Transport the input from the input source to the executor, if it is file-like,
    returning a DataFuture that wraps the stage-in operation.

    If no staging in is required - because the ``file`` parameter is not file-like,
    then return that parameter unaltered.

    Args:
        - self
        - input (Any) : input to stage in. If this is a File or a
          DataFuture, stage in tasks will be launched with appropriate
          dependencies. Otherwise, no stage-in will be performed.
        - executor (str) : an executor the file is going to be staged in to.
    """

    if isinstance(input, DataFuture):
        parent_fut = input  # type: Optional[Future]
    elif isinstance(input, File):
        parent_fut = None
    else:
        raise ValueError("Internal consistency error - should have checked DataFuture/File earlier")

    executor_obj = self.dfk.executors[executor]
    if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
        storage_access = executor_obj.storage_access
    else:
        storage_access = default_staging

    for provider in storage_access:
        # logger.debug("stage_in checking Staging provider {}".format(provider))
        if provider.can_stage_in(file):
            staging_fut = provider.stage_in(self, executor, file, parent_fut=parent_fut)
            if staging_fut:
                return staging_fut
            else:
                return input

    logger.debug("reached end of staging provider list")
    # if we reach here, we haven't found a suitable staging mechanism
    raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))


def replace_task_no_logging(self, file: File, func: Callable, executor: str) -> Callable:
    """This will give staging providers the chance to wrap (or replace entirely!) the task function."""

    executor_obj = self.dfk.executors[executor]
    if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
        storage_access = executor_obj.storage_access
    else:
        storage_access = default_staging

    for provider in storage_access:
        # logger.debug("stage_in checking Staging provider {}".format(provider))
        if provider.can_stage_in(file):
            newfunc = provider.replace_task(self, executor, file, func)
            if newfunc:
                return newfunc
            else:
                return func


def apply_hacks(dfk, update_memo=True, check_memo=False, disable_dm_logging=True):
    """Overwrite functions in DataFlowKernel object."""
    if update_memo:
        func_update_memo = MethodType(my_update_memo, dfk.memoizer)
        dfk.memoizer.update_memo = func_update_memo
    
    if check_memo:
        func_check_memo = MethodType(my_check_memo, dfk.memoizer)
        dfk.memoizer.check_memo = func_check_memo

    if disable_dm_logging:
        func_replace_task_stage_out = MethodType(replace_task_stage_out_no_logging, dfk.data_manager)
        func_stage_out = MethodType(stage_out_no_logging, dfk.data_manager)
        func_stage_in = MethodType(stage_in_no_logging, dfk.data_manager)
        func_replace_task = MethodType(replace_task_no_logging, dfk.data_manager)
        dfk.data_manager.replace_task_stage_out = func_replace_task_stage_out
        dfk.data_manager.stage_out = func_stage_out
        dfk.data_manager.stage_in = func_stage_in
        dfk.data_manager.replace_task = func_replace_task
