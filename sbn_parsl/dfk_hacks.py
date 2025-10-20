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

logger = logging.getLogger('parsl.dataflow.memoization')

class ResultFuture(Future):
    def __init__(self, task_id):
        super().__init__()
        self.tid = task_id


def my_update_memo(self, task, r: Future) -> None:
    """
    update memo function that stores a copy of the future result, instead of
    the original future, in the memo_lookup_table. The original future result
    contains references to the parent AppFutures, preventing them from being
    garbage collected otherwise.
    """
    # TODO: could use typeguard
    assert isinstance(r, Future)

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



def apply_hacks(dfk, update_memo=True, check_memo=False):
    """Overwrite functions in DataFlowKernel object."""
    if update_memo:
        func_update_memo = MethodType(my_update_memo, dfk.memoizer)
        dfk.memoizer.update_memo = func_update_memo
    
    if check_memo:
        func_check_memo = MethodType(my_check_memo, dfk.memoizer)
        dfk.memoizer.check_memo = func_check_memo
