
class WorkQueue(object):

    def __init__(self, q):
        object.__setattr__(self, '_q', q)
        object.__setattr__(self, '_task_table', dict())

    def __getattribute__(self, attr):
        try:
            attribute = object.__getattribute__(self, attr)
        except AttributeError:
            q = object.__getattribute__(self, '_q')
            if isinstance(q, WorkQueue):
                attribute = WorkQueue.__getattribute__(q, attr)
            else:
                attribute = object.__getattribute__(q, attr)

        return attribute

    def submit(self, taskable):
        ccl_task = taskable.to_ccl_task()
        ccl_task.specify_tag(taskable.uuid)
        taskid = self._q.submit(ccl_task)
        self._task_table[taskid] = taskable
        return taskid

    def wait(self, *args, **kws):
        ccl_task = self._q.wait(*args, **kws)
        if ccl_task:
            task = self._task_table[ccl_task.id]
            del self._task_table[ccl_task.id]
            task.from_ccl_task(ccl_task)
            return task
