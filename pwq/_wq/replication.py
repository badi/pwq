"""
Provides a WorkQueue that supports task replication
"""

from . import decorator
import collections
import random

class _TagSet(object):
    def __init__(self, maxreps=5):
        self._tags = collections.defaultdict(set)
        self._maxreps = maxreps

    def can_duplicate(self):
        """
        Can any of the tasks be duplicated?
        """
        valid = filter(lambda k: k < self._maxreps, self._tags.iterkeys())
        return len(valid) > 0

    def clear(self):
        self._tags.clear()

    def clean(self):
        for k in self._tags.keys():
            if len(self._tags[k]) < 1:
                del self._tags[k]

    def _find_tag_group(self, tag):
        """
        Returns the replication group of the tag if it is running.
        None otherwise
        """

        for group, tags in self._tags.iteritems():
            if tag in tags:
                return group
        return None

    def add(self, tag):
        """
        Increment a tag's replication count
        """

        key = self._find_tag_group(tag)

        ### add the tag to the appropriate group, removing it from previous one
        if key is None:
            self._tags[0].add(tag)
        else:
            self._tags[key+1].add(tag)
            self._tags[key  ].discard(tag)

        ### delete the group if it became empty
        if key is not None and len(self._tags[key]) == 0:
            del self._tags[key]


    def select(self):
        """
        Randomly select a tag from amonght the least-replicated tags.
        """
        if len(self) > 0:
            count  = 1
            minkey = min(self._tags.keys())
            assert len(self._tags[minkey]) > 0, str(minkey) + ', ' + str(self._tags[minkey])
            return random.sample(self._tags[minkey], count)[0]
        else:
            return None

    def discard(self, tag):
        """
        Remove the tag from the collection
        """
        key = self._find_tag_group(tag)
        if key is not None:
            self._tags[key].discard(tag)

    def __len__(self):
        return reduce(lambda s, k: s + len(self._tags[k]), self._tags.iterkeys(), 0 )

    def __str__(self):
        d = dict([(k,len(s)) for k,s in self._tags.iteritems()])
        return '<TagSet(maxreps=%s): %s>' % (self._maxreps, d)

class WorkQueue(decorator.WorkQueue):
    """
    A decorated WorkQueue class.
    Supports all attributes of the underlying WorkQueue with the addition of the `replicate` method.

    This class supports replicating tasks when the available resources outnumber the running tasks.
    When replication is possible a the current running tasks are grouped by their replication count.
    A task is then chosen at random from the group with the smalled count an submited as a new task.
    A call to `replicate` will choose tasks and submit them until the queue is full.
    """

    def __init__(self, q, maxreplicas=1):
        super(WorkQueue, self).__init__(q)
        self._tags = _TagSet(maxreps=maxreplicas)
        self._tasks = dict() # tag -> Task

    ################################################################################ WQ API

    def submit(self, task):
        self._tags.add(task.tag)
        self._tasks[task.tag] = task
        return super(WorkQueue, self).submit(task)

    def wait(self, *args, **kws):
        task = super(WorkQueue, self).wait(*args, **kws)
        if task and task.result == 0:
            self._cancel(task)
        return task

    ################################################################################ Replication

    def _tasks_in_queue(self):
        """
        Return the number of tasks in the queue:

        t_q = t_running + t_waiting
        """
        return self.stats.tasks_running + self.stats.tasks_waiting

    def _active_workers(self):
        """
        Return the number of workers connected

        w_active = w_busy + w_ready
        """
        return self.stats.workers_busy + self.stats.workers_ready 

    def _can_duplicate_tasks(self):
        """
        Determine if the queue can support task replication.

        A queue supports replication when there is more resources than
        work (t_q < w_active) and there are tasks that haven't been
        maximally replicated.
        """
        return  self._tasks_in_queue() < self._active_workers() \
            and self._tags.can_duplicate()

    def _cancel(self, task):
        """
        Cancels all tasks with the same tag.
        Returns the number of tasks canceled.
        """
        count = 0
        self._tags.discard(task.tag)
        while self.cancel_by_tasktag(task.tag):
            count += 1
        return count


    ################################################################################ Replication API

    def replicate(self):
        """
        Replicate all candidate tasks.
        Returns the number of tasks replicated.
        """
        count = 0
        while self._can_duplicate_tasks():
            tag = self._tags.select()
            if tag is None: break
            task = self._tasks[tag].clone()
            self.submit(task)
            count += 1
        return count
