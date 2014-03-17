pwq
===

 Process-safe Python interface to the CCL WorkQueue library supporting multiple WorkQueue

# Dependencies

* [CCTools](https://github.com/badi/cctools/tree/3.7.X-badi)
* [Python](http://www.python.org)
* [PyYAML](http://pyyaml.org/)
* [PyZMQ](http://zeromq.github.io/pyzmq/#)


# Usage

```python
>>> import pwq
>>> mk = pwq.MkWorkQueue().port(9123).debug('all')
>>> q = pwq.WorkQueue(mk)
>>> t = pwq.Task('echo hello world')
>>> q.submit(t)
>>> q.wait(10) # then start a worker: $ work_queue_worker -d all localhost 9123
<pwq.workqueue.Taskat ...>
```
