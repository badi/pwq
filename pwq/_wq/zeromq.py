from . import decorator

import zmq
import multiprocessing

def _start_work_queue(zport, builder):
    wq = builder()

    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.connect("tcp://0.0.0.0:%s" % zport)

    task_table = dict()

    while True:
        msg, args, kws = socket.recv_pyobj()
        # print 'Got:', msg, args, kws

        if msg == 'submit':
            task = args[0]
            wq.submit(task)
            task_table[task.uuid] = task
            socket.send_pyobj(('ok', task.uuid))

        elif msg == 'wait':
            task = wq.wait(*args, **kws)
            if task:
                taskable = task_table[task.uuid]
                taskable.from_ccl_task(task)
                del task_table[task.uuid]
            else:
                taskable = None
            socket.send_pyobj(('ok', taskable))

        elif msg == 'empty':
            empty = wq.empty()
            socket.send_pyobj(('ok', empty))

        elif msg == 'stop':
            socket.send_pyobj(('ok', ()))
            return

        else:
            attr = getattr(wq, msg)
            if hasattr(attr, '__call__'):
                result = attr(*args, **kws)
            else:
                result = attr
            socket.send_pyobj(('ok', result))

class WorkQueue(object):
    def __init__(self, builder):
        self._ctx = zmq.Context()
        self._socket = self._ctx.socket(zmq.PAIR)
        zport = self._socket.bind_to_random_port('tcp://*')
        self._process = multiprocessing.Process(target=_start_work_queue, args=(zport, builder))
        self._process.start()

    def __del__(self):
        self._socket.send_pyobj(('stop', ()))
        msg, result = self._socket.recv_pyobj()
        self._process.terminate()

    def _zmq_remote_call(self, name, args=None, kws=None):
        args = args or ()
        kws  = kws  or {}
        self._socket.send_pyobj((name, args, kws))
        msg, result = self._socket.recv_pyobj()
        if msg == 'ok': return result
        else: raise ValueError, 'ZMQ error" %s(%s, args=%s, kws=%s": %s, %s' % (name, args, kws, msg, result)

    def submit(self, *args, **kws):
        return self._zmq_remote_call('submit', args, kws)

    def wait(self, *args, **kws):
        return self._zmq_remote_call('wait', args, kws)

    def stop(self, *args, **kws):
        return self._zmq_remote_call('stop')

    def empty(self):
        return self._zmq_remote_call('empty')

    def specify_log(self, *args, **kws):
        return self._zmq_remote_call('specify_log', args, kws)

    def replicate(self):
        return self._zmq_remote_call('replicate')
