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
        print 'Got:', msg, args, kws

        if msg == 'submit':
            task = args[0]
            ccl_task = task.to_task()
            taskid = wq.submit(ccl_task)
            task_table[taskid] = task
            socket.send_pyobj(('ok', taskid))

        elif msg == 'wait':
            ccl_task = wq.wait(*args, **kws)
            task     = task_table[ccl_task.id]
            del task_table[ccl_task.id]
            socket.send_pyobj(('ok', task))

        elif msg == 'empty':
            empty = wq.empty()
            socket.send_pyobj(('ok', empty))

        elif msg == 'stop':
            socket.send_pyobj(('ok', ()))
            break

        else:
            attr = getattr(wq, msg)
            if hasattr(attr, '__call__'):
                result = attr(*args, **kws)
            else:
                result = attr
            socket.send_pyobj(('ok', result))

class WorkQueue(object):
    def __init__(self, builder, ctx):
        self._socket = ctx.socket(zmq.PAIR)
        zport = self._socket.bind_to_random_port('tcp://*')
        self._process = multiprocessing.Process(target=_start_work_queue, args=(zport, builder))
        self._process.start()

    def __del__(self):
        self._socket.send_pyobj(('stop', ()))
        msg, result = self._socket.recv_pyobj()
        self._process.terminate()

    def submit(self, *args, **kws):
        self._socket.send_pyobj(('submit', args, kws))
        msg, result = self._socket.recv_pyobj()
        if msg == 'ok': return result
        else: raise ValueError, 'ZQ error "submit" %s, %s' % (msg, result)

    def wait(self, *args, **kws):
        self._socket.send_pyobj(('wait', args, kws))
        msg, result = self._socket.recv_pyobj()
        if msg == 'ok': return result
        else: raise ValueError, 'ZQ error "wait" %s, %s' % (msg, result)

    def stop(self, *args, **kws):
        self._socket.send_pyobj(('stop', args, kws))
        msg, result = self._socket.recv_pyobj()
        if msg == 'ok': return
        else: raise ValueError, 'ZQ error "stop": %s, %s' % (msg, result)


