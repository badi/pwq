
import work_queue as ccl

from . import _wq

from pxul.os import ensure_dir
import pxul.StringIO as stringio

import base64
import copy
import os
import pwd
import shutil
import socket
import subprocess
import tempfile
import uuid
import gzip

class FileType:
    INPUT  = ccl.WORK_QUEUE_INPUT
    OUTPUT = ccl.WORK_QUEUE_OUTPUT

    @classmethod
    def str(cls, ftype):
        return {FileType.INPUT : 'INPUT',
                FileType.OUTPUT: 'OUTPUT'
            }[ftype]

class File(object):
    def __init__(self, local, remote=None, cache=True, filetype=None):
        assert filetype is not None
        self._local    = local
        self._remote   = remote if remote is not None else os.path.basename(local)
        self._cache    = cache
        self._filetype = filetype

    def __str__(self):
        return '<File local=%(local)r remote=%(remote)r cache=%(cache)s type=%(filetype)s>' % \
            dict(local    = self._local,
                 remote   = self._remote,
                 cache    = self._cache,
                 filetype = FileType.str(self._filetype))

    def __repr__(self):
        return 'File(%(local)r, remote=%(remote)r, cache=%(cache)r, filetype=%(filetype)s)' % \
            dict(local = self._local,
                 remote = self._remote,
                 cache = self._cache,
                 filetype = self._filetype)

    def to_yaml(self):
        with stringio.StringIO() as si:
            self.add_yaml(si)
            return si.getvalue()

    def add_yaml(self, builder):
        """Add the yaml representation of this File to string builder"""
        si = builder
        si.writeln('file:')
        si.indent()
        si.writeln('local:  %s' % self._local)
        si.writeln('remote:  %s' % self._remote)
        si.writeln('cache:  %s' % self._cache)
        si.writeln('filetype:  %s' % self._filetype)
        si.dedent()

    @property
    def local(self):
        """Name of the file on the local machine"""
        return self._local

    @local.setter
    def local(self, newname):
        self._local = newname

    @property
    def remote(self):
        """Name of the file on the worker"""
        return self._remote

    @property
    def cached(self):
        """Is this file to be cached on the remote machine?"""
        return self._cache

    @property
    def type(self):
        """The file type"""
        return self._filetype

    def add_to_task(self, task):
        """
        Add this file a `work_queue.Task`
        """
        task.specify_file(self.local, remote_name=self.remote, type=self.type, cache=self.cached)

class Buffer(object):
    def __init__(self, data, remote, cache=True):
        self._data = data
        self._remote = remote
        self._cache = cache

    @property
    def data(self):
        """The data to materialize on the worker"""
        return self._data

    @property
    def remote(self):
        """The file name on the worker containing this data"""
        return self._remote

    @property
    def cached(self):
        """Cache this data on the worker?"""
        return self._cache

    def add_to_task(self, task):
        """Add this buffer to a `work_queue.Task`"""
        task.specify_buffer(self._data, self._remote, cache=self._cache)

    def add_yaml(self, builder):
        si = builder
        si.writeln('buffer:')
        si.indent()
        si.writeln('data: !!binary |')
        si.indent()

        # .data
        data = base64.encodestring(self._data).split('\n')
        fill = '\n' + (si.indentlvl * ' ')
        binary = fill.join(data)
        si.writeln(binary)
        si.dedent()

        # .remote
        si.writeln('remote: %s' % self._remote)

        # .cache
        si.writeln('cache: %s' % self._cache)


class Schedule:
    """An `enum` of scheduling algorithms"""
    FCFS  = ccl.WORK_QUEUE_SCHEDULE_FCFS
    FILES = ccl.WORK_QUEUE_SCHEDULE_FILES
    TIME  = ccl.WORK_QUEUE_SCHEDULE_TIME
    RAND  = ccl.WORK_QUEUE_SCHEDULE_RAND

class Taskable(object):
    """
    Interface for objects that can have pwq.workqueue.Task` representations
    """

    def uuid(self):
        """
        Return a str of `uuid.UUID`
        """

    def to_task(self):
        """Convert the current object to a `Task`"""
        raise NotImplemented

    def update_task(self, obj):
        """Update the current object with the values from a `Task`"""
        raise NotImplemented

class TaskStatsLogger(object):
    def __init__(self, path, mode='a', delimiter=','):
        self._path = path
        self._mode = mode
        self.delim = delimiter

        attrs = 'result send_input_start send_input_stop cmd_execution_time'.split()
        attrs+= 'receive_output_start receive_output_stop transfer_time bytes_transferred'.split()
        self.attrs  = attrs
        self.header = self.delim.join(attrs) + '\n'

        ensure_dir(os.path.dirname(self._path))
        self.write(self.header)

    def write(self, string):
        with gzip.open(self._path, self._mode) as fd:
            fd.write(string)

    def process(self, task):
        vals = []
        for attr in self.attrs:
            val = getattr(task, attr)
            vals.append(str(val))
        string = self.delim.join(vals) + '\n'
        self.write(string)

class Task(object):
    """
    A pure python description of a task mirroring the `work_queue.Task` API
    """
    def __init__(self, command):
        self._command = command
        self._files = list()
        self._named_files = dict()
        self._buffers = list()
        self._uuid = uuid.uuid1()

        self._algorithm = Schedule.FCFS
        self._id = None
        self._output = ''
        self._result = -1
        self._success = False
        self._send_input_start = -1
        self._send_input_stop  = -1
        self._execute_cmd_start = -1
        self._execute_cmd_stop  = -1
        self._cmd_execution_time = -1
        self._receive_output_start = -1
        self._receive_output_stop  = -1
        self._bytes_received = -1
        self._bytes_sent = -1
        self._bytes_transfered = -1
        self._transfer_time = -1


    ################################################################################ WQ API wrapper

    def clone(self):
        """Create a copy of this task that may be submitted"""
        return copy.deepcopy(self)

    def specify_algorithm(self, alg):
        self._algorithm = alg

    def specify_buffer(self, string, remote, cache=True):
        self._buffers.append(Buffer(string, remote, cache=cache))

    def specify_file(self, local, remote=None, filetype=None, cache=True, name=None):
        f = File(local, remote=remote, cache=cache, filetype=filetype)
        self._files.append(f)
        if name is not None:
            self._named_files[name] = f

    def specify_input_file(self, local, remote=None, cache=True, name=None):
        self.specify_file(local, remote=remote, cache=cache, name=name, filetype=FileType.INPUT)

    def specify_output_file(self, local, remote=None, cache=True, name=None):
        self.specify_file(local, remote=remote, cache=cache, name=name, filetype=FileType.OUTPUT)

    def specify_tag(self, tag):
        """Tags are used internally so user use is disallowed"""
        raise ValueError, 'Tags are used internally so user use is disallowed'

    @property
    def id(self):
        return self._id

    @property
    def command(self):
        """The command to execute"""
        return self._command

    @property
    def tag(self): return self._tag

    @property
    def output(self):
        return self._output

    @property
    def result(self):
        return self._result

    @property
    def success(self):
        "Indicates if the task completed successfully and transferred all result files"
        return self._success

    @property
    def send_input_start(self):
        "The time when the task started to transfer input files"
        return self._send_input_start

    @property
    def send_input_stop(self):
        "The time when the task stopped transferring input files"
        return self._send_input_stop

    @property
    def execute_cmd_start(self):
        "The time when the task began executing"
        return self._execute_cmd_start

    @property
    def execute_cmd_stop(self):
        "The time when the task finished executing"
        return self._execute_cmd_stop

    @property
    def cmd_execution_time(self):
        "The time (in microseconds) spent executing the command"
        return self._cmd_execution_time

    @property
    def receive_output_start(self):
        "The time when the task started tranferring output files"
        return self._receive_output_start

    @property
    def receive_output_stop(self):
        "The time when the task stopped transferring output files"
        return self._receive_output_stop

    @property
    def receive_output_time(self):
        "The time spent transferring output files"
        return self._receive_output_stop - self._receive_output_start

    @property
    def bytes_received(self):
        "The number of bytes received since the task came online"
        return self._bytes_received

    @property
    def bytes_sent(self):
        "The number of bytes sent since the task came online"
        return self._bytes_sent

    @property
    def bytes_transferred(self):
        "The total number of bytes transferred"
        return self._bytes_transferred

    @property
    def transfer_time(self):
        "The total amount of time (in microseconds) spent transferring data"
        return self._transfer_time

    def _filter_files_by(self, filetype):
        return filter(lambda f: f.type == filetype, self._files)

    ################################################################################

    @property
    def uuid(self): return str(self._uuid)

    @property
    def algorithm(self):
        return self._algorithm

    @property
    def input_files(self):
        return self._filter_files_by(FileType.INPUT) + self._buffers

    @property
    def output_files(self):
        return self._filter_files_by(FileType.OUTPUT)

    @property
    def buffers(self):
        return self._buffers

    @property
    def files(self):
        """The list of input and output files"""
        return self.input_files + self.output_files

    @property
    def named_files(self):
        return self._named_files

    def to_yaml(self):
        """
        Represent this Task as a yaml string
        """
        with stringio.StringIO() as si:
            si.writeln('task:')
            si.indent()
            si.writeln('command: %s' % self.command)
            si.writeln('uuid: %s' % self.uuid)
            si.writeln('algorithm: %s' % self.algorithm)
            si.writeln('id: %s' % self.id)
            si.writeln('result: %s' % self.result)

            if self._files:
                si.writeln('files:')
                si.indent()
                for f in self._files:
                    si.writeln('- ')
                    si.indent()
                    f.add_yaml(si)
                    si.dedent()
                si.dedent()

            if self._buffers:
                si.writeln('buffers:')
                si.indent()
                for b in self._buffers:
                    si.writeln('-')
                    si.indent()
                    b.add_yaml(si)
                    si.dedent()
                si.dedent()

            return si.getvalue()

    def __str__(self):
        return self.to_yaml()

    ################################################################################ To WQ Tasks

    def to_ccl_task(self):
        """
        Return a `work_queue.Task` object that can be submitted to a `work_queue.WorkQueue`
        """
        task = ccl.Task(self.command)
        for f in self._files:
            f.add_to_task(task)
        task.specify_algorithm(self.algorithm)
        for b in self._buffers:
            b.add_to_task(task)
        task.specify_tag(self.uuid)
        return task

    def from_ccl_task(self, ccl_task):
        """Update (in-place) this task the the result of running a task on a WorkQueue Worker"""
        self._id     = ccl_task.id
        self._output = ccl_task.output
        self._result = ccl_task.result
        self._success = ccl_task.result == 0 and ccl_task.return_status == 0
        self._send_input_start = ccl_task.send_input_start
        self._send_input_stop  = ccl_task.send_input_finish
        self._execute_cmd_start = ccl_task.execute_cmd_start
        self._execute_cmd_stop  = ccl_task.execute_cmd_finish
        self._cmd_execution_time = ccl_task.cmd_execution_time
        self._receive_output_start = ccl_task.receive_output_start
        self._receive_output_stop  = ccl_task.receive_output_finish
        self._bytes_transferred = ccl_task.total_bytes_transferred
        self._transfer_time = ccl_task.total_transfer_time

        try:
            # only in CCTools version 4
            self._bytes_received = ccl_task.total_bytes_received
            self._bytes_sent     = ccl_task.total_bytes_sent
        except AttributeError: pass

        return self

class WorkerEmulator(object):
    """
    Emulate a work_queue_worker environment, allowing Tasks to be run directly.
    Mainly indended for debugging purposes

    E.g.
    >>> t = Task('echo hello world')
    >>> worker = WorkerEmulator()
    >>> worker(t)
    >>> print t.output
    """

    def __init__(self):
        self._workarea = tempfile.mkdtemp()
        print self.__class__.__name__,'working in:', self._workarea

    def _copy_path(self, src, dst):
        if   os.path.isfile(src): shutil.copy    (src, dst)
        elif os.path.isdir (src): shutil.copytree(src, dst)
        else: raise ValueError, '%s is neither file nor directory' % src

    def _unlink_path(self, path):
        if   os.path.isfile(path): os.unlink    (path)
        elif os.path.isdir(path) : shutil.rmtree(path)
        else: raise ValueError, '%s is neither file nor directory' % path

    def path(self, remote):
        return os.path.join(self._workarea, remote)

    def put(self, obj):
        if isinstance(obj, File):
            self.put_file(obj)
        elif isinstance(obj, Buffer):
            self.put_buffer(obj)
        else: raise ValueError, 'Unknow type %s' % type(obj)

    def put_buffer(self, buffer):
        dst = self.path(buffer.remote)
        with open(dst, 'w') as fd:
            fd.write(buffer.data)

    def put_file(self, file):
        src = file.local
        dst = os.path.join(self._workarea, file.remote)
        self._copy_path(src, dst)

    def run(self, command):
        p = subprocess.Popen(command, shell=True,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             cwd=self._workarea)
        out, err = p.communicate()
        output = out + err
        if p.returncode is not 0:
            raise ValueError, output
            # raise subprocess.CalledProcessError(p.returncode, command, output)
        return output, p.returncode

    def get(self, file):
        src = self.path(file.remote)
        dst = file.local
        self._copy_path(src, dst)

    def clear(self, file):
        if not file.cached:
            self._unlink_path(self.path(file.remote))

    def __call__(self, task):
        # put input files
        for file in task.input_files:
            self.put(file)

        # run
        task._output, task._result = self.run(task.command)

        # get output files
        for file in task.output_files:
            self.get(file)

        # cleanup
        for file in task.files:
            self.clear(file)


    def __del__(self):
        import shutil
        shutil.rmtree(self._workarea)

class MkWorkQueue(object):
    """
    Builder for creating WorkQueue object
    Call the factory instance to return the WorkQueue instance
    E.g.
      mk = MkWorkQueue()
      mk.port(1234).name('hello').catalog().debug_all().logfile()
      q = mk()
      for t in <tasks>: q.submit(t)
      ...
    """

    def __init__(self):
        self._port = 9123
        self._name = None
        self._catalog = False
        self._exclusive = True
        self._shutdown = False
        self._fast_abort = -1
        self._debug = None
        self._logfile = None

        # additions
        self._replicate = None

    def __call__(self):

        ################################################## Debugging
        if self._debug is not None:
            ccl.set_debug_flag(self._debug)

        ################################################## Vanilla WorkQueue
        kws = dict()
        kws['port'] = self._port
        kws['catalog'] = self._catalog
        if self._name is not None:
            kws['name'] = self._name
        kws['exclusive'] = self._exclusive
        kws['shutdown'] = self._shutdown

        q = ccl.WorkQueue(**kws)

        q.activate_fast_abort(self._fast_abort)
        if self._logfile is not None:
            q.specify_log(self._logfile)

        ################################################## Task Replication
        if self._replicate is not None:
            q = _wq.replication.WorkQueue(q, maxreplicas=self._replicate)

        return q

    def port(self, port=None):
        """
        no arg: choose random port
        port: int
        """
        p = -1 if port is None else port
        self._port = p
        return self
        
    def name(self, name=None):
        """
        Give a name to the WorkQueue.
        If none: default to "<hostname>-<username>-<pid>"
        """
        if name is None:
            host = socket.gethostname()
            user = pwd.getpwuid(os.getuid()).pw_name
            pid  = os.getpid()
            name = '%s-%s-%s' % (host, user, pid)
        self._name = name
        return self

    def catalog(self, catalog=True):
        """
        Set catalog mode
        """
        self._catalog = catalog
        return self

    def exclusive(self, exclusive=None):
        """
        no args: toggle exclusivity
        exclusive: bool
        """
        if exclusive is None:
            self._exclusive = self._exclusive or not self._exclusive
        else:
            self._exclusive = exclusive
        return self

    def shutdown(self, shutdown=None):
        """
        no args: toggle automatic shutdown of workers when work queue finished
        shutdown: bool
        """
        if shutdown is None:
            self._shutdown = self._shutdown or not self._shutdown
        else:
            self._shutdown = shutdown
        return self

    def fast_abort(self, multiplier=None):
        """
        no arg: toggle between inactive and 3
        multiplier: float
        """
        if multiplier is None:
            if self._fast_abort < 0:
                self._fast_abort = 3
            else:
                self._fast_abort = -1
        else:
            self._fast_abort = multiplier
        return self

    def debug(self, debug='all'):
        self._debug = debug
        return self

    def debug_all(self):
        return self.debug('all')

    def debug_wq(self):
        return self.debug('wq')

    def logfile(self, logfile='wq.log'):
        self._logfile = logfile
        return self

    def replicate(self, maxreplicates=9):
        """
        Use task replication.
        """
        self._replicate = maxreplicates
        return self


def WorkQueue(builder):
    """
    Create a WorkQueue using a `MkWorkQueue` builder
    """
    q = _wq.zeromq.WorkQueue(builder)
    return q
