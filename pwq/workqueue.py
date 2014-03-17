
import work_queue as ccl

from . import _wq
from . import stringio

import base64
import copy
import os
import pwd
import shutil
import socket
import subprocess
import tempfile
import uuid

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
        si = stringio.StringIO()
        self.add_yaml(si)
        s = si.getvalue()
        si.close()
        return s

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

class Task(object):
    """
    A pure python description of a task mirroring the `work_queue.Task` API
    """
    def __init__(self, command):
        self._command = command
        self._files = list()
        self._named_files = dict()
        self._algorithm = Schedule.FCFS
        self._buffers = list()
        self._tag = None
        self._output = ''
        self._result = -1
        self._uuid = uuid.uuid1()

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
        self._tag = tag

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
        si = stringio.StringIO()
        si.writeln('task:')
        si.indent()
        si.writeln('command: %s' % self.command)

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

        s = si.getvalue()
        si.close()
        return s

    def __str__(self):
        return self.to_yaml()

    ################################################################################ To WQ Tasks

    def to_task(self):
        """
        Return a `work_queue.Task` object that can be submitted to a `work_queue.WorkQueue`
        """
        task = ccl.Task(self.command)
        for f in self._files:
            f.add_to_task(task)
        task.specify_algorithm(self.algorithm)
        for b in self._buffers:
            b.add_to_task(task)
        if self._tag is not None:
            task.specify_tag(self._tag)
        return task

    def from_task(self, ccl_task):
        """Update (in-place) this task the the result of running a task on a WorkQueue Worker"""
        self._output = ccl_task.output
        self._result = ccl_task.result

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
    Factory for creating WorkQueue object
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

        # mdq additions
        self._replicate = None
        self._generations = False

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

        ################################################## Generational Tasks
        if self._generations:
            q = _wq.generational.WorkQueue(q, self._generations)

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

    def generations(self, generations=True):
        self._generations = generations
        return self


def WorkQueue(builder):
    import zmq
    ctx = zmq.Context()
    q = _wq.zeromq.WorkQueue(builder, ctx)
    return q
