from multiprocessing import Process, Queue, cpu_count
from functools import wraps

import screen
from util import ChunkableList

DEBUG=False
SMART=-1
numprocs=SMART

def map(f):
    @wraps(f)
    def wrapper(**kwargs):
        inq=kwargs['inq']
        outq=kwargs['outq']
        del kwargs['inq']
        del kwargs['outq']
        for chunk in iter(inq.get, 'ITER_STOP'):
            resp = f(chunk, **kwargs)
            for i in resp:
                outq.put(i)
    return wrapper

def reduce(f):
    @wraps(f)
    def wrapper(**kwargs):
        inq=kwargs['inq']
        outq=kwargs['outq']
        del kwargs['inq']
        del kwargs['outq']
        for chunk in iter(inq.get, 'ITER_STOP'):
            outq.put(f(chunk, **kwargs))
    return wrapper

class Job(object):
    def __init__(self, datalen, name):
        self.datalen = datalen
        self.name = name
        self.processes = []
        self.in_queue = Queue()
        self.out_queue = Queue()
        self.processed_rows = 0
        self.pct_complete = 0

    def __del__(self):
        del self.in_queue
        del self.out_queue

def run(func, data, name="<main>", chunksize=SMART, numprocs=numprocs, wait=True, _print=False, **kwargs):
    l = len(data)
    if l < cpu_count():
        c = l
    else:
        c = l/cpu_count()
    if chunksize == SMART:
        chunksize = min(10000, c)
        
    if numprocs == SMART:
        if l < 1000:
            c = 1
        else:
            c = l/chunksize
        numprocs = min(int(cpu_count()*1.5), c)

    job = Job(len(data), name)
    _run(func, job, numprocs, _print, **kwargs)
    _enqueue_data(data, chunksize, job)
    if not wait:
        return job
    resp = _wait(job, _print)
    killall(job)
    del job
    return resp

def _wait(job, _print):
    data = []
    while True:
        if _check_running(job):
            data += _get_data(job, _print)
        else:
            break
    data += _get_data(job, _print)
    if DEBUG or _print:
        screen.print_mutable("", True)
    return data

def _get_data(job, _print):
    data = []
    while True:
        try:
            chunk = job.out_queue.get_nowait()
        except:
            break
        job.processed_rows +=1
        data.append(chunk)
    if job.processed_rows != 0:
        pct = int((float(job.processed_rows)/job.datalen) * 100)
        if pct != job.pct_complete:
            job.pct_complete = pct
            if DEBUG or _print:
                screen.print_mutable("Processing data... %d%%" % pct)
    return data

def _check_running(job):
    for p in job.processes:
        p.join(1)
        if p.exitcode is None:
            return True
    return False

def _enqueue_data(data, chunksize, job):
    for chunk in ChunkableList(data).chunks(chunksize):
        job.in_queue.put(chunk)
    for j in job.processes:
        job.in_queue.put('ITER_STOP')

def _run(func, job, numprocs, _print, **kwargs):
    global _procs
    if DEBUG or _print:
        screen.print_line("Spawning %d processes to crunch data for %s." % (numprocs, job.name))
    for i in xrange(0, numprocs+1):
        kwargs['inq'] = job.in_queue
        kwargs['outq'] = job.out_queue
        proc = Process(target=func, kwargs=kwargs)
        proc.start()
        job.processes.append(proc)

def killall(job):
    for p in job.processes:
        p.terminate()
    del job.processes[:]
