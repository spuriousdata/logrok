from multiprocessing import Process, Queue, cpu_count
from functools import wraps

import screen
from util import ChunkableList

SMART=-1
numprocs=SMART

def map(f):
    @wraps(f)
    def wrapper(inq, outq):
        for chunk in iter(inq.get, 'ITER_STOP'):
            resp = f(chunk)
            for i in resp:
                outq.put(i)
    return wrapper

def reduce(f):
    @wraps(f)
    def wrapper(inq, outq):
        for chunk in iter(inq.get, 'ITER_STOP'):
            outq.put(f(chunk))
    return wrapper

class Job(object):
    def __init__(self, datalen):
        self.datalen = datalen
        self.processes = []
        self.in_queue = Queue()
        self.out_queue = Queue()
        self.processed_rows = 0
        self.pct_complete = 0

def run(func, data, chunksize=SMART, numprocs=numprocs, wait=True):
    if chunksize == SMART:
        chunksize = min(10000, len(data)/cpu_count())
        
    if numprocs == SMART:
        numprocs = min(int(cpu_count()*1.5), len(data)/chunksize)

    job = Job(len(data))
    _run(func, job, numprocs)
    _enqueue_data(data, chunksize, job)
    if not wait:
        return job
    return _wait(job)

def _wait(job):
    data = []
    while True:
        if _check_running(job):
            data += _get_data(job)
        else:
            break
    data += _get_data(job)
    screen.print_mutable("", True)
    return data

def _get_data(job):
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

def _run(func, job, numprocs):
    screen.print_line("Spawning %d processes to crunch data." % numprocs)
    for i in xrange(0, numprocs+1):
        proc = Process(target=func, args=(job.in_queue, job.out_queue))
        proc.start()
        job.processes.append(proc)
