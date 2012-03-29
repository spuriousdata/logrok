import ast
try:
    from itertools import chain as flatten
except:
    def flatten(l):
        r = []
        for i in l:
            r += i
        return r

import parallel
import screen

__funcs__ = ['avg', 'count', 'max', 'int', 'us_to_ms', 'ms_to_s', 'min']
__wholetable = False

def do(stmt, data):
    global  __wholetable
    __wholetable = False # reset

    d = data
    if stmt.where:
        d = where(stmt.where, d)
    if stmt.groupby:
        groups = groupby(stmt.groupby, d)
        resp = []
        for group in groups:
            resp.append(fields(stmt.fields, group))
        d = flatten(resp)
    else:
        d = fields(stmt.fields, d)

    return d

def where(where, data):
    """
    Compile `where` ast into executable code and run 
    a parallel 'filter' on the data with it
    """
    if where is None:
        return
    ast.fix_missing_locations(where)
    return parallel.run(_where, data, "<where clause>", syntree=where)

@parallel.map
def _where(chunk, syntree):
    code = compile(syntree, '', 'eval')
    res = []
    for line in chunk:
        for k in line.keys():
            locals()[k] = line[k]
        if eval(code):
            res.append(line)
    return res

def fields(fields, __data__):
    """
    Compile fields ast into executable code and run 
    a parallel 'filter' on the data with it
    """
    global __wholetable
    __wholetable = True
    if fields is None:
        raise SyntaxError("What fields are you selecting?")
    ast.fix_missing_locations(fields)
    code = compile(fields, '', 'eval')
    resp = []
    for line in __data__:
        for k in line.keys():
            locals()[k] = line[k]
        newrow = eval(code)
        resp.append(newrow)
        if __wholetable:
            break
    return resp

@parallel.map
def _fields(chunk, fields):
    _fields = []
    _funcs = []
    code = compile(syntree, '', 'eval')
    res = []

def count(data):
    global __wholetable
    __wholetable = True
    return len(data)

def int(data, i):
    return __builtins__['int'](i)

def avg(data, column):
    global __wholetable
    __wholetable = True
    int = __builtins__['int']
    vals = [row[column] for row in data]
    data = parallel.run(parallel.map(
        lambda chunk: [(sum([int(line) for line in chunk]), len(chunk))]), 
        vals
    )
    dividend = parallel.run(parallel.reduce(lambda data: sum([d[0] for d in data], 0.0)), data)
    divisor  = parallel.run(parallel.reduce(lambda data: sum([d[1] for d in data])), data)
    return sum(dividend)/sum(divisor)

def max(data, column):
    global __wholetable
    __wholetable = True
    max = __builtins__['max']
    int = __builtins__['int']
    vals = [row[column] for row in data]
    return max(parallel.run(parallel.reduce(lambda chunk: max([int(i) for i in chunk])), vals))

def min(data, column):
    global __wholetable
    __wholetable = True
    min = __builtins__['min']
    int = __builtins__['int']
    vals = [row[column] for row in data]
    return min(parallel.run(parallel.reduce(lambda chunk: min([int(i) for i in chunk])), vals))

def us_to_ms(data, i):
    return i/1000.0

def ms_to_s(data, i):
    return i/1000.0
