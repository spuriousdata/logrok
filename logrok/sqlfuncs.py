import ast
from itertools import groupby as _groupby
import parallel
import screen
import util

def flatten(l):
    r = []
    for i in l:
        r += i
    return r


__funcs__ = ['avg', 'count', 'max', 'int', 'us_to_ms', 'ms_to_s', 'min']
__wholetable = False

def do(stmt, data):
    global  __wholetable
    __wholetable = False # reset

    d = data
    if stmt.where:
        d = where(stmt.where, d)
    if stmt.groupby:
        groups = []
        for k, g in _groupby(groupby(stmt.groupby, d), lambda x: x[stmt.groupby[0]]):
            groups.append(list(g))
        resp = []
        for group in groups:
            resp.append(fields(stmt.fields, group))
        d = flatten(resp)
    else:
        d = fields(stmt.fields, d)
    if stmt.orderby:
        if stmt.orderby[1] == 'desc':
            d = reversed(orderby(stmt.orderby[0], d))
        else:
            d = orderby(stmt.orderby[0], d)
    if stmt.limit:
        l = stmt.limit
        d = d[l[0]:l[0]+l[1]]

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

def groupby(fields, data):
    return orderby(fields, data)

def orderby(fields, data):
    newdata = []
    f = fields[0]
    buckets = util.radish_sort(f, data)
    for b in buckets.values():
        newdata += sorted(b, key=lambda x: x[f])
    try:
        return orderby(fields[1:], newdata)
    except IndexError:
        return newdata

def fields(fields, __data__):
    """
    Compile fields ast into executable code and run 
    a parallel 'filter' on the data with it
    """
    if fields is None:
        raise SyntaxError("What fields are you selecting?")
    ast.fix_missing_locations(fields)
    code = compile(fields, '', 'eval')
    resp = []
    for __line__ in __data__:
        for k in __line__.keys():
            locals()[k] = __line__[k]
        newrow = eval(code)
        if newrow.has_key('__line__'):
            newrow = newrow['__line__']
        resp.append(newrow)
        if __wholetable:
            break
    return resp

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
        vals,
        'avg()'
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

def micro_s_to_s(data, i):
    return i/1000000.0
