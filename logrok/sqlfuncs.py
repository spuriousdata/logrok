import ast

import parallel
import screen

__funcs__ = ['avg', ]

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
    if fields is None:
        raise SyntaxError("What fields are you selecting?")
    ast.fix_missing_locations(fields)
    code = compile(fields, '', 'eval')
    resp = []
    for line in __data__:
        for k in line.keys():
            locals()[k] = line[k]
        resp.append(eval(code))
    return resp

@parallel.map
def _fields(chunk, fields):
    _fields = []
    _funcs = []
    code = compile(syntree, '', 'eval')
    res = []

_avgs = {}

def avg(data, column):
    global _avgs
    if _avgs.has_key(column):
        return _avgs[column]
    vals = [row[column] for row in data]
    data = parallel.run(parallel.map(
        lambda chunk: [(sum([int(line) for line in chunk]), len(chunk))]), 
        vals
    )
    dividend = parallel.run(parallel.reduce(lambda data: sum([d[0] for d in data], 0.0)), data)
    divisor  = parallel.run(parallel.reduce(lambda data: sum([d[1] for d in data])), data)
    avg = sum(dividend)/sum(divisor)
    _avgs[column] = avg
    return avg
