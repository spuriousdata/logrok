import ast
from itertools import groupby
import parallel
import screen
import util
import time
try:
    from collections import OrderedDict
except ImportError:
    # python < 2.7 compatability
    from compat.OrderedDict import OrderedDict
try:
    from collections import Counter
except ImportError:
    # python < 2.7 compatability
    from compat.Counter import Counter

DEBUG = False
__is_aggregate = False

def do(stmt, data):
    global  __is_aggregate
    __is_aggregate = False # reset

    d = data
    if stmt.where:
        d = _where(stmt.where, d)
    if stmt.groupby:
        groups = []
        for k, g in groupby(_groupby(stmt.groupby, d), lambda x: x[stmt.groupby[0]]):
            groups.append(list(g))
        resp = []
        for group in groups:
            resp.append(_fields(stmt.fields, group))
        d = _flatten(resp)
    else:
        d = _fields(stmt.fields, d)
    if stmt.orderby:
        if stmt.orderby[1] == 'desc':
            d = reversed(_orderby(stmt.orderby[0], d))
        else:
            d = _orderby(stmt.orderby[0], d)
    if stmt.limit:
        l = stmt.limit
        d = d[l[0]:l[0]+l[1]]

    return d

def _where(where, data):
    """
    Compile `where` ast into executable code and run 
    a parallel 'filter' on the data with it
    """
    if where is None:
        return
    ast.fix_missing_locations(where)
    return parallel.run(__where, data, "<where clause>", syntree=where)

@parallel.map
def __where(chunk, syntree):
    code = compile(syntree, '', 'eval')
    res = []
    for line in chunk:
        for k in line.keys():
            locals()[k] = line[k]
        if eval(code):
            res.append(line)
    return res

def _groupby(fields, data):
    return _orderby(fields, data, "<groupby>")

def _orderby(fields, data, name="<orderby>"):
    if DEBUG:
        print "starting sort for %s on %d lines" % (name, len(data))
    s = time.time()
    f = fields[0]
    newdata = sorted(data, key=lambda x: x[f])
    if DEBUG:
        print "sort for %s ran in %0.3f seconds" % (name, time.time() - s)
    return newdata

def _fields(fields, __data__):
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
        if __is_aggregate:
            break
    return resp

def _flatten(l):
    r = []
    for i in l:
        r += i
    return r

def count(data, i):
    global __is_aggregate
    __is_aggregate = True
    return len(data)

def avg(data, column):
    global __is_aggregate
    __is_aggregate = True
    vals = [row[column] for row in data]
    data = parallel.run(parallel.map(
        lambda chunk: [(sum([int(line) for line in chunk]), len(chunk))]), 
        vals,
        'avg()'
    )
    dividend = parallel.run(parallel.reduce(lambda data: sum([d[0] for d in data], 0.0)), data)
    divisor  = parallel.run(parallel.reduce(lambda data: sum([d[1] for d in data])), data)
    return sum(dividend)/sum(divisor)

def mean(data, column):
    return avg(data, column)

def median(data, column):
    global __is_aggregate
    __is_aggregate = True
    d = sorted(data, key=lambda x: x[column])
    if len(d) & 0x01:
        return data[(len(d)-1)/2]
    m = len(d)/2
    a, b = data[m-1:m+1]
    return (a[column]+b[column]/2)

def mode(data, column, ind=0):
    global __is_aggregate
    __is_aggregate = True
    ind = int(ind)
    return Counter([x[column] for x in data]).most_common(ind+1)[ind][0]

def max(data, column):
    global __is_aggregate
    __is_aggregate = True
    max = __builtins__['max']
    vals = [row[column] for row in data]
    return max(parallel.run(parallel.reduce(lambda chunk: max([int(i) for i in chunk])), vals))

def min(data, column):
    global __is_aggregate
    __is_aggregate = True
    min = __builtins__['min']
    vals = [row[column] for row in data]
    return min(parallel.run(parallel.reduce(lambda chunk: min([int(i) for i in chunk])), vals))

def div(data, a, b):
    try:
        a = int(a)
        b = int(b)
    except ValueError:
        a = float(a)
        b = float(b)
    return a/b

def year(data, d):
    if type(d) == str:
        d = data[d]
    else:
        d = str(d)
    return int(d[:4])

def month(data, d):
    if type(d) == str:
        d = data[d]
    else:
        d = str(d)
    return int(d[4:6])

def day(data, d):
    if type(d) == str:
        d = data[d]
    else:
        d = str(d)
    return int(d[6:8])

def hour(data, d):
    if type(d) == str:
        d = data[d]
    else:
        d = str(d)
    return int(d[8:10])

def minute(data, d):
    if type(d) == str:
        d = data[d]
    else:
        d = str(d)
    return int(d[10:12])

def second(data, d):
    if type(d) == str:
        d = data[d]
    else:
        d = str(d)
    return int(d[12:14])
