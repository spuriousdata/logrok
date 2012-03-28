import ast

import parallel
import screen

def where(where, data):
    """
    Compile `where` ast into executable code and run 
    a parallel 'filter' on the data with it
    """
    if where is None:
        return
    ast.fix_missing_locations(where)
    return parallel.run(_where, data, "<where clause>", ast=where)

def what(what, data):
    """
    Compile fields ast into executable code and run 
    a parallel 'filter' on the data with it
    """
    if len(what) == 0:
        raise SyntaxError("What fields are you selecting?")
    return parallel.run(_what, data, "<fields>", syntree=what)


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

@parallel.map
def _what(chunk, syntree):
    code = compile(syntree, '', 'eval')
    res = []

