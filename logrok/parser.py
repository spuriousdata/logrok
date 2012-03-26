import ast
from collections import namedtuple

from ply import yacc

from util import sqlerror
import lexer


DEBUG = False
tokens = lexer.tokens

Statement   = namedtuple('Statement',   ['fields', 'frm', 'where', 'groupby', 'orderby', 'limit'])
GroupBy     = namedtuple('GroupBy',     ['fields'])
OrderBy     = namedtuple('OrderBy',     ['fields', 'direction'])
Limit       = namedtuple('Limit',       ['value'])

precedence = (
        ('left', 'OPERATOR'),
    )

def p_error(p):
    sqlerror(p)

def p_statement(p):
    'statement : select fields from where group order limit'
    p[0] = Statement(p[2], p[3], p[4], p[5], p[6], p[7])

def p_select(p):
    '''select :
              | SELECT'''

def p_fields(p):
    'fields : field fieldlist'
    if p[2] is not None:
        p[0] = p[1] + p[2]
    else:
        p[0] = p[1]

def p_field(p):
    '''field : STAR
             | IDENTIFIER
             | INTEGER
             | STRING
             | function'''
    p[0] = [p[1]]

def p_fieldlist(p):
    '''fieldlist :
                 | COMMA field fieldlist'''
    if len(p) > 1:
        if p[3] != None:
            p[0] = p[2] + p[3]
        else:
            p[0] = p[2]

def p_function(p):
    'function : fname LPAREN field fieldlist RPAREN'
    if p[4] is not None:
        params = [p[3]] + p[4]
    else:
        params = [p[3]]
    p[0] = ast.Call(p[1], params)

def p_fname(p):
    '''fname : F_AVG
             | F_MAX
             | F_MIN
             | F_COUNT'''
    p[0] = ast.Name(p[1], ast.Load())

def p_from(p):
    '''from :
            | FROM IDENTIFIER'''

def p_where(p):
    '''where :
             | WHERE wherelist'''
    if len(p) > 1:
            p[0] = ast.Module(ast.Expr(p[2]))

def p_wherelist(p):
    '''wherelist : 
                 | wherexpr
                 | wherexpr AND wherelist
                 | wherexpr OR wherelist'''
    if len(p) == 4:
            rval = [p[1], p[3]]
            p[0] = ast.BoolOp(p[2], rval)
    else:
        if len(p) > 1:
            #p[0] = p[1], p[2]
            p[0] = p[1]

def p_wherexpr(p):
    '''wherexpr : whereval OPERATOR whereval
                | whereval IN inlist
                | whereval BETWEEN whereval AND whereval
                | wherexpr_grouped'''
    if len(p) == 4:
        # field = value
        lval = p[1]
        rval = p[3]
        p[0] = ast.Compare(
                    left=lval,
                    ops=[p[2]],
                    comparators=[rval]
                )
    elif len(p) == 6:
        # between 1 and 10
        # this is analogous to:
        #  p[1] >= p[3] and p[1] <= p[5]
        p[0] = ast.BoolOp(ast.And(), [
                ast.Compare(
                    p[1],
                    [ast.GtE()],
                    [p[3]]
                ),
                ast.Compare(
                    p[1],
                    [ast.LtE()],
                    [p[5]]
                )
            ])
    else:
        # group
        p[0] = p[1]

def p_inlist(p):
    'inlist : LPAREN initem initemlist RPAREN'
    if p[3] is not None:
        p[0] = ast.Tuple([p[2]] + p[3], ast.Load())
    else:
        p[0] = ast.Tuple([p[2]], ast.Load())

def p_initemlist(p):
    '''initemlist : 
                  | COMMA initem initemlist'''
    if len(p) > 1:
        if p[3] != None:
            p[0] = [p[2]] + p[3]
        else:
            p[0] = [p[2]]

def p_initem(p):
    '''initem : STRING
              | INTEGER
              | IDENTIFIER'''
    p[0] = p[1]

def p_wherexpr_grouped(p):
    'wherexpr_grouped : LPAREN wherelist RPAREN'
    p[0] = p[2]

def p_whereval(p):
    '''whereval : IDENTIFIER
                | INTEGER
                | STRING'''
    p[0] = p[1]

def p_group(p):
    '''group :
             | GROUP BY IDENTIFIER identlist'''
    if len(p) > 1:
        if p[4] is None:
            p[0] = GroupBy([p[3]])
        else:
            p[0] = GroupBy([p[3]] + p[4])

def p_order(p):
    '''order :
             | ORDER BY IDENTIFIER identlist direction'''
    if len(p) > 1:
        if p[4] is not None:
            fields = [p[3]] + p[4]
        else:
            fields = [p[3]]
        p[0] = OrderBy(fields, p[5])

def p_direction(p):
    '''direction :
                 | ASC
                 | DESC'''
    try:
        p[0] = p[1]
    except IndexError:
        p[0] = 'asc' # default to 'asc'

def p_identlist(p):
    '''identlist :
                 | COMMA IDENTIFIER identlist'''
    if len(p) > 1:
        if p[3] != None:
            p[0] = [p[2]] + p[3]
        else:
            p[0] = [p[2]]

def p_limit(p):
    '''limit :
             | LIMIT INTEGER'''
    if len(p) > 1:
        p[0] = Limit(p[2])

_parser = None
_lexer = None
def init():
    global _parser, _lexer
    lexer.DEBUG = DEBUG
    _lexer = lexer.getlexer()
    _parser = yacc.yacc(debug=DEBUG)

def parse(sql):
    return _parser.parse(sql, lexer=_lexer, debug=DEBUG)

