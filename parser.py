from collections import namedtuple

from ply import yacc

from util import sqlerror
import lexer


DEBUG = False
tokens = lexer.tokens

Statement   = namedtuple('Statement',   ['fields', 'frm', 'where', 'groupby', 'orderby', 'limit'])
Function    = namedtuple('Function',    ['name', 'args'])
Field       = namedtuple('Field',       ['name'])
Where       = namedtuple('Where',       ['predicates'])
And         = namedtuple('And',         ['lval', 'rval'])
Or          = namedtuple('Or',          ['lval', 'rval'])
In          = namedtuple('In',          ['field', 'inlist'])
Boolean     = namedtuple('Boolean',     ['lval', 'operator', 'rval'])
Between     = namedtuple('Between',     ['field', 'lval', 'rval'])
From        = namedtuple('From',        ['table'])
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
    p[0] = p[1] + p[2]

def p_field(p):
    '''field : STAR
             | IDENTIFIER
             | function'''
    p[0] = [Field(p[1])]

def p_fieldlist(p):
    '''fieldlist :
                 | COMMA field fieldlist'''
    if len(p) > 1:
        if p[3] != None:
            p[0] = p[2] + p[3]
        else:
            p[0] = p[2]

def p_function(p):
    'function : fname LPAREN IDENTIFIER RPAREN'
    p[0] = Function(p[1], p[3])

def p_fname(p):
    '''fname : F_AVG
             | F_MAX
             | F_MIN
             | F_COUNT'''
    p[0] = p[1]

def p_from(p):
    '''from :
            | FROM IDENTIFIER'''
    if len(p) > 1:
        return From(p[2])

def p_where(p):
    '''where :
             | WHERE wherelist'''
    if len(p) > 1:
            p[0] = Where(p[2])

def p_wherelist(p):
    '''wherelist : 
                 | wherexpr
                 | wherexpr AND wherelist
                 | wherexpr OR wherelist'''
    if len(p) == 4:
        if p[2].lower() == 'and':
            p[0] = And(p[1], p[3])
        else:
            p[0] = Or(p[1], p[3])
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
        if p[2].lower() == 'in':
            p[0] = In(p[1], p[3])
        else:
            p[0] = Boolean(p[1], p[2], p[3])
    elif len(p) == 6:
        p[0] = Between(p[1], p[3], p[5])
    else:
        p[0] = p[1]

def p_inlist(p):
    'inlist : LPAREN initem initemlist RPAREN'
    if p[3] is not None:
        p[0] = [p[2]] + p[3]
    else:
        p[0] = [p[2]]

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
    p[0] = (p[2],)

def p_whereval(p):
    '''whereval : IDENTIFIER
                | INTEGER
                | STRING'''
    p[0] = p[1]

def p_group(p):
    '''group :
             | GROUP BY IDENTIFIER identlist'''
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
        p[0] = 'asc'

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
             | LIMIT IDENTIFIER'''
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

