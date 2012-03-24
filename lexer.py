from ply import lex

from util import sqlerror

DEBUG = False

_keywords = {
    'select':'SELECT',
    'avg':'F_AVG',
    'max':'F_MAX',
    'min':'F_MIN',
    'count':'F_COUNT',
    'from':'FROM',
    'where':'WHERE',
    'between':'BETWEEN',
    'group':'GROUP',
    'order':'ORDER',
    'by' : 'BY',
    'limit':'LIMIT',
    'and':'AND',
    'or' : 'OR',
    'in' : 'IN',
    'asc': 'ASC',
    'desc': 'DESC',
}

tokens = [
    'STAR',
    'LPAREN',
    'RPAREN',
    'STRING',
    'IDENTIFIER',
    'COMMA',
    'OPERATOR',
    'INTEGER',
] + _keywords.values()

t_ignore = ' '

t_STAR      = r'\*'
t_LPAREN    = r'\('
t_RPAREN    = r'\)'
t_COMMA     = r','
t_OPERATOR  = r'=|<>|<|>'
t_INTEGER   = r'\d+'

def t_IDENTIFIER(t):
    r'[\w][\w\.\-]*'
    t.type = _keywords.get(t.value.lower(), 'IDENTIFIER')
    return t

def t_error(t):
    sqlerror(t)

def t_STRING(t):
    r'(\"([^\\"]|(\\.))*\'|\'([^\\\']|(\\.))*\')'
    s = t.value[1:-1] # cut off quotes
    read_backslash = False
    output = ''
    for i in xrange(0, len(s)):
        c = s[i]
        if read_backslash:
            if c == 'n':
                c = '\n'
            elif c == 't':
                c = '\t'
            output += c
            read_backslash = False
        else:
            if c == '\\':
                read_backslash = True
            else:
                output += c
    t.value = output
    return t

def getlexer():
    lexer = lex.lex(debug=DEBUG)

