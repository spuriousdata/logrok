import re
import itertools
import time
from logformat import FORMAT, Regex

class NoTokenError(SyntaxError): pass

def sqlerror(t):
    if t is None:
        raise NoTokenError("Unknown Error in query: ")
    print "Syntax Error '%s' at position %d" % (t.value, t.lexpos)
    print ("\t" + t.lexer.lexdata.replace('\t', ' ')) # Tabs are expanded to spaces when they're printed to the terminal
    carrotline = "\t"
    for i in xrange(1, t.lexpos+1):
        carrotline += " "
    carrotline += '^'*len(t.value)
    print carrotline
    raise SyntaxError()

class ChunkableList(list):
    def chunks(self, size):
        for i in xrange(0, len(self), size):
            yield self[i:i+size]

class ColSizes(object):
    sizes = {}
    @classmethod
    def add(cls, key, value):
        if cls.sizes.get(key, None) is None:
            cls.sizes[key] = len(key)
        if len(value) > cls.sizes.get(key):
            cls.sizes[key] = len(value)

    @classmethod
    def get(cls, key):
        return cls.sizes[key]

def parse_format_string(fmt):
    """ simple LALR scanner/parser for format string """
    output = r'^'
    state = None 
    sq_state = None # start quote state
    flen = len(fmt)
    condition = re.compile(r'([!,\d\\]+)')
    name = re.compile(r'\{([^\}]+)}')
    capname = None
    i = 0
    while True:
        if i == flen: break

        c = fmt[i]
        try:
            nxt = fmt[i+1]
        except IndexError:
            nxt = None

        if c == '%':
            state = c
            i += 1
            c = fmt[i]
            nxt = fmt[i+1]

        if state != '%':
            if nxt == '%' and c not in (' ', '\t'):
                sq_state = c
                i += 1
                continue
            else:
                output += c
                i += 1
                continue
        if state == '%':
            if c in FORMAT:
                if sq_state is not None:
                    # this value is quoted, so we'll use dstring()
                    if not capname:
                        capname = FORMAT[c][1]
                    output += Regex.dstring(sq_state, nxt, nxt, name=capname)
                    sq_state = None
                    i += 1
                else:
                    if not capname:
                        capname = FORMAT[c][1]
                    output += FORMAT[c][0](name=capname)
                i += 1
                state = None
                capname = None
                continue
            if c == '{':
                n = name.match(fmt[i:]).group(0)
                capname=n[1:-1].replace('-', '_').lower()
                i += len(n)
                continue
            if condition.match(c):
                # this is a 'conditional' log message
                cond = condition.match(fmt[i:]).group(0)
                i += len(cond) # jump ahead
                continue
            if c in ('>', '<'):
                # just skip these
                i += 1
                continue

            raise SyntaxError()
    return output + r'$'

def pretty_print(sq):
    oq = ""
    indent = 0
    for c in sq:
        if c in ('(', '['):
            indent += 1
            oq += c + ('\n%s' % ('    '*indent))
        elif c in (')', ']'):
            indent -= 1
            oq += ('\n%s' % ('    '*indent)) + c
        elif c == ',':
            oq += c + ('\n%s' % ('    '*indent))
        else:
            oq += c
    print oq

class Complete(object):
    def __init__(self, opts=[]):
        self.options = opts

    def addopts(self, opts=[]):
        self.options = sorted(self.options + opts)

    def complete(self, text, state):
        """
        print ""
        print "line buffer: %s" % readline.get_line_buffer()
        print "completion type %s:" % readline.get_completion_type()
        print "start = %d" % readline.get_begidx()
        print "end = %d" % readline.get_endidx()
        """
        if state == 0:
            if text:
                self.matches = [s for s in self.options if s and s.startswith(text)]
            else:
                self.matches = self.options[:]
        try:
            return self.matches[state]
        except IndexError:
            return None

class Table(object):
    def __init__(self, data, start_time):
        self.data = data
        self.start = start_time
        self.size_columns()

    def size_columns(self):
        self.columnsize = {}
        self.fmt = "|"
        for k in self.data.keys():
            size = max([len(str(x)) for x in self.data[k]+[k]])
            self.fmt += "%%%ds|" % size
            self.columnsize[k] = size

    def print_bar(self):
        keys = tuple(self.data.keys())
        width = len(self.fmt % keys)-2
        print "+%s+" % ('-'*width)

    def translate(self):
        """
        Translate self.data from key:(row1, row2, row3), key2:(row1, row2, row3)
        into (key, key2),(row1, row1), (row2, row2), (row3, row3)
        """
        return [tuple(self.data.keys())] + list(itertools.izip_longest(*self.data.values(), fillvalue='NULL'))

    def prnt(self):
        outdata = self.translate()
        headers = outdata.pop(0)
        self.print_bar()
        print self.fmt % headers
        self.print_bar()
        for row in outdata:
            print self.fmt % row
        self.print_bar()
        print "%d rows in set (%0.3f sec)" % (len(outdata), (time.time() - self.start))
