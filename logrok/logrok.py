#!/usr/bin/env python

"""Query and aggregate data from log files using SQL-like syntax"""

import sys

if sys.version < '2.7':
    print "%s requires python2 version 2.7 or higher" % sys.argv[0]
    sys.exit(1)

import argparse
import os
import re
from multiprocessing import cpu_count

from ply import yacc

import parser
import parallel
import screen
from logformat import TYPES
from util import NoTokenError, parse_format_string, Complete, Table, pretty_print

DEBUG = False
log_regex = None

class LogQuery(object):
    def __init__(self, data, query):
        self.data = data
        self.query = query
        try:
            self.ast = parser.parse(query)
        except NoTokenError, e:
            print "ERROR: %s" % e.message
            print query
            return
        except SyntaxError:
            return
        if DEBUG:
            # pretty-printer
            import ast
            sq = "Statement(fields=" + ', '.join([ast.dump(x) for x in self.ast.fields]) + ", frm=xx, where=" + ast.dump(self.ast.where) + ")"
            pretty_print(sq)
            print self.ast
            print ast.dump(self.ast.where)
        print '-'*screen.width
        self.run()
    
    def avg(self, column):
        """
        This is actually a reduce/reduce because both of the
        operations return a single aggregated value for the data set
        """
        vals = [v for row in self.data for v in row[column]]
        # reduce
        data = parallel.run(self._avg_reduce, vals)
        # reduce again
        dividend = parallel.run(parallel.reduce(lambda data: sum([d[0] for d in data], 0.0)))
        divisor  = parallel.run(parallel.reduce(lambda data: sum([d[1] for d in data])))
        avg = dividend/divisor
        return [avg]

    @parallel.reduce
    def _avg_reduce(self, chunk):
        numlines = len(chunk)
        total = sum([int(line) for line in chunk])
        return (total, numlines)

    def where(self):
        """ and or in boolean between """
        if self.ast.where is None:
            return
        where = self.ast.where
        if type(where) != list:
            pass

    def run(self):
        self.op_data = self.data[:] # COPY!!! 
        self.where()
        return
        """
        for item in self.what:
            lparen = item.find('(')
            if lparen != -1:
                rparen = item.find(')')
                if rparen == -1:
                    raise SyntaxError("Error at %s" % tok)
                func = item[:lparen]
                param = item[lparen+1:rparen]
                try:
                    f = getattr(self, func)
                except AttributeError:
                    raise SyntaxError("ERROR: function %s does not exist" % func)
                response[item] = f(param)
            else:
                response[item] = [row[item] for row in self.data]
        """
        self.op_data = None
        Table(response).prnt()

class LoGrok(object):
    def __init__(self, args, interactive=False, curses=False, chunksize=10000):
        if curses:
            screen.init_curses()
        else:
            screen.init_linebased()
        self.interactive = interactive
        self.args = args
        self.processed_rows = 0
        self.oldpct = 0
        self.data = []
        self.chunksize = chunksize
        self.complete = Complete()
        self.crunchlogs()
        self.interact()

    def crunchlogs(self):
        global log_regex
        if self.args.format is not None:
            logformat = self.args.format
        else:
            logformat = TYPES[self.args.type]

        print
        lines = []
        for logfile in self.args.logfile:
            screen.print_mutable("Reading lines from %s:" % logfile.name)
            lines += logfile.readlines()
            screen.print_mutable("Reading lines from %s: %d" % (logfile.name, len(lines)))
            logfile.close()
            screen.print_mutable("", True)


        log_regex = re.compile(parse_format_string(logformat))
        if self.args.lines:
            lines = lines[:self.args.lines]
        self.data = parallel.run(log_match, lines)

    def interact(self):
        if screen.is_curses():
            screen.draw_curses_screen(self.data)
            self.main_loop()
        elif self.interactive:
            self.shell()
        else:
            print self.query(self.args.query)

    def shell(self):
        try:
            history = os.path.expanduser('~/.logrok_history')
            readline.read_history_file(history)
        except IOError:
            pass
        atexit.register(readline.write_history_file, history)
        readline.set_history_length(1000)
        readline.parse_and_bind('tab: complete')
        readline.set_completer(self.complete.complete)
        # XXX This is ugly and needs to be more intelligent. Ideally, the 
        #     completer would use readline.readline() to contextually switch out
        #     the returned matches
        self.complete.addopts(['select', 'from log', 'where', 'avg', 'max', 'min', 'count', 'between',
            'order by', 'group by', 'limit', ] + self.data[0].keys())
        while True:
            q = raw_input("logrok> ").strip()
            while not q.endswith(";"):
                q += raw_input("> ").strip()
            self.query(q)

    def query(self, query):
        semicolon = query.find(';')
        if semicolon != -1:
            query = query[:semicolon]
        if query in ('quit', 'bye', 'exit'):
            sys.exit(0)
        if query.startswith('help') or query.startswith('?'):
            answer = "Use sql syntax against your log, `from` clauses are ignored.\n"\
                     "Queries can span multiple lines and _must_ end in a semicolon `;`.\n"\
                     " Try: `show fields;` to see available field names. Press TAB at the\n"\
                     " beginning of a new line to see all available completions."
            return answer
        if query in ('show fields', 'show headers'):
            return ', '.join(self.data[0].keys())
        else:
            try:
                q = LogQuery(self.data, query)
                return q.run()
            except SyntaxError, e:
                return e.message
    
    def main_loop(self):
        while 1:
            c = screen.getch()
            if c == ord('x'): break
            if c == ord('q'): screen.prompt("QUERY:", self.query)

@parallel.map
def log_match(chunk):
    response = []
    for line in chunk:
        out = {}
        m = log_regex.match(line)
        for key in log_regex.groupindex:
            out[key] = m.group(key)
        response.append(out)
    return response

def main():
    cmd = argparse.ArgumentParser(description="Grok/Query/Aggregate log files. Requires python2 >= 2.7")
    typ = cmd.add_mutually_exclusive_group(required=True)
    typ.add_argument('-t', '--type', metavar='TYPE', choices=TYPES, help='{%s} Use built-in log type (default: apache-common)'%', '.join(TYPES), default='apache-common')
    typ.add_argument('-f', '--format', action='store', help='Log format (use apache LogFormat string)')
    cmd.add_argument('-j', '--processes', action='store', type=int, help='Number of processes to fork for log crunching (default: smart)', default=parallel.SMART)
    cmd.add_argument('-l', '--lines', action='store', type=int, help='Only process LINES lines of input')
    interactive = cmd.add_mutually_exclusive_group(required=False)
    interactive.add_argument('-i', '--interactive', action='store_true', help="Use line-based interactive interface")
    interactive.add_argument('-c', '--curses', action='store_true', help=argparse.SUPPRESS)
    interactive.add_argument('-q', '--query', help="The query to run")
    cmd.add_argument('-d', '--debug', action='store_true', help="Turn debugging on (you don't want this)")
    cmd.add_argument('logfile', nargs='+', type=argparse.FileType('r'), help="log(s) to parse/query")
    args = cmd.parse_args(sys.argv[1:])

    global DEBUG
    DEBUG = args.debug
    parser.DEBUG = DEBUG
    parser.init()

    parallel.numprocs = args.processes

    LoGrok(args, interactive=args.interactive, curses=args.curses)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        # TODO -- kill multiprocesses
        # TODO -- reset terminal if curses
        print
        sys.exit(1)
