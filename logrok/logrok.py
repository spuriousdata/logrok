#!/usr/bin/env python

"""Query and aggregate data from log files using SQL-like syntax"""

import sys

if sys.version < '2.7':
    print "%s requires python2 version 2.7 or higher" % sys.argv[0]
    sys.exit(1)

import argparse
import os
import re
import curses
import readline
import atexit
import fcntl
import termios
import struct
import signal
from multiprocessing import Process, cpu_count, Queue

from ply import yacc

import parser
from logformat import TYPES
from util import NoTokenError, ChunkableList, ColSizes, parse_format_string, Complete, Table

DEBUG = False

class LogQuery(object):
    def __init__(self, parent, data, query):
        self.parent = parent
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
            print self.ast
            print ast.dump(self.ast.where)
        print '-'*LoGrok.width
        self.run()
    
    def avg(self, column):
        vals = ChunkableList([v for row in self.data for v in row[column]])
        tasklist = []
        # map
        for chunk in vals.chunks(self.parent.chunksize):
            tasklist.append(chunk)
        data = self.parent.parallel(self._avg_mapper, tasklist, len(vals), wait=True)
        # reduce
        avg = sum([d[0] for d in data], 0.0) / sum([d[1] for d in data])
        return [avg]

    def _avg_mapper(self, inq, outq):
        for lines in iter(inq.get, 'STOP'):
            numlines = len(lines)
            total = sum([int(line) for line in lines])
            outq.put((total, numlines))

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
    width = 0
    height = 0
    fullwidth = ""
    curses = False
    def __init__(self, args, interactive=False, curses=False, chunksize=10000):
        if curses:
            LoGrok.curses = True
            #self.screen = screen
        self.interactive = interactive
        self.args = args
        self.processed_rows = 0
        self.oldpct = 0
        self.data = []
        self.chunksize = chunksize
        self.complete = Complete()
        LoGrok.setup_screen()
        self.crunchlogs()
        self.interact()

    @staticmethod
    def setup_screen():
        if LoGrok.curses:
            (LoGrok.height, LoGrok.width) = self.screen.getmaxyx()
        else:
            s = struct.pack("HHHH", 0, 0, 0, 0)
            (LoGrok.height, LoGrok.width, h, w) = struct.unpack("HHHH", fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, s))
            signal.signal(signal.SIGWINCH, LoGrok.sigwinch)
        LoGrok.fullwidth = "%%-%ds" % LoGrok.width

    @staticmethod
    def sigwinch(sig, frame):
        LoGrok.setup_screen()

    def crunchlogs(self):
        if self.args.format is not None:
            logformat = self.args.format
        else:
            logformat = TYPES[self.args.type]

        regex = parse_format_string(logformat)
        func = rx_closure(regex)
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.processes = []

        lines = ChunkableList()
        for logfile in self.args.logfile:
            self.print_header("   Reading lines from %s:" % logfile.name)
            lines += logfile.readlines()
            self.print_header("   Reading lines from %s: %d" % (logfile.name, len(lines)))
            logfile.close()

        self.end_header()

        if self.args.lines != None:
            lines = ChunkableList(lines[:self.args.lines])

        tasks = []
        for chunk in lines.chunks(self.chunksize):
            tasks.append(chunk)

        self.data = self.parallel(func, tasks, len(lines), wait=True)

    def parallel(self, func, tasks, tasklen, numprocs=None, wait=False):
        if numprocs == None:
            numprocs = min(self.args.processes, len(tasks))

        del self.processes[:]
        self.processed_rows = 0
        self.print_header("   Starting workers: %d" % numprocs, True)
        for proc in xrange(0, numprocs+1):
            p = Process(target=func, args=(self.task_queue, self.result_queue))
            p.start()
            self.processes.append(p)
        for job in tasks:
            self.task_queue.put(job)
        for p in self.processes:
            self.task_queue.put('STOP')
        if wait:
            data = []
            while True:
                if self.check_running():
                    data += self.get_data(tasklen)
                else:
                    break
            data += self.get_data(tasklen)
            return data
        return None


    def print_header(self, s, end=False):
        if LoGrok.curses:
            self.screen.addstr(1, 0, LoGrok.fullwidth % s, curses.A_STANDOUT)
            self.screen.refresh()
        else:
            sys.stdout.write("\r%s" % (LoGrok.fullwidth % s))
            sys.stdout.flush()
            if end:
                self.end_header()
        
    def end_header(self):
        sys.stdout.write("\r \r\n")
        sys.stdout.flush()

    def interact(self):
        if LoGrok.curses:
            self.draw_start_screen()
            self.main_loop()
            return
        if self.interactive:
            self.shell()
            return
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
            self.query(q[:-1])

    def query(self, query):
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
                semicolon = query.find(';')
                if semicolon != -1:
                    query = query[:semicolon]
                q = LogQuery(self, self.data, query)
                return q.run()
            except SyntaxError, e:
                return e.message
    
    def draw_start_screen(self):
        headers = self.data[0].keys()
        fmt = "|| "
        w = {}
        for h in headers:
            w[h] = ColSizes.get(h) if ColSizes.get(h) <= 20 else 20
            fmt += "%%-%ds || " % w[h]

        self.print_header(fmt % tuple(headers))

        for row in xrange(2, self.height):
            rdata = []
            for h in headers:
                rdata.append(self.data[row-2][h][:w[h]])
            try:
                self.screen.addnstr(row, 0, fmt % tuple(rdata), self.width)
            except curses.error:
                pass
        self.screen.refresh()

    def get_data(self, datalen=None):
        data = []
        if datalen is None:
            datalen = self.loglen
        while True:
            try:
                chunk = self.result_queue.get(True, 1)
            except:
                break
            self.processed_rows += 1
            data.append(chunk)
            pct = int((float(self.processed_rows)/datalen) * 100)
            if pct != self.oldpct:
                self.oldpct = pct
                self.print_header("   Processing log... %d%%" % pct)
        return data

    def check_running(self):
        for p in self.processes:
            p.join(1)
            if p.exitcode is None:
                return True

    def get_query(self):
        self.screen.addstr(self.height-4, 0, LoGrok.fullwidth % "QUERY:", curses.A_STANDOUT)
        self.screen.move(self.height-3, 0)
        self.screen.clrtobot()
        self.screen.refresh()
        curses.echo()
        curses.nocbreak()
        query = ""
        while True:
            c = self.screen.getch()
            if c == ord('\n'): break
            query += chr(c)
        self.query(query)
        curses.noecho()
        curses.cbreak()

    def run(self, q):
        pass

    def main_loop(self):
        while 1:
            c = self.screen.getch()
            if c == ord('x'): break
            if c == ord('q'): self.get_query()

def rx_closure(rx):
    def dorx(iq, oq):
        r = re.compile(rx)
        for lines in iter(iq.get, 'STOP'):
            for line in lines:
                out = {}
                m = r.match(line)
                for key in r.groupindex:
                    out[key] = m.group(key)
                oq.put(out)
    return dorx

def main():
    cmd = argparse.ArgumentParser(description="Grok/Query/Aggregate log files. Requires python2 >= 2.7", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    typ = cmd.add_mutually_exclusive_group(required=True)
    typ.add_argument('-t', '--type', metavar='TYPE', choices=TYPES, help='{%s} Use built-in log type'%', '.join(TYPES), default='apache-common')
    typ.add_argument('-f', '--format', action='store', help='Log format (use apache LogFormat string)')
    cmd.add_argument('-j', '--processes', action='store', type=int, help='Number of processes to fork for log crunching', default=int(cpu_count()*1.5))
    cmd.add_argument('-l', '--lines', action='store', type=int, help='Only process LINES lines of input')
    interactive = cmd.add_mutually_exclusive_group(required=False)
    interactive.add_argument('-i', '--interactive', action='store_true', help="Use line-based interactive interface")
    interactive.add_argument('-c', '--curses', action='store_true', help=argparse.SUPPRESS)
    interactive.add_argument('-q', '--query', help="The query to run")
    cmd.add_argument('-d', '--debug', action='store_true', help="Turn debugging on (you don't want this)")
    cmd.add_argument('logfile', nargs='+', help="log(s) to parse/query")
    args = cmd.parse_args(sys.argv[1:])

    DEBUG = args.debug
    parser.DEBUG = DEBUG
    parser.init()

    LoGrok(args, interactive=args.interactive, curses=args.curses)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        # TODO -- kill multiprocesses
        # TODO -- reset terminal if curses
        print
        sys.exit(1)
