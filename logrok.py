#!/usr/bin/env python

import argparse
import sys
import os
import re
import curses
import code
import readline
import atexit
import fcntl
import termios
import struct
import signal
import math

from functools import partial
from multiprocessing import Process, cpu_count, Queue

TYPES = {
    'apache-common': "%h %l %u %t \"%r\" %>s %b",
    'apache-common-vhost': "%v %h %l %u %t \"%r\" %>s %b",
    'ncsa-combined': "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-agent}i\"",
    'referer': "%{Referer}i -> %U",
    'agent': "%{User-agent}i",
    'syslog': "%{%b %d %H:%M:%S}t %h %v[%P]: %M",
}

class Regex(object):
    @staticmethod
    def r(rx, name, nocapture):
        if nocapture:
            return rx
        if name is not '':
            name = r'?P<%s>' % name
        return r'(%s%s)' % (name, rx)

    @staticmethod
    def host(name='', nocapture=False):
        return Regex.r(r'[a-zA-Z0-9\-\.]+', name, nocapture)

    @staticmethod
    def number(name='', nocapture=False):
        return Regex.r(r'\d+', name, nocapture)

    @staticmethod
    def string(name='', nocapture=False):
        return Regex.r(r'[^\s]+', name, nocapture)

    @staticmethod
    def commontime(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        if nocapture:
            return r'\[[^\]]+]'
        return r'\[(%s[^\]]+)]' % name

    @staticmethod
    def nil(name='', nocapture=False):
        return Regex.r(r'-', name, nocapture)

    @staticmethod
    def cstatus(name='', nocapture=False):
        return Regex.r(r'X|\+|\-', name, nocapture)

    def any(name='', nocapture=False):
        return Regex.r(r'.*', name, nocapture)

    @staticmethod
    def _or(a, b, name=''):
        if name is not '':
            name = r'?P<%s>' % name
        return "("+name+a(nocapture=True)+"|"+b(nocapture=True)+")"

    @staticmethod
    def dstring(start, negmatch, end, name='', nocapture=False):
        """ grab all not-negmatch chars, but allow for backslash-escaped negmatch """
        if name is not '':
            name = r'?P<%s>' % name
        if nocapture:
            return r'%s[^%s\\]*(?:\\.[^%s\\]*)*%s' % (start, negmatch, negmatch, end)
        return r'%s(%s[^%s\\]*(?:\\.[^%s\\]*)*)%s' % (start, name, negmatch, negmatch, end)

FORMAT = {
    'a': (Regex.host, "remote_ip"),
    'A': (Regex.host, "local_ip"),
    'B': (Regex.number, "body_size"),
    'b': (partial(Regex._or, Regex.number, Regex.nil), "body_size"),
    'C': (Regex.string, "cookie"),
    'D': (Regex.number, "response_time_ms"),
    'e': (Regex.string, "environment_var"),
    'f': (Regex.string, "filename"),
    'h': (Regex.host, "remote_host"),
    'H': (Regex.string, "protocol"),
    'i': (Regex.string, "input_header"),
    'l': (Regex.string, "logname"),
    'm': (Regex.string, "method"),
    'M': (Regex.any, "message"),
    'n': (Regex.string, "note"),
    'o': (Regex.string, "output_header"),
    'p': (Regex.number, "port"),
    'P': (Regex.number, "pid"),
    'q': (Regex.string, "query_string"),
    'r': (Regex.string, "request"),
    's': (Regex.number, "status_code"),
    't': (Regex.commontime, "date_time"),
    'T': (Regex.number, "response_time_s_"),
    'u': (Regex.string, "auth_user"),
    'U': (Regex.string, "url"),
    'v': (Regex.host, "server_name"),
    'V': (Regex.host, "canonical_server_name"),
    'X': (Regex.cstatus, "conn_status"),
    'I': (Regex.number, "bytes_received"),
    'O': (Regex.number, "bytes_sent"),
}

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

class LogQuery(object):
    def __init__(self, parent, data, query):
        self.parent = parent
        self.data = data
        self.query = query
        self.select(query)
    
    def select(self, query):
        self.what = []
        if query.startswith('select'):
            # skip select
            query = query[query.find(' '):]

        brklater = False
        while True:
            (tok, delim, query) = self.qtok(',', query)
            spc = tok.find(' ')
            nxt = ''
            if spc != -1:
                t= tok[:spc]
                nxt = tok[spc+1:]
                tok = t
                query = nxt + ' ' + query # unget token
                brklater=True
            if len(query) == 0:
                brklater=True
            #if tok not in self.data[0].keys():
            #    raise SyntaxError("ERROR: %s is not a valid field!\nChoices are %s" % (tok, ','.join(self.data[0].keys())))
            self.what.append(tok)
            if brklater:
                break
        if query.startswith('from '):
            try:
                query = query[len('from '):]
                query = query[query.find(' ')+1:]
            except IndexError:
                pass

    def qtok(self, delim, s):
        s.strip()
        ret = s.partition(delim)
        return tuple([x.strip() for x in ret])

    def avg(self, column):
        vals = ChunkableList([v for row in self.data for v in row[column]])
        tasklist = []
        # map
        for chunk in vals.chunks(1000):
            tasklist.append(chunk)
        data = self.parent.parallel(self._avg_mapper, tasklist, len(vals), numprocs=1, wait=True)
        # reduce
        avg = sum([d[0] for d in data], 0.0) / sum([d[1] for d in data])
        return avg

    def _avg_mapper(self, inq, outq):
        for lines in iter(inq.get, 'STOP'):
            numlines = len(lines)
            total = sum([int(line) for line in lines])
            outq.put((total, numlines))

    def run(self):
        response = {}
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
        print response
        #self.print_response(response)

    def print_response(self, r):
        tablewidth = max([len(x)for row in r for x in row.keys() ] + [len(x) for row in r for x in row.values()])
        print '+' + ('-'*(tablewidth+r[0].keys())-2) + '+'
        headers = r.keys()
        columns = []
        fmt = "|"
        for h in headers:
            columns.append(h)
            fmt += "%%%ds|" % max([len(x[h]) for x in r])
        for row in r:
            print fmt % tuple([row[h] for h in columns]) 
        print '+' + ('-'*(tablewidth+r[0].keys())-2) + '+'



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

class LoGrok(object):
    width = 0
    height = 0
    fullwidth = ""
    curses = False
    def __init__(self, screen, args, interactive=False, chunksize=10000):
        if screen:
            LoGrok.curses = True
            self.screen = screen
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
        self.query(self.args.query)

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
        self.complete.addopts(['select', 'where', 'avg', 'max', 'min', 'count', 'between', ] + self.data[0].keys())
        while True:
            q = raw_input("logrok> ").strip()
            while not q.endswith(";"):
                q += raw_input("> ").strip()
            print self.query(q[:-1])

    def query(self, query):
        query = query.lower()
        if query in ('quit', 'bye', 'exit'):
            sys.exit(0)
        if query.startswith('help') or query.startswith('?'):
            answer = "Use sql syntax against your log, `from` clauses are ignored.\n"\
                    "Queries can span multiple lines and _must_ end in a semicolon `;`.\n"\
                    " Try: `show fields;` to see available field names."
            return answer
        if query in ('show fields', 'show headers'):
            return ', '.join(self.data[0].keys())
        else:
            try:
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
    cmd = argparse.ArgumentParser(description="Grok/Query/Aggregate log files", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    typ = cmd.add_mutually_exclusive_group(required=True)
    typ.add_argument('-t', '--type', metavar='TYPE', choices=TYPES, help='{%s} Use built-in log type'%', '.join(TYPES), default='apache-common')
    typ.add_argument('-f', '--format', action='store', help='Log format (use apache LogFormat string)')
    cmd.add_argument('-j', '--processes', action='store', type=int, help='Number of processes to fork for log crunching', default=int(cpu_count()*1.5))
    cmd.add_argument('-l', '--lines', action='store', type=int, help='Only process LINES lines of input')
    interactive = cmd.add_mutually_exclusive_group(required=False)
    interactive.add_argument('-i', '--interactive', action='store_true', help="Use line-based interactive interface")
    interactive.add_argument('-c', '--curses', action='store_true', help="Use curses-based interactive interface (Currntly Disabled)")
    cmd.add_argument('-q', '--query', help="The query to run")
    cmd.add_argument('logfile', nargs='+', type=argparse.FileType('r'))
    args = cmd.parse_args(sys.argv[1:])

    if args.interactive:
        LoGrok(None, args, interactive=True)
    elif args.curses:
        curses.wrapper(LoGrok, args, curses=True)
    else:
        LoGrok(None, args)


if __name__ == '__main__':
    main()
