#!/usr/bin/env python

import argparse
import sys
import re
import curses

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
    def host(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        rx = r'[a-zA-Z0-9\-\.]+'
        if nocapture:
            return rx
        return r'(%s%s)' % (name, rx)

    @staticmethod
    def number(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        rx = r'\d+'
        if nocapture:
            return rx
        return r'(%s%s)' % (name, rx)

    @staticmethod
    def string(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        rx = r'[^\s]+'
        if nocapture:
            return rx
        return r'(%s%s)' % (name, rx)

    @staticmethod
    def commontime(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        if nocapture:
            return r'\[[^\]]+]'
        return r'\[(%s[^\]]+)]' % name

    @staticmethod
    def nil(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        rx = r'-'
        if nocapture:
            return rx
        return r'(%s%s)' % (name, rx)

    @staticmethod
    def cstatus(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        rx = r'X|\+|\-'
        if nocapture:
            return rx
        return r'(%s%s)' % (name, rx)

    def any(name='', nocapture=False):
        if name is not '':
            name = r'?P<%s>' % name
        rx = r'.*'
        if nocapture:
            return rx
        return r'(%s%s)' % (name, rx)

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

class LoGrok(object):
    def __init__(self, screen, args):
        self.screen = screen
        self.args = args
        self.processed_rows = 0
        self.data = []
        self.setup()
        self.crunchlogs()
        self.interactive()

    def setup(self):
        (self.height, self.width) = self.screen.getmaxyx()
        self.fullwidth = "%%-%ds" % self.width

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
            self.print_header("   Reading %s" % logfile.name)
            lines += logfile.readlines()
            logfile.close()

        for i in xrange(0, self.args.processes):
            p = Process(target=func, args=(self.task_queue, self.result_queue))
            p.daemon = True
            p.start()
            self.processes.append(p)

        for line in lines.chunks(10000):
            self.task_queue.put(line)
            break

        for i in range(0, self.args.processes):
            self.task_queue.put('STOP')

    def print_header(self, s):
        self.screen.addstr(1, 0, self.fullwidth % s, curses.A_STANDOUT)
        self.screen.refresh()
        
    def interactive(self):
        data = []
        while True:
            if self.check_running():
                self.get_data()
            else:
                break
        self.get_data()
        self.draw_start_screen()
        self.main_loop()
    
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

    def get_data(self):
        while True:
            try:
                row = self.result_queue.get(True, 1)
            except:
                break
            self.processed_rows += 1
            self.data.append(row)
            [ColSizes.add(k,v) for k,v in row.items()]
            if self.processed_rows % 100 == 0:
                self.print_header("     Processing log...  Read %10d lines" % self.processed_rows)

    def check_running(self):
        for p in self.processes:
            p.join(1)
            if p.exitcode is None:
                return True

    def get_query(self):
        self.screen.addstr(self.height-4, 0, self.fullwidth % "QUERY:", curses.A_STANDOUT)
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
        curses.noecho()
        curses.cbreak()

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
    cmd.add_argument('logfile', nargs='+', type=argparse.FileType('r'))
    args = cmd.parse_args(sys.argv[1:])

    curses.wrapper(LoGrok, args)

if __name__ == '__main__':
    main()
