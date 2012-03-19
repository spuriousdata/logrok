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
        for line in iter(iq.get, 'STOP'):
            out = {}
            m = r.match(line)
            for key in r.groupindex:
                out[key] = m.group(key)
            oq.put(out)
    return dorx

def crunchlogs(scr, args):
    if args.format is not None:
        logformat = args.format
    else:
        logformat = TYPES[args.type]

    regex = parse_format_string(logformat)
    func = rx_closure(regex)
    task_queue = Queue()
    result_queue = Queue()
    processes = []

    lines = []
    for logfile in args.logfile:
        print_header(scr, "   Reading %s" % logfile.name)
        lines += logfile.readlines()
        logfile.close()

    for i in xrange(0, args.processes):
        p = Process(target=func, args=(task_queue, result_queue))
        p.start()
        processes.append(p)

    for line in lines[:1000]:
        task_queue.put(line)

    for i in range(0, args.processes):
        task_queue.put('STOP')
    
    return processes, result_queue

def print_header(scr, s):
    (height, width) = scr.getmaxyx()
    fullwidth = "%%-%ds" % width
    scr.addstr(1, 0, fullwidth % s, curses.A_STANDOUT)
    scr.refresh()

def interactive(scr, args):
    (height, width) = scr.getmaxyx()
    procs, result = crunchlogs(scr, args)
    data = []
    while True:
        if check_running(procs):
            data += get_data(scr, result)
        else:
            break
    data += get_data(scr, result)
    draw_start_screen(scr, data)
    main_loop(scr)

def get_data(scr, queue):
    data = []
    while True:
        try:
            row = queue.get(True, 1)
            get_data.i += 1
        except:
            break
        data.append(row)
        if get_data.i % 100 == 0:
            print_header(scr, "     Processing log...  Read %10d lines" % get_data.i)
    return data
get_data.i = 0

def draw_start_screen(scr, data):
    (height, width) = scr.getmaxyx()
    headers = data[0].keys()
    hlen = len(''.join(headers))
    if width > hlen:
        spaces = len(headers)-1
        space = width/spaces
    else:
        space = 1
    fmt = ""
    for h in headers:
        fmt += "%%%ds " % space
    scr.addstr(1, 0, fmt % tuple(headers), curses.A_STANDOUT)
    for row in xrange(2, height):
        rdata = []
        for h in headers:
            rdata.append(data[row-2][h][:space-2])
        try:
            scr.addnstr(row, 0, fmt % tuple(rdata), width)
        except curses.error:
            pass
    scr.refresh()

def main_loop(scr):
    while 1:
        c = scr.getch()
        if c == ord('q'): break

def check_running(procs):
    for p in procs:
        p.join(1)
        if p.exitcode is None:
            return True

def main():
    cmd = argparse.ArgumentParser(description="Grok/Query/Aggregate log files", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    typ = cmd.add_mutually_exclusive_group(required=True)
    typ.add_argument('-t', '--type', metavar='TYPE', choices=TYPES, help='{%s} Use built-in log type'%', '.join(TYPES), default='apache-common')
    typ.add_argument('-f', '--format', action='store', help='Log format (use apache LogFormat string)')
    cmd.add_argument('-j', '--processes', action='store', type=int, help='Number of processes to fork for log crunching', default=int(cpu_count()*1.5))
    cmd.add_argument('logfile', nargs='+', type=argparse.FileType('r'))
    args = cmd.parse_args(sys.argv[1:])

    curses.wrapper(interactive, args)

if __name__ == '__main__':
    main()
