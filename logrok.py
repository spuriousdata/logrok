#!/usr/bin/env python

import argparse
import sys
import re

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
    'D': (Regex.number, "response_time_ms_"),
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
    def dorx(q):
        r = re.compile(rx)
        while True:
            line = q.get()
            if line == 'STOP':
                break
            r.match(line)
    return dorx

def main():
    cmd = argparse.ArgumentParser(description="Grok/Query/Aggregate log files")
    typ = cmd.add_mutually_exclusive_group(required=True)
    typ.add_argument('-t', '--type', metavar='TYPE', choices=TYPES, help='{%s} Use built-in log type'%', '.join(TYPES))
    typ.add_argument('-f', '--format', action='store', help='Log format (use apache LogFormat string)')
    cmd.add_argument('-j', '--processes', action='store', help='Number of processes to fork for log crunching', default=int(cpu_count()*1.5))
    cmd.add_argument('logfile', nargs='+', type=argparse.FileType('r'))
    args = cmd.parse_args(sys.argv[1:])

    if args.format is not None:
        logformat = args.format
    else:
        logformat = TYPES[args.type]

    print "fount %d cpus" % cpu_count()

    regex = parse_format_string(logformat)
    func = rx_closure(regex)
    task_queue = Queue()
    processes = []

    lines = []
    for logfile in args.logfile:
        lines += logfile.readlines()
        logfile.close()

    print "passing off to workers"
    print "processing %d lines" % len(lines)
    for i in xrange(0, args.processes):
        p = Process(target=func, args=(task_queue,))
        p.start()
        processes.append(p)

    i = 0
    for line in lines:
        i += 1
        task_queue.put(line)

    for i in range(0, args.processes):
        task_queue.put('STOP')
    
    for p in processes:
        p.join()

    print "done!"


if __name__ == '__main__':
    main()
