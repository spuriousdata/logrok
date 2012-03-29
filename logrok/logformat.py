import re
from functools import partial

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
    'D': (Regex.number, "response_time_us"),
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
