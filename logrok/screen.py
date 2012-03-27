import curses
import atexit
import readline
import fcntl
import termios
import signal
import struct
import sys

from util import ColSizes

width, height = (0, 0)
fullwidth_fmt = ""
_screen = None

def init_curses(callback):
    curses.wrapper(_init_curses, callback)

def _init_curses(scr, callback):
    global width, height, _screen, fullwidth_fmt
    _screen = scr
    (height, width) = scr.getmaxyx()
    fullwidth_fmt = "  %%-%ds" % (width-2)
    callback()

def init_linebased():
    line_screen()
    signal.signal(signal.SIGWINCH, line_screen)

def line_screen():
    global width, height, fullwidth_fmt
    s = struct.pack("HHHH", 0, 0, 0, 0)
    (height, width, h, w) = struct.unpack("HHHH", fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ,s))
    fullwidth_fmt = "  %%-%ds" % (width-2)

def print_mutable(line, end=False):
    if _screen:
        _screen.addstr(1, 0, fullwidth_fmt % line, curses.A_STANDOUT)
        _screen.refresh()
    else:
        if len(line):
            sys.stdout.write("\r%s" % (fullwidth_fmt % line))
            sys.stdout.flush()
        if end:
            _end_print_mutable()

def _end_print_mutable():
    sys.stdout.write("\r \n")
    sys.stdout.flush()

def print_line(s, row=None, column=None, delay_refresh=False):
    if _screen and row and column:
        _screen.addnstr(row, column, fullwidth_fmt % s, width)
        if not delay_refresh:
            _screen.refresh()
    else:
        print fullwidth_fmt % s

def refresh():
    _screen.refresh()

def is_curses():
    return True if _screen else False

def draw_curses_screen(data):
    headers = data[0].keys()
    fmt = "|| "
    w = {}
    for h in headers:
        w[h] = ColSizes.get(h) if ColSizes.get(h) <= 20 else 20
        fmt += "%%-%ds || " % w[h]

    screen.print_mutable(fmt % tuple(headers))

    for row in xrange(2, height):
        rdata = []
        for h in headers:
            rdata.append(self.data[row-2][h][:w[h]])
        try:
            print_line(fmt % tuple(rdata), row, 0, True)
        except curses.error:
            pass
        refresh()

def prompt(p, callback):
    _screen.addstr(height-4, 0, fullwidth_fmt % s, curses.A_STANDOUT)
    _screen.move(height-3, 0)
    _screen.clrtobot()
    _screen.refresh()
    curses.echo()
    curses.nocbreak()
    inpt = ""
    while True:
        c = _screen.getch()
        if c == ord('\n'): break
        inpt += chr(c)
    callback(inpt)
    curses.nocecho()
    curses.cbreak()

def getch():
    return _screen.getch()
