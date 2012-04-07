######
LoGrok
######

LoGrok reads and parses log files of arbitrary format and allows you to run queries against their data. LoGrok can parse
standard Apache LogFormat strings to describe the format of the logdata and it can process mutliple logs at one time.
LoGrok uses python's multiprocessing package to take full advantage of all of your CPUs ensuring the fastest parse and
query time possible.

Requires Python2 2.7 or higher and the ply_ module

============
Installation
============

``sudo python ./setup.py install``


=====
Usage
=====


./logrok.py [-h] (-t TYPE | -f FORMAT) [-j PROCESSES] [-l LINES] [-i | -c] [-q QUERY] [-d] logfile [logfile ...]

positional arguments:
  logfile

optional arguments:
  -h, --help                            
                                        show help message and exit
  -t TYPE, --type TYPE                  {syslog, apache-common-vhost, agent, apache-common, referer, ncsa-combined} 
                                        Use built-in log type (default: apache-common)
  -f FORMAT, --format FORMAT            Log format (use apache LogFormat string) (default: None)
  -j PROCESSES, --processes PROCESSES   Number of processes to fork for log crunching (default: 12)
  -l LINES, --lines LINES               Only process LINES lines of input (default: None)
  -i, --interactive                     Use line-based interactive interface (default: False)
  -q QUERY, --query QUERY               The query to run (default: None)
  -d, --debug                           
                                            Turn debugging on (default: False)

Note
----
* You probably want to run in interactive mode to avoid repeatedly parsing the log(s) at startup

=======
Queries
=======

Format
------

* show <fields|headers>
* [select] <fieldlist> [from xxx] <where <wherelist>> [group by <fieldlist>] [order by <fieldlist>]; 

Helpers
=======

* ``show fields;``    lists available field names
* ``show headers;``   alias for ``show fields;``
* ``help;``           prints a short help

Select Syntax
=============

* ``select``          is ignored, but can be passed
* ``fieldlist``       can be any field or fields separated by commas; fields can also be function calls

Functions
~~~~~~~~~

Aggregate Functions
^^^^^^^^^^^^^^^^^^^
Aggregate functions will calculate a total value for all rows

* ``avg``             calculates average for specified column
* ``mean``            alias for ``avg``
* ``median``          calculates median value for specified column
* ``mode``            calculates mode for specified column
* ``count``           counts rows
* ``max``             calculates max value in specified column
* ``min``             calculates min value in specified column

Value Functions
^^^^^^^^^^^^^^^

Value functions will modify one value

* ``div``         divides first parameter by second parameter
* ``year``        returns year from date field
* ``month``       returns month from date field
* ``day``         returns day from date field
* ``hour``        returns hour from date field
* ``minute``      returns minute from date field
* ``second``      returns second from date field

Example Queries
---------------

* select max(response_time_us), auth_user;
* select date_time, auth_user, request from log where auth_user <> 'bob_smith';
* select hour(date_time) as hr, avg(response_time_us) as resp_time, auth_user from log where auth_user <> 'bob_smith' group by auth_user, hr;
* select date_time, response_time_us where response_time_us > 5000000;
* select div(avg(response_time_us), 1000000) as resp_seconds, auth_user group by auth_user;

=======
License
=======

LoGrok is licensed under the MIT license. [#]_

.. [#] See the file LICENSE_

.. _LICENSE: http://github.com/spuriousdata/logrok/blob/master/LICENSE
.. _ply: http://www.dabeaz.com/ply/
.. _pypi.python.org: http://pypi.python.org
