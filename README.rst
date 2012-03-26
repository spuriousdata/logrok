######
LoGrok
######

LoGrok reads and parses arbitrary log files and allows you to run queries against their data. LoGrok can parse
standard Apache LogFormat strings to describe the format of the logdata and it can process mutliple logs at one time.
LoGrok uses python's multiprocessing package to take full advantage of all of your CPUs ensuring the fastest parse and
query time possible.

Requires Python2 2.7 or higher

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


=======
License
=======

LoGrok is licensed under the MIT license. [#]_

.. [#] See the file LICENSE_

.. _LICENSE: http://github.com/spuriousdata/logrok/blob/master/LICENSE
