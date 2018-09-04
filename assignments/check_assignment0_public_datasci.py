#!/usr/bin/python
"""
CS 451 Data-Intensive Distributed Computing (Fall 2018):
Assignment 0 public check script for the Datasci cluster

Sample usage:
$ ./check_assignment0_public_altiscale.py lintool
"""

import sys
import os
import signal
from subprocess import call

# hack to avoid broken pipe error
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def check_a0(u):
  """Run Assignment 0 on the Datasci cluster"""
  call(["mvn", "clean", "package"])
  call(["hadoop", "jar", "target/assignments-1.0.jar",
        "ca.uwaterloo.cs451.a0.PerfectX",
        "-input", "/data/cs451/enwiki-20180901-sentences-0.1sample.txt",
        "-output", "cs451-"+u+"-a0-wiki" ])
  print("Question 5.")
  call("hadoop fs -cat cs451-"+u+"-a0-wiki/part* | sort -k 2 -n -r | head -10", shell=True)

if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    check_a0(sys.argv[1])
  except Exception as e:
    print(e)
