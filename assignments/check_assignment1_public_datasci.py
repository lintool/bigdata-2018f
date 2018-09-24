#!/usr/bin/python
"""
CS 451 Data-Intensive Distributed Computing (Fall 2018):
Assignment 1 public check script for the Datasci cluster

Sample usage:
$ ./check_assignment1_public_datasci.py lintool
"""

import sys
import os
import signal
from subprocess import call

# hack to avoid broken pipe error
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def check_a1(u):
  """Run Assignment 1 on the Datasci Cluster"""
  call(["mvn", "clean", "package"])

  call([ "hadoop", "jar", "target/assignments-1.0.jar", "ca.uwaterloo.cs451.a1.PairsPMI",
         "-input", "/data/cs451/simplewiki-20180901-sentences.txt",
         "-output", "cs451-"+u+"-a1-wiki-pairs", "-reducers", "5", "-threshold", "50"])
  call([ "hadoop", "jar", "target/assignments-1.0.jar", "ca.uwaterloo.cs451.a1.StripesPMI",
         "-input", "/data/cs451/simplewiki-20180901-sentences.txt",
         "-output", "cs451-"+u+"-a1-wiki-stripes", "-reducers", "5", "-threshold", "50"])
  print("\n\nQuestion 7.")
  call("hadoop fs -cat cs451-"+u+"-a1-wiki-pairs/part-r-0000* | grep '(hockey,' | sort -t'(' -k 3 -g -r | head -5",shell=True)
  print("\n\nQuestion 8.")
  call("hadoop fs -cat cs451-"+u+"-a1-wiki-pairs/part-r-0000* | grep '(data,' | sort -t'(' -k 3 -g -r | head -5",shell=True)
  print("");

if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    check_a1(sys.argv[1])
  except Exception as e:
    print(e)
