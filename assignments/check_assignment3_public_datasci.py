#!/usr/bin/python
"""
CS 451 Data-Intensive Distributed Computing (Fall 2018):
Assignment 3 public check script for the Datasci cluster

Sample usage:
$ ./check_assignment3_public_datasci.py lintool
"""

import sys, os, signal, argparse
from subprocess import call

# hack to avoid broken pipe error
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def check_a3(username,reducers):
  """Run Assignment 3 on the Datasci Cluster"""
  call(["mvn", "clean", "package"])
  call(["hadoop", "jar", "target/assignments-1.0.jar",
        "ca.uwaterloo.cs451.a3.BuildInvertedIndexCompressed".format(username),
        "-input", "/data/cs451/enwiki-20180901-sentences-0.1sample.txt", 
        "-output", "cs451-{0}-a3-index-wiki".format(username), "-reducers", str(reducers) ])
  print("\n\nQuestion 2.\n")
  call(["hadoop", "fs", "-du", "-h", "cs451-{0}-a3-index-wiki".format(username)])
  print("\n\nQuestion 3.\n")
  call(["hadoop", "jar", "target/assignments-1.0.jar",
        "ca.uwaterloo.cs451.a3.BooleanRetrievalCompressed".format(username),
        "-index", "cs451-{0}-a3-index-wiki".format(username), 
        "-collection", "/data/cs451/enwiki-20180901-sentences-0.1sample.txt",
        "-query", "waterloo stanford OR cheriton AND"])
  print("\n\nQuestion 4.\n")
  call(["hadoop", "jar", "target/assignments-1.0.jar",
        "ca.uwaterloo.cs451.a3.BooleanRetrievalCompressed".format(username),
        "-index", "cs451-{0}-a3-index-wiki".format(username),
        "-collection", "/data/cs451/enwiki-20180901-sentences-0.1sample.txt",
        "-query", "big data AND hadoop spark OR AND"])

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description="CS 451 A3 Public Check Script for the Datasci Cluster")
  parser.add_argument('username',metavar='[Github Username]', help="Github username",type=str)
  parser.add_argument('-r','--reducers',help="Number of reducers to use.",type=int,default=4)
  args=parser.parse_args()
  try:
    check_a3(args.username,args.reducers)
  except Exception as e:
    print(e)
