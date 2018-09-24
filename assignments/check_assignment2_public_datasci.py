#!/usr/bin/python
"""
CS 451 Data-Intensive Distributed Computing (Fall 2018):
Assignment 2 public check script for the Datasci cluster

Sample usage:
$ ./check_assignment2_public_datasci.py lintool
"""

import sys
import os
import signal
from subprocess import call

# hack to avoid broken pipe error
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def check_a2(u):
  """Run Assignment 2 on the Datasci Cluster"""
  call(["mvn", "clean", "package"])
  call([ "spark-submit", "--class", "ca.uwaterloo.cs451.a2.ComputeBigramRelativeFrequencyPairs",
         "--num-executors", "2", "--executor-cores", "4", "--executor-memory", "24G", "target/assignments-1.0.jar",
         "--input", "/data/cs451/enwiki-20180901-sentences-0.1sample.txt",
         "--output", "cs451-"+u+"-a2-wiki-bigram-pairs", "--reducers", "8"])
  call([ "spark-submit", "--class", "ca.uwaterloo.cs451.a2.ComputeBigramRelativeFrequencyStripes",
         "--num-executors", "2", "--executor-cores", "4", "--executor-memory", "24G", "target/assignments-1.0.jar",
         "--input", "/data/cs451/enwiki-20180901-sentences-0.1sample.txt",
         "--output", "cs451-"+u+"-a2-wiki-bigram-stripes", "--reducers", "8"])
  call([ "spark-submit", "--class", "ca.uwaterloo.cs451.a2.PairsPMI",
         "--num-executors", "2", "--executor-cores", "4", "--executor-memory", "24G", "target/assignments-1.0.jar",
         "--input", "/data/cs451/simplewiki-20180901-sentences.txt",
         "--output", "cs451-"+u+"-a2-wiki-pmi-pairs", "--reducers", "8", "--threshold", "10"])
  call([ "spark-submit", "--class", "ca.uwaterloo.cs451.a2.StripesPMI",
         "--num-executors", "2", "--executor-cores", "4", "--executor-memory", "24G", "target/assignments-1.0.jar",
         "--input", "/data/cs451/simplewiki-20180901-sentences.txt",
         "--output", "cs451-"+u+"-a2-wiki-pmi-stripes", "--reducers", "8", "--threshold", "10"])
  print("\n\nBigram pairs:")
  call("hadoop fs -cat cs451-"+u+"-a2-wiki-bigram-pairs/part-0000* | grep '((dream,' | sort | head", shell=True)
  print("\n\nBigram stripes:")
  call("hadoop fs -cat cs451-"+u+"-a2-wiki-bigram-stripes/part-0000* | grep '(dream,'", shell=True)
  print("\n\nPMI pairs:")
  call("hadoop fs -cat cs451-"+u+"-a2-wiki-pmi-pairs/part-0000* | grep '((dream,' | sort | head", shell=True)
  print("\n\nPMI stripes:")
  call("hadoop fs -cat cs451-"+u+"-a2-wiki-pmi-stripes/part-0000* | grep '(dream,'", shell=True)
  print("")

if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    check_a2(sys.argv[1])
  except Exception as e:
    print(e)
