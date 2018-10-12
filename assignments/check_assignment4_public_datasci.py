#!/usr/bin/python
"""
CS 451 Data-Intensive Distributed Computing (Fall 2018):
Assignment 4 public check script for the Datasci cluster

Sample usage:
$ ./check_assignment4_public_datasci.py lintool
"""

import sys, os, signal, argparse
from subprocess import call

# hack to avoid broken pipe error
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def check_assignment(u):
    """Run Assignment 4 on the Datasci Cluster"""
    call(["mvn","clean","package"])

    # First, convert the adjacency list into PageRank node records:
    call([ "hadoop","jar","target/assignments-1.0.jar",
           "ca.uwaterloo.cs451.a4.BuildPersonalizedPageRankRecords",
           "-input", "/data/cs451/enwiki-20180901-adj.txt",
           "-output", "cs451-"+u+"-a4-wiki-PageRankRecords",
           "-numNodes", "14099242", "-sources", "73273,73276" ])

    # Next, partition the graph (hash partitioning) and get ready to iterate:
    call("hadoop fs -mkdir cs451-"+u+"-a4-wiki-PageRank",shell=True)
    call([ "hadoop","jar","target/assignments-1.0.jar",
           "ca.uwaterloo.cs451.a4.PartitionGraph",
           "-input", "cs451-"+u+"-a4-wiki-PageRankRecords",
           "-output", "cs451-"+u+"-a4-wiki-PageRank/iter0000",
           "-numPartitions", "10", "-numNodes", "14099242" ])

    # After setting everything up, iterate multi-source personalized PageRank:
    call([ "hadoop","jar","target/assignments-1.0.jar",
           "ca.uwaterloo.cs451.a4.RunPersonalizedPageRankBasic",
           "-base", "cs451-"+u+"-a4-wiki-PageRank",
           "-numNodes", "14099242", "-start", "0", "-end", "20", "-sources", "73273,73276" ])

    # Finally, run a program to extract the top ten personalized PageRank values, with respect to each source.
    call([ "hadoop","jar","target/assignments-1.0.jar",
           "ca.uwaterloo.cs451.a4.ExtractTopPersonalizedPageRankNodes",
           "-input", "cs451-"+u+"-a4-wiki-PageRank/iter0020",
           "-output", "cs451-"+u+"-a4-wiki-PageRank-top10",
           "-top", "10", "-sources", "73273,73276" ])


if __name__ == "__main__":
  try:
    if len(sys.argv) < 2:
        print "usage: "+sys.argv[0]+" [github-username]"
        exit(1)
    check_assignment(sys.argv[1])
  except Exception as e:
    print(e)
