# Runs SON algorithm to find frequent item set in a distributed fashion
import sys
from pyspark import SparkContext
from collections import Counter


def candidates_count(partition, candidates):
    partition = list(partition)
    counts = [0] * len(candidates)
    for bucket in partition:
        for _, candidate in enumerate(candidates):
            for i in range(len(candidate[0])):
                if candidate[0][i] not in bucket[1]:
                    break
                if i == len(candidate[0])-1:
                    counts[_] += 1
    return ((tuple(candidates[_][0]), counts[_])for _ in range(len(candidates)))


def new_candidates(oldcandidates):
    if len(oldcandidates) <= 1:
        return []
    newcandidates = []
    for _, oldcan1 in enumerate(oldcandidates):
        for oldcan2 in oldcandidates[_+1:]:
            if(oldcan1[:-1] == oldcan2[:-1]):
                newcan = []
                newcan.extend(oldcan1)
                newcan.extend([oldcan2[-1]])
                newcandidates.append(tuple(newcan))
            else:
                break
    return tuple(newcandidates)


def apriori(partition, support, total_buckets):
    partition = list(partition)
    support *= (float(len(partition))/float(total_buckets))
    frequent = []
    old_candidates = []
    c = Counter()
    for bucket in partition:
        c.update(bucket[1])
    for i in c:
        if c[i] >= support:
            frequent.append((i))
            old_candidates.append([i])
    old_candidates.sort(key=lambda x: x[0])
    newcandidates = new_candidates(old_candidates)
    while newcandidates:
        c = [0] * len(newcandidates)
        for b in partition:
            for index, _ in enumerate(newcandidates):
                for i in range(len(_)):
                    if _[i] not in b[1]:
                        break
                    if i == len(_)-1:
                        c[index] += 1
        frequent.extend((newcandidates[_] for _ in range(
            len(newcandidates)) if c[_] >= support))
        newcandidates = new_candidates(newcandidates)
    return tuple(frequent)


# take from arguments
bucketlist = "/home/saurabh/frequent/buckets"
support = 4

# removed for running in pyspark
sc = SparkContext("local[*]", "SON")
buckets = sc.textFile(
    "file:///"+bucketlist).map(lambda x: (x.strip().split(',')[0], x.strip().split(',')[1]))
buckets = buckets.groupByKey().mapValues(set)
total_buckets = buckets.count()
candidates = buckets.mapPartitions(lambda x: apriori(x, support, total_buckets)).map(
    lambda x: (x, 1)).reduceByKey(lambda x, y: (1)).collect()
frequent_set = buckets.mapPartitions(lambda x: candidates_count(x, candidates)).reduceByKey(
    lambda x, y: x+y).filter(lambda x: x[1] >= support).collect()
frequent_set.sort(key=lambda x: len(x[0]))
print(frequent_set)
with open("/home/saurabh/frequent/output", "w") as outFile:
    for _ in frequent_set:
        outFile.write(','.join(_[0])+"\n")
