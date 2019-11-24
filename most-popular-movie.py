
from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields = line.split()
    return (int(fields[1]))

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

line = sc.textFile("data/movie-ratings.data")
rdd = line.map(parseLine) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(False) 



# result = rdd.collect()
result = rdd.take(1)





for item in result:
    print item