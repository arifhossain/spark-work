from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    fields = line.split(',')
    return (fields[0], fields[2], float(fields[3]))

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

line = sc.textFile("weather-1800.csv")
rdd = line.map(parseLine)
rdd = rdd.filter(lambda x: "TMAX" in x[1]).map(lambda x: (x[0], x[2]))
rdd = rdd.reduceByKey(lambda x,y: max(x,y))

result = rdd.collect()

for item in result:
    print item
