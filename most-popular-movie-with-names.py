from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)


def loadMovieNames():
    movieNames = {}
    with open("data/u.item") as fopen:
        for line in fopen:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames


def parseLine(line):
    fields = line.split()
    return (int(fields[1]))


# broadcast all movie names to all nodes
nameDict = sc.broadcast(loadMovieNames())

line = sc.textFile("data/movie-ratings.data")
sortedMovies = line.map(parseLine) \
        .map(lambda x: (x,1)) \
        .reduceByKey(lambda x,y: x+y) \
        .map(lambda (x,y): (y,x)) \
        .sortByKey(False) 


sortedMoviesWithNames = sortedMovies.map(lambda (count, movie): (nameDict.value[movie], count))

# result = rdd.collect()
result = sortedMoviesWithNames.take(10)


for item in result:
    print item