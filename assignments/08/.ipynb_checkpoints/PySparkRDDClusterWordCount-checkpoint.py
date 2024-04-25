import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("Spark Count")
    sc = SparkContext(conf=conf)

    text_file = sc.textFile(sys.argv[1])
    words = text_file.flatMap(lambda line: line.split(" "))
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.saveAsTextFile(sys.argv[2])
