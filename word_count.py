from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)
    for i in range(10000):
        spark = SparkSession\
            .builder\
            .appName("PythonWordCount")\
            .getOrCreate()
        filename = sys.argv[1]
        lines = spark.read.text(filename).rdd.map(lambda r: r[0])
        counts = lines.flatMap(lambda x: x.split(' ')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add)
        output = counts.collect()
        for (word, count) in output:
            print("%s: %i" % (word, count))

        spark.stop()
if __name__ == "__main__":
    main()
