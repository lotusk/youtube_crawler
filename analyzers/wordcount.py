from __future__ import print_function
from operator import add
import json
from pyspark.sql import SparkSession
import argparse


def to_id_pair(line):
    line = line.strip()
    doc = json.loads(line)
    id = doc.get("id").get("videoId")
    return id, line


def extract_word(line):
    """extract word from title and description"""
    print("line: %s" % line)
    # line = line.strip()
    doc = json.loads(line)
    snippet = doc.get("snippet")
    description = snippet.get("description")
    title = snippet.get("title")
    return description.split(' ') + title.split(' ')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='filename')
    args = parser.parse_args()
    file_name = args.file
    if not args.file:
        print("Please input file name")
        exit(0)
    print("Ready for wordcount from file: %s" % file_name)

    spark = SparkSession \
        .builder \
        .appName("WordCount") \
        .getOrCreate()

    lines = spark.sparkContext.textFile(file_name) \
        .map(to_id_pair) \
        .reduceByKey(lambda x, y: x) \
        .map(lambda x: x[1]) # deduplicate by id

    print("lines count %s" % lines.count())

    counts = lines.flatMap(extract_word) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1])

    # output = counts.collect()
    counts.coalesce(1, shuffle=True).saveAsTextFile("%s_wordcount" % file_name)
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))

    spark.stop()
