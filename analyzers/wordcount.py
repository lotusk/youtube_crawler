from __future__ import print_function

import sys
from random import random
from operator import add
import json
from pyspark.sql import SparkSession


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
    file_name = "/Users/lucifer/PycharmProjects/youtube_crawler/ybi01.jl"
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

    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()
