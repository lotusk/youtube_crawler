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
    return [word.lower() for word in description.split(' ') + title.split(' ')]


def extract_title(line):
    print("line: %s" % line)
    # line = line.strip()
    doc = json.loads(line)
    snippet = doc.get("snippet")
    return snippet.get("channelTitle")


def extract_word_nltk(line):
    import nltk
    doc = json.loads(line)
    snippet = doc.get("snippet")
    description = snippet.get("description")
    title = snippet.get("title")
    return [word.lower() for word in nltk.word_tokenize(description) + nltk.word_tokenize(title)]


def word_count():
    """count the word in title and description"""
    print("Ready for wordcount from file: %s" % file_name)

    spark = SparkSession \
        .builder \
        .appName("WordCount") \
        .getOrCreate()

    lines = spark.sparkContext.textFile(file_name) \
        .map(to_id_pair) \
        .reduceByKey(lambda x, y: x) \
        .map(lambda x: x[1])  # deduplicate by id

    print("lines count %s" % lines.count())

    # counts = lines.flatMap(extract_word_nltk) \
    counts = lines.flatMap(extract_word) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1])

    # output = counts.collect()
    counts.coalesce(1, shuffle=True).saveAsTextFile("%s_wordcount" % file_name)
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))

    spark.stop()


def channel_title_count():
    """count channel title"""
    print("Ready for wordcount from file: %s" % file_name)

    spark = SparkSession \
        .builder \
        .appName("WordCount") \
        .getOrCreate()

    lines = spark.sparkContext.textFile(file_name) \
        .map(to_id_pair) \
        .reduceByKey(lambda x, y: x) \
        .map(lambda x: x[1])  # deduplicate by id

    print("lines count %s" % lines.count())

    counts = lines.map(extract_title) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1])

    counts.coalesce(1, shuffle=True).saveAsTextFile("%s_channel_title_count" % file_name)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='filename')
    parser.add_argument('--option', help='wordcount or channeltitle count')
    args = parser.parse_args()
    file_name = args.file
    if not args.file:
        print("Please input file name")
        exit(1)

    if not args.option:
        print("Please input which count you will do word or channeltitle")
        exit(1)

    if args.option == 'word':
        word_count()
    elif args.option == 'channeltitle':
        channel_title_count()
    else:
        print("options is word or channeltitle")
        exit(1)
