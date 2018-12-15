from datetime import datetime
from elasticsearch import Elasticsearch
import json
import argparse

es = Elasticsearch()


def write_from_file(filename):
    with open(filename, 'r') as f:
        for line in f.readlines():
            doc = json.loads(line.strip())
            id = doc.get("id").get("videoId")
            es.index(index="ybvideos", id=id, doc_type="_doc", body=doc)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', help='filename')
    args = parser.parse_args()
    filename = args.file
    if not args.file:
        print("Please input file name")
        exit(0)
    print("ready for upsert index from file: %s" % filename)
    write_from_file(filename)
