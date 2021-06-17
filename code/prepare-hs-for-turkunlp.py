#!/usr/bin/env python3
"""Script to parse HS into the TurkuNLP input format
"""

import argparse
import csv
import ctypes
import json
import logging
import os

from utils.clean_text import clean_text

logging.basicConfig(level=logging.INFO)

# %%


class Article:
    id: str
    title: str
    ingress: str
    body: str

    def __init__(self, id, title, ingress, body):
        self.id = id
        self.title = title
        self.ingress = ingress
        self.body = body


def yield_articles(input_directory: str):
    with open(os.path.join(input_directory, "assets_output.csv")) as input_file:
        csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
        csv_input = csv.reader(input_file)
        next(csv_input)
        for row in csv_input:
            # id,resourcetype,startdate,modifieddate,title,data,custom,timestamp,nodeid,body,splitbody
            if row[1] == 'article':
                id = row[0]
                title = clean_text(row[4])
                ingress = clean_text(json.loads(row[5])['ingress'])
                body = clean_text(row[9])
                yield Article(id, title, ingress, body)


def process(input_directory: str, output_directory: str, split: int):
    os.makedirs(output_directory, exist_ok=True)
    i = 0
    current_output = None
    try:
        for article in yield_articles(input_directory):
            if i % split == 0:
                if current_output is not None:
                    current_output.close()
                logging.info("Creating chunk %d.", i)
                current_output = open(os.path.join(output_directory, f"chunk-{i}.txt"), "w")
            if article.title != '':
                current_output.write(f'###C: {article.id}_title\n')
                current_output.write(article.title)
                current_output.write('\n\n')
            if article.ingress != '':
                current_output.write(f'###C: {article.id}_ingress\n')
                current_output.write(article.ingress)
                current_output.write('\n\n')
            if article.body != '':
                current_output.write(f'###C: {article.id}_body\n')
                current_output.write(article.body)
                current_output.write('\n\n')
            i += 1
    finally:
        if current_output is not None:
            current_output.close()


# process("/Users/jiemakel/tyo/flopo-data-pipeline/data/input/hs_sample","/Users/jiemakel/tyo/flopo-data-pipeline/data/processed/hs_sample", 500)


# %%
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--split", type=int,
                        help="number of articles to put in each file", default=5000)
    parser.add_argument("-i", "--input-directory", help="input directory", required=True)
    parser.add_argument("-o", "--output-directory", help="output directory", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    process(args.input_directory, args.output_directory, args.split)


if __name__ == '__main__':
    main()

# %%
