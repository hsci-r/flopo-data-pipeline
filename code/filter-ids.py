#!/usr/bin/env python3
import argparse
import csv
import glob
import json
import logging
import os

import ijson.backends.yajl2_cffi as ijson

logging.basicConfig(level=logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-directory", help="input directory", required=True)
    parser.add_argument("-f", "--filter-csv", help="filter CSV", required=True)
    parser.add_argument("-o", "--output", help="output json file", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    filter = set()
    with open(args.filter_csv) as input_file:
        csv_input = csv.reader(input_file)
        for row in csv_input:
            filter.add(row[0])
    logging.info("Filter has %d identifiers", len(filter))
    with open(args.output, 'w') as output_file:
        output_file.write('{ "data": [\n')
        first = True
        for file in glob.glob(os.path.join(args.input_directory, "**", "*.json"), recursive=True):
            logging.info("Processing %s...", file)
            with open(file) as input_file:
                for article in ijson.items(input_file, 'data.item', use_float=True):
                    year_published = int(article['datePublished'][:4])
                    if article['id'] not in filter and article['language'] == 'fi' and year_published < 2011:
                        if first is True:
                            first = False
                        else:
                            output_file.write(",\n")
                        output_file.write(json.dumps(article))

        output_file.write(']}')


if __name__ == '__main__':
    main()
