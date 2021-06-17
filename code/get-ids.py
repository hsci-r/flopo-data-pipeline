#!/usr/bin/env python3
import argparse
import csv
import glob
import logging
import os

import ijson.backends.yajl2_cffi as ijson

logging.basicConfig(level=logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-directory", help="input directory", required=True)
    parser.add_argument("-o", "--output", help="output CSV", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    with open(args.output, 'w') as output_file:
        csv_output = csv.writer(output_file)
        for input_file_name in glob.glob(os.path.join(args.input_directory, "**", "*.json"), recursive=True):
            logging.info("Processing %s...", input_file_name)
            with open(input_file_name) as input_file:
                for article_id in ijson.items(input_file, 'data.item.id'):
                    csv_output.writerow([article_id])


if __name__ == '__main__':
    main()
