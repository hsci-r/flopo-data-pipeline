#!/usr/bin/env python3
import os
import csv
import ijson.backends.yajl2_cffi as ijson
import json
import glob
import argparse
import logging

logging.basicConfig(level=logging.INFO)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input-directory",help="input directory",required=True)
    parser.add_argument("-f","--filter-csv",help="filter CSV",required=True)
    parser.add_argument("-o","--output",help="output json file",required=True)
    return(parser.parse_args())


def main() -> None:
    args = parse_arguments()
    filter = set()
    with open(args.filter_csv) as inf:
        cr = csv.reader(inf)
        for row in cr:
            filter.add(row[0])
    logging.info(f"Filter has {len(filter)} identifiers")
    with open(args.output,'w') as ow:
        ow.write('{ data: [\n')
        for file in glob.glob(os.path.join(args.input_directory,"**","*.json"),recursive=True):
            logging.info(f"Processing {file}...")
            with open(file) as af:
                for article in ijson.items(af, 'data.item',use_float=True):
                    if article['id'] not in filter and article['language']=='fi':
                        ow.write(json.dumps(article))
                        ow.write(",\n")
                    else:
                        logging.info(f"Filtering {article['id']}")
        ow.write(']}')

if __name__ == '__main__':
    main()
