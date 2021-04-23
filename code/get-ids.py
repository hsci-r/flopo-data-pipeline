#!/usr/bin/env python3
import os
import csv
import ijson.backends.yajl2_cffi as ijson
import glob
import argparse
import logging

logging.basicConfig(level=logging.INFO)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input-directory",help="input directory",required=True)
    parser.add_argument("-o","--output",help="output CSV",required=True)
    return(parser.parse_args())


def main() -> None:
    args = parse_arguments()
    with open(args.output,'w') as of:
        ow = csv.writer(of)
        for file in glob.glob(os.path.join(args.input_directory,"**","*.json"),recursive=True):
            logging.info(f"Processing {file}...")
            with open(file) as af:
                for id in ijson.items(af, 'data.item.id'):
                    ow.writerow([id])

if __name__ == '__main__':
    main()
