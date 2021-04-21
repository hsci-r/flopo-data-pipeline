#!/usr/bin/env python3

# %%

import os
import logging
import csv
import json
import argparse
from utils.clean_text import clean_text
from typing import Optional

from regex.regex import Match

logging.basicConfig(level=logging.INFO)

# %%

class Article:
    id: str
    title: str
    ingress: Optional[str]
    body: str

    def __init__(self, id, title, ingress, body):
        self.id = id
        self.title = title
        self.ingress = ingress
        self.body = body

import ctypes

def yield_articles(input_directory: str):
    with open(os.path.join(input_directory,"assets_output.csv")) as af:
        csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
        cr = csv.reader(af)
        next(cr)
        for row in cr:
            # id,resourcetype,startdate,modifieddate,title,data,custom,timestamp,nodeid,body,splitbody
            if row[1] == 'article':
                id = row[0]
                title = clean_text(row[4])
                ingress = clean_text(json.loads(row[5])['ingress'])
                body = clean_text(row[9])
                yield Article(id,title,ingress,body)

def process(input_directory: str, output_directory: str, split: int):
    os.makedirs(output_directory,exist_ok=True)
    i = 0
    co = None
    try:
        for article in yield_articles(input_directory):
            if i % split == 0:
                if co is not None:
                    co.close()
                logging.info(f"Creating chunk {i}.")
                co = open(os.path.join(output_directory,f"chunk-{i}.txt"),"w")
            if article.title != '':
                co.write(f'###C: {article.id}_title\n')
                co.write(article.title)
                co.write('\n\n')
            if article.ingress != '':
                co.write(f'###C: {article.id}_ingress\n')
                co.write(article.ingress)
                co.write('\n\n')
            if article.body != '':
                co.write(f'###C: {article.id}_body\n')
                co.write(article.body)
                co.write('\n\n')
            i += 1
    finally:
        if co is not None:
            co.close()


# process("/Users/jiemakel/tyo/flopo-data-pipeline/data/input/hs_sample","/Users/jiemakel/tyo/flopo-data-pipeline/data/processed/hs_sample", 500)


# %%
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--split",type=int,help="number of articles to put in each file",default=5000)
    parser.add_argument("-i","--input-directory",help="input directory",required=True)
    parser.add_argument("-o","--output-directory",help="output directory",required=True)
    return(parser.parse_args())


def main() -> None:
    args = parse_arguments()
    process(args.input_directory,args.output_directory,args.split)
                

if __name__ == '__main__':
    main()

# %%
