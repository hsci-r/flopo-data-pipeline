#!/usr/bin/env python3

# %%

import os
import logging
from typing import Iterator, Optional
import ijson.backends.yajl2_cffi as ijson
import argparse
import glob
from utils.clean_text import clean_text

logging.basicConfig(level=logging.INFO)

# %%

class Article:
    id: str
    title: Optional[str]
    ingress: Optional[str]
    body: list[(int,Optional[str])]

    def __init__(self, id: str, title: Optional[str], ingress: Optional[str], body: list[(int,str)]):
        self.id = id
        self.title = title
        self.ingress = ingress
        self.body = body

def yield_articles(input_directory: str) -> Iterator[Article]:
    for file in glob.glob(os.path.join(input_directory,"**","*.json"),recursive=True):
        logging.info(f"Processing {file}")
        with open(file) as af:
            for article in ijson.items(af, 'data.item'):
                id = article['id']
                if 'headline' in article:
                    title = article['headline']['full']
                if 'lead' in article:
                    ingress = article['lead']
                body = [ (index,clean_text(content['text'])) for index,content in enumerate(article['content']) if 'text' in content ]
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
            if article.title is not None:
                co.write(f'###C: {article.id}_title\n')
                co.write(article.title)
                co.write('\n\n')
            if article.ingress is not None:
                co.write(f'###C: {article.id}_ingress\n')
                co.write(article.ingress)
                co.write('\n\n')
            for (index,text) in article.body:
                co.write(f'###C: {article.id}_body_{index}\n')
                co.write(text)
                co.write('\n\n')
            i += 1
    finally:
        if co is not None:
            co.close()


# process("/Users/jiemakel/tyo/flopo-data-pipeline/data/input/yle_sample","/Users/jiemakel/tyo/flopo-data-pipeline/data/processed/for-turkunlp/yle_sample", 500)


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
