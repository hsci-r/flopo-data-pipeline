#!/usr/bin/env python3

# %%

import os
import logging
from typing import Iterator, Optional
import ijson.backends.yajl2_cffi as ijson
import argparse
import glob
import functools
import multiprocessing
import itertools
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

def yield_articles(file: str) -> Iterator[Article]:
    logging.info(f"Processing {file}")
    with open(file) as af:
        try:
            for article in ijson.items(af, 'data.item'):
                id = article['id']
                title = article['headline']['full'] if 'headline' in article else None
                ingress = article['lead'] if 'lead' in article else None
                body = [ (index,clean_text(content['text'])) for index,content in enumerate(article['content']) if 'text' in content and type(content['text']) is str]
                yield Article(id,title,ingress,body)
        except Exception as e:
            logging.error(f"Exception parsing {file}.")
            raise

def process(prefix: int,input_files: list[str], output_directory: str, split: int):
    co = None
    i = 0
    try:
        for input_file in input_files:
            for article in yield_articles(input_file):
                if i % split == 0:
                    if co is not None:
                        co.close()
                    logging.info(f"Creating chunk {prefix}-{i}.")
                    co = open(os.path.join(output_directory,f"chunk-{prefix}-{i}.txt"),"w")
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
    parser.add_argument("-i","--input-directory",help="input directory",nargs="+")
    parser.add_argument("-o","--output-directory",help="output directory",required=True)
    parser.add_argument("-p","--processes",help="number of processes to use",type=int,default=len(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else os.cpu_count())
    return(parser.parse_args())

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def main() -> None:
    args = parse_arguments()
    os.makedirs(args.output_directory,exist_ok=True)
    files = list(itertools.chain.from_iterable([ glob.glob(os.path.join(input_directory,"**","*.json"),recursive=True) for input_directory in args.input_directory]))
    cores = args.processes
    with multiprocessing.Pool(cores) as p:
       p.starmap(functools.partial(process,output_directory=args.output_directory,split=args.split),enumerate(chunks(files,max(1,len(files)//cores))))

if __name__ == '__main__':
    main()

# %%
