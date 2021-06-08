#!/usr/bin/env python3

# %%

import os
import logging
from typing import Iterator, Optional
import argparse
import glob
import functools
import multiprocessing
import itertools
import re
from utils.clean_text import clean_text

logging.basicConfig(level=logging.INFO)

# %%

class Article:
    id: str
    title: Optional[str]
    body: str

    def __init__(self, id: str, title: Optional[str], body: str):
        self.id = id
        self.title = title
        self.body = body

def yield_article(file: str) -> Article:
    logging.info(f"Processing {file}")
    id = os.path.basename(file)[:-4]
    headline = ""
    content = ""
    with open(file) as ir:
        try:
            for line in ir:
                if line == "<contentMeta>\n":
                    line = next(ir)
                    while line != "</contentMeta>\n":
                        if line.startswith("<headline>"):
                            if "</headline>" in line:
                                headline=re.match('<headline>(.*)</headline>',line).group(1)
                            else: 
                                while not "</headline>" in line:
                                    headline = headline + line
                                    line = next(ir)
                                headline  = headline + line
                                headline=re.match('<headline>(.*)</headline>',headline,re.DOTALL).group(1)
                        line = next(ir)
                elif line == "<html>\n":
                    line = next(ir)
                    while line != "</html>\n":
                        content += line
                        line = next(ir)
            return(Article(id,clean_text(headline) if headline!="" else None,clean_text(content)))
        except Exception as e:
            logging.error(f"Exception parsing {file}.")
            raise

def process(prefix: int,input_files: list[str], output_directory: str, split: int):
    co = None
    i = 0
    try:
        for input_file in input_files:
            article = yield_article(input_file)
            if i % split == 0:
                if co is not None:
                    co.close()
                logging.info(f"Creating chunk {prefix}-{i}.")
                co = open(os.path.join(output_directory,f"chunk-{prefix}-{i}.txt"),"w")
            if article.title is not None:
                co.write(f'###C: {article.id}_title\n')
                co.write(article.title)
                co.write('\n\n')
            co.write(f'###C: {article.id}_body\n')
            co.write(article.body)
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
    files = list(itertools.chain.from_iterable([ glob.glob(os.path.join(input_directory,"**","*.xml"),recursive=True) for input_directory in args.input_directory]))
    cores = args.processes
    with multiprocessing.Pool(cores) as p:
       p.starmap(functools.partial(process,output_directory=args.output_directory,split=args.split),enumerate(chunks(files,max(1,len(files)//cores))))

if __name__ == '__main__':
    main()

# %%
