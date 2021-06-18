#!/usr/bin/env python3
"""Script to parse IL into the TurkuNLP input format
"""
import glob
import functools
import multiprocessing
import itertools
import argparse
import os
import logging
import json
from typing import Optional,Any
import regex
from utils.clean_text import clean_text

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

def parse_body(node: Any) -> str:
    content = node['text'] if 'text' in node else ''
    if node['type']=='list':
        for entry in node['items']:
            content += '\n * '
            for child in entry:
                content += parse_body(child)
        content += '\n'
    elif node['type']=='list-ordered':
        for index,entry in enumerate(node['items']):
            content += f'\n {index+1}. '
            for child in entry:
                content += parse_body(child)
        content += '\n'        
    elif 'items' in node:
        for child in node['items']:
            content += parse_body(child)
    #if node['type']=='image' and 'caption' in node['properties']:
    #    str += node['properties']['caption']+'\n\n'
    if node['type'] == 'paragraph':
        content += '\n\n'
    return content

def yield_article(file: str):
    id = os.path.basename(file)   
    try:
        with open(file) as ir:
            html = ir.read()
            match = regex.search(r'({"article_id":.*}),"lastUpdated":\d+}},"authorInfo":',html)
            if match is None:
                logging.error("Couldn't find article json in %s.",file)
                return None
            article = json.loads(match.group(1))
            headline = clean_text(article['title'])
            lead = clean_text(article['lead']) if article['lead']!='' else None
            body = ""
            for elem in article['body']:
                body += parse_body(elem)
            body = clean_text(body)
            return Article(id,headline,lead,body)
    except:
        logging.exception("Error processing %s",id)

def process(prefix: int,input_files: list[str], output_directory: str, split: int):
    current_output = None
    i = 0
    try:
        for input_file in input_files:
            article = yield_article(input_file)
            if article is not None:
                if i % split == 0:
                    if current_output is not None:
                        current_output.close()
                    logging.info("Creating chunk %s-%d.",prefix,i)
                    current_output = open(os.path.join(output_directory,f"chunk-{prefix}-{i}.txt"),"w")
                if article.title is not None:
                    current_output.write(f'###C: {article.id}_title\n')
                    current_output.write(article.title)
                    current_output.write('\n\n')
                current_output.write(f'###C: {article.id}_body\n')
                current_output.write(article.body)
                current_output.write('\n\n')
                i += 1
    finally:
        if current_output is not None:
            current_output.close()


# process("/Users/jiemakel/tyo/flopo-data-pipeline/data/input/yle_sample","/Users/jiemakel/tyo/flopo-data-pipeline/data/processed/for-turkunlp/yle_sample", 500)


# %%
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--split",type=int,help="number of articles to put in each file",default=5000)
    parser.add_argument("-i","--input-directory",help="input directory",nargs="+")
    parser.add_argument("-o","--output-directory",help="output directory",required=True)
    parser.add_argument("-p","--processes",help="number of processes to use",type=int,default=len(os.sched_getaffinity(0)) if hasattr(os,'sched_getaffinity') else os.cpu_count())
    return parser.parse_args()

def chunks(lst, chunk_size):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def main() -> None:
    args = parse_arguments()
    os.makedirs(args.output_directory,exist_ok=True)
    files = list(itertools.chain.from_iterable([ glob.glob(os.path.join(input_directory,"**","*.html"),recursive=True) for input_directory in args.input_directory]))
    cores = args.processes
    with multiprocessing.Pool(cores) as p:
        p.starmap(functools.partial(process,output_directory=args.output_directory,split=args.split),enumerate(chunks(files,max(1,len(files)//cores))))

if __name__ == '__main__':
    main()
