#!/usr/bin/env python3
"""Script to parse YLE into the TurkuNLP input format
"""

import argparse
import functools
import glob
import itertools
import logging
import multiprocessing
import os
from typing import Iterator, Optional

import ijson.backends.yajl2_cffi as ijson
from utils.clean_text import clean_text

logging.basicConfig(level=logging.INFO)


class Article:
    id: str
    title: Optional[str]
    ingress: Optional[str]
    body: list[tuple[int, str]]

    def __init__(self, id: str, title: Optional[str], ingress: Optional[str], body: list[tuple[int, str]]):
        self.id = id
        self.title = title
        self.ingress = ingress
        self.body = body


def yield_articles(filename: str) -> Iterator[Article]:
    logging.info("Processing %s", filename)
    with open(filename, 'rb') as articles_file:
        try:
            for article in ijson.items(articles_file, 'data.item'):
                article_id = article['id']
                title = article['headline']['full'] if 'headline' in article else None
                ingress = article['lead'] if 'lead' in article else None
                body = [(index, clean_text(content['text'])) for index, content in enumerate(
                    article['content']) if 'text' in content and isinstance(content['text'], str)]
                yield Article(article_id, title, ingress, body)
        except Exception:
            logging.error("Exception parsing %s.", filename)
            raise


def process(prefix: int, input_files: list[str], output_directory: str, split: int):
    current_output = None
    i = 0
    try:
        for input_file in input_files:
            for article in yield_articles(input_file):
                if i % split == 0:
                    if current_output is not None:
                        current_output.close()
                    logging.info("Creating chunk %s-%d.", prefix, i)
                    current_output = open(os.path.join(
                        output_directory, f"chunk-{prefix}-{i}.txt"), "w")
                if article.title is not None:
                    current_output.write(f'###C: {article.id}_title\n')
                    current_output.write(article.title)
                    current_output.write('\n\n')
                if article.ingress is not None:
                    current_output.write(f'###C: {article.id}_ingress\n')
                    current_output.write(article.ingress)
                    current_output.write('\n\n')
                for (index, text) in article.body:
                    current_output.write(f'###C: {article.id}_body_{index}\n')
                    current_output.write(text)
                    current_output.write('\n\n')
                i += 1
    finally:
        if current_output is not None:
            current_output.close()


# process("/Users/jiemakel/tyo/flopo-data-pipeline/data/input/yle_sample","/Users/jiemakel/tyo/flopo-data-pipeline/data/processed/for-turkunlp/yle_sample", 500)


# %%
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--split", type=int,
                        help="number of articles to put in each file", default=5000)
    parser.add_argument("-i", "--input-directory", help="input directory", nargs="+")
    parser.add_argument("-o", "--output-directory", help="output directory", required=True)
    parser.add_argument("-p", "--processes", help="number of processes to use", type=int,
                        default=len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count())
    return parser.parse_args()


def chunks(lst, chunk_size):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def main() -> None:
    args = parse_arguments()
    os.makedirs(args.output_directory, exist_ok=True)
    files = list(itertools.chain.from_iterable([glob.glob(os.path.join(
        input_directory, "**", "*.json"), recursive=True) for input_directory in args.input_directory]))
    cores = args.processes
    with multiprocessing.Pool(cores) as pool:
        pool.starmap(functools.partial(process, output_directory=args.output_directory,
                                       split=args.split), enumerate(chunks(files, max(1, len(files)//cores))))


if __name__ == '__main__':
    main()

# %%
