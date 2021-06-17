#!/usr/bin/env python3
"""Script to parse STT into the TurkuNLP input format
"""

import argparse
import functools
import glob
import itertools
import logging
import multiprocessing
import os
import re
from typing import Optional

from utils.clean_text import clean_text

logging.basicConfig(level=logging.INFO)


class Article:
    id: str
    title: Optional[str]
    body: str

    def __init__(self, id: str, title: Optional[str], body: str):
        self.id = id
        self.title = title
        self.body = body


def yield_article(file: str) -> Article:
    logging.info("Processing %s", file)
    id = os.path.basename(file)[:-4]
    headline = ""
    content = ""
    with open(file) as input_file:
        try:
            for line in input_file:
                if line == "<contentMeta>\n":
                    line = next(input_file)
                    while line != "</contentMeta>\n":
                        if line.startswith("<headline>"):
                            if "</headline>" in line:
                                headline = re.match('<headline>(.*)</headline>', line).group(1)
                            else:
                                while not "</headline>" in line:
                                    headline = headline + line
                                    line = next(input_file)
                                headline = headline + line
                                headline = re.match('<headline>(.*)</headline>',
                                                    headline, re.DOTALL).group(1)
                        line = next(input_file)
                elif line == "<html>\n":
                    line = next(input_file)
                    while line != "</html>\n":
                        content += line
                        line = next(input_file)
            return Article(id, clean_text(headline) if headline != "" else None, clean_text(content))
        except Exception:
            logging.error("Exception parsing %s.", file)
            raise


def process(prefix: int, input_files: list[str], output_directory: str, split: int):
    current_output = None
    i = 0
    try:
        for input_file in input_files:
            article = yield_article(input_file)
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
        input_directory, "**", "*.xml"), recursive=True) for input_directory in args.input_directory]))
    cores = args.processes
    with multiprocessing.Pool(cores) as pool:
        pool.starmap(functools.partial(process, output_directory=args.output_directory,
                                       split=args.split), enumerate(chunks(files, max(1, len(files)//cores))))


if __name__ == '__main__':
    main()

# %%
