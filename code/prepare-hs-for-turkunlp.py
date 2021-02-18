#!/usr/bin/env python3

# %%

import os
import logging
import csv
import json
import argparse
import regex
import html
from bs4 import BeautifulSoup,NavigableString
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

def clean_text(txt: str):
    # html formatting
    txt = txt.replace('<span class="ndash">&ndash;</span>','-')
    txt = regex.sub(r'<div[^>]*><div[^>]*></div></div>','\n',txt)
    txt = regex.sub(r'<div[^>]*></div>','\n',txt)
    txt = txt.replace('<div class="quotes">&nbsp;</div>','')
    txt = txt.replace('<p class="imgplaceholder left">&nbsp;</p>','')
    txt = txt.replace('<p class="imgplaceholder left">&nbps;</p>','') # sic
    txt = txt.replace('<p class="imgplaceholder center">&nbsp;</p>','')
    txt = txt.replace('<p class="imgplaceholder right">&nbsp;</p>','')
    txt = txt.replace('<span class="pi_BlackSquare">&nbsp;</span>',' * ')
    txt = regex.sub(r'<p class="videoplaceholder"[^>]*>&nbsp;?</p>','',txt)
    txt = txt.replace('<li>',' * ').replace('</li>','')
    txt = txt.replace('<h2[^>]*>','\n').replace('</h2>','\n\n')
    txt = txt.replace('</p>','\n\n').replace('<p[^>]*>','')
    txt = txt.replace('<br />','\n')
    txt = regex.sub(r'<iframe[^>]*></iframe ?>','',txt)
    # markdown
    # markdown headings
    txt = regex.sub(r"^#+ ", "", txt, flags=regex.MULTILINE)
    while True:
        txt2 = regex.sub(r"_(.+?)_", r"\1", txt)  # markdown emphases
        txt2 = regex.sub(r"\*(.+?)\*", r"\1", txt2)  # markdown emphases
        if txt == txt2:
            break
        txt = txt2
    txt = regex.sub(r"\[([^\]]+?)\]\([^\)]+?\)", r"\1", txt)  # markdown links

    # normalization
    txt = regex.sub(r"^&bull; ", " * ", txt, flags=regex.MULTILINE)
    txt = txt.replace("&nbsp;", " ") \
        .replace("&#160;", " ") \
        .replace("&nbps;", "") \
        .replace("&39;", "'")
    txt = txt.replace("\xad", "")
    txt = txt.replace("\x95", "-")
    txt = txt.replace("\x96", "-")
    txt = txt.replace("\x94", '"')
    txt = txt.replace("–", "-")  # ndash
    txt = txt.replace("—", "-")  # mdash
    txt = txt.replace("\-", "-")
    txt = txt.replace("•", "*")  # bull
    txt = txt.replace("…", "...")  # hellip
    txt = txt.replace("“", "\"")  # ldquo
    txt = txt.replace("”", "\"")
    txt = txt.replace("’", "'")  # rsquo
    txt = regex.sub(r"\p{Zs}", " ", txt)  # weird spaces
    try: 
        s = BeautifulSoup(txt,'lxml')
        for bq in s.find_all('blockquote'):
            bq.insert_before(NavigableString('- '))
            for text in bq.find_all(string=True):
                text.replace_with(text.replace('\n',' '))
            bq.unwrap()
        txt = s.get_text() # remove all other HTML
    except:
        logging.warn("BeautifulSoup parsing failed.")
    txt = html.unescape(txt)

    # missing quote separators
    txt = regex.sub(r"^-([^ ]+)", r"- \1", txt, flags=regex.MULTILINE)
    # too many quote separators
    txt = regex.sub(r"^-  +", "- ", txt, flags=regex.MULTILINE)
    # multiple quote markers
    txt = regex.sub(r"^(- *-)+","- ", txt, flags=regex.MULTILINE)
    # remove superfluous paragraph breaks
    txt = regex.sub(r"\n\n\n+","\n\n",txt)
    txt = regex.sub(r"\n*$","",txt)
    return(txt)

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
