#!/usr/bin/env python3

import os
import argparse
import glob
import logging
import re

logging.basicConfig(level=logging.INFO)

def parse_arguments():
    ap = argparse.ArgumentParser(description="STT XML to HTML converter")  
    ap.add_argument("-o","--output",help="Output directory",required=True)
    ap.add_argument("input",help="Input STT XML files or directories",nargs="+")
    return (ap.parse_args())

# %%

def main():
    args = parse_arguments()
    os.makedirs(args.output, exist_ok=True)
    for ig in args.input:
        if os.path.isdir(ig):
            iglob = os.path.join(ig,"**","*.xml")
        else:
            iglob = ig 
        for ifile in glob.glob(iglob,recursive=True):
            logging.info(f"Processing {ifile}")
            with open(ifile) as ir,open(os.path.join(args.output,os.path.basename(ifile).replace(".xml",".html")),'w') as ow:
                ow.write('<html><head><meta charset="UTF-8"></head>\n')
                itemMeta = []
                contentMeta = []
                headline = ""
                contentCreated = ""
                for line in ir:
                    if line == "<itemMeta>\n":
                        line = next(ir)
                        while line != "</itemMeta>\n":
                            itemMeta.append(line.replace("<","&lt;").replace(">","&gt;") + "<br />\n")
                            line = next(ir)
                    elif line == "<contentMeta>\n":
                        line = next(ir)
                        while line != "</contentMeta>\n":
                            if line.startswith("<headline>"):
                                if "</headline>" in line:
                                    headline=re.match('<headline>(.*)</headline>',line).group(1)
                                else: 
                                    while not "</headline>" in line:
                                        headline = headline + line
                                        contentMeta.append(line.replace("<","&lt;").replace(">","&gt;") + "<br />\n")
                                        line = next(ir)
                                    headline  = headline + line
                                    headline=re.match('<headline>(.*)</headline>',headline,re.DOTALL).group(1)
                            elif line.startswith("<contentCreated>"):
                                contentCreated=re.match('<contentCreated>(.*)</contentCreated>',line).group(1)
                            contentMeta.append(line.replace("<","&lt;").replace(">","&gt;") + "<br />\n")
                            line = next(ir)
                    elif line == "<html>\n":
                        ow.write(f'<h1>{headline}</h1>\n')
                        ow.write(f'<p>{contentCreated}</p>')
                        line = next(ir)
                        while line != "</html>\n":
                            ow.write(line)
                            line = next(ir)
                ow.write('<hr />\n')
                ow.write('<h2>Content metadata</h2>\n')
                ow.write("\n".join(contentMeta))
                ow.write('<hr />\n')
                ow.write('<h2>Item metadata</h2>\n')
                ow.write("\n".join(itemMeta))
                ow.write('</html>\n')

                
                    

if __name__ == '__main__':
    main()
