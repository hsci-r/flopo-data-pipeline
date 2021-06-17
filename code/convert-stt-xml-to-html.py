#!/usr/bin/env python3
"""Script to convert STT XML into HTML
"""

import argparse
import glob
import logging
import os
import re

logging.basicConfig(level=logging.INFO)


def parse_arguments():
    parser = argparse.ArgumentParser(description="STT XML to HTML converter")
    parser.add_argument("-o", "--output", help="Output directory", required=True)
    parser.add_argument("input", help="Input STT XML files or directories", nargs="+")
    return parser.parse_args()

# %%


def main():
    args = parse_arguments()
    os.makedirs(args.output, exist_ok=True)
    for input_spec in args.input:
        if os.path.isdir(input_spec):
            input_glob = os.path.join(input_spec, "**", "*.xml")
        else:
            input_glob = input_spec
        for input_file_name in glob.glob(input_glob, recursive=True):
            logging.info("Processing %s", input_file_name)
            with open(input_file_name) as input_file, open(os.path.join(args.output, os.path.basename(input_file_name).replace(".xml", ".html")), 'w') as output_file:
                output_file.write('<html><head><meta charset="UTF-8"></head>\n')
                item_meta = []
                content_meta = []
                headline = ""
                content_created = ""
                for line in input_file:
                    if line == "<itemMeta>\n":
                        line = next(input_file)
                        while line != "</itemMeta>\n":
                            item_meta.append(line.replace("<", "&lt;").replace(
                                ">", "&gt;") + "<br />\n")
                            line = next(input_file)
                    elif line == "<contentMeta>\n":
                        line = next(input_file)
                        while line != "</contentMeta>\n":
                            if line.startswith("<headline>"):
                                if "</headline>" in line:
                                    headline = re.match('<headline>(.*)</headline>', line).group(1)
                                else:
                                    while not "</headline>" in line:
                                        headline = headline + line
                                        content_meta.append(line.replace(
                                            "<", "&lt;").replace(">", "&gt;") + "<br />\n")
                                        line = next(input_file)
                                    headline = headline + line
                                    headline = re.match('<headline>(.*)</headline>',
                                                        headline, re.DOTALL).group(1)
                            elif line.startswith("<contentCreated>"):
                                content_created = re.match(
                                    '<contentCreated>(.*)</contentCreated>', line).group(1)
                            content_meta.append(line.replace(
                                "<", "&lt;").replace(">", "&gt;") + "<br />\n")
                            line = next(input_file)
                    elif line == "<html>\n":
                        output_file.write(f'<h1>{headline}</h1>\n')
                        output_file.write(f'<p>{content_created}</p>')
                        line = next(input_file)
                        while line != "</html>\n":
                            output_file.write(line)
                            line = next(input_file)
                output_file.write('<hr />\n')
                output_file.write('<h2>Content metadata</h2>\n')
                output_file.write("\n".join(content_meta))
                output_file.write('<hr />\n')
                output_file.write('<h2>Item metadata</h2>\n')
                output_file.write("\n".join(item_meta))
                output_file.write('</html>\n')


if __name__ == '__main__':
    main()
