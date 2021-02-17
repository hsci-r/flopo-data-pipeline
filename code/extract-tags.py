#!/usr/bin/env python3
import os
import csv
import regex
import json
import argparse

def extract_tags(input_directory: str):
    d = {}
    with open(os.path.join(input_directory,"assets_output.csv")) as af:
        cr = csv.reader(af)
        next(cr)
        for row in cr:
            # id,resourcetype,startdate,modifieddate,title,data,custom,timestamp,nodeid,body,splitbody
            if row[1] == 'article':
                title = row[4]
                ingress = json.loads(row[5])['ingress']
                body = row[9]
                for m in regex.findall(r'</?[^>]*/?>|&\w+;',body):
                    print(regex.sub(r'id=\'autoresize[^\']*\'|["\']http[^"\']*["\']|["\']/[^"\']*["\']|data-id=["\'][^"\']*["\']','',m))
                for m in regex.findall(r'</?[^>]*/?>|&\w+;',ingress):
                    print(regex.sub(r'id=\'autoresize[^\']*\'|["\']http[^"\']*["\']|["\']/[^"\']*["\']|data-id=["\'][^"\']*["\']','',m))
                for m in regex.findall(r'</?[^>]*/?>|&\w+;',title):
                    print(regex.sub(r'id=\'autoresize[^\']*\'|["\']http[^"\']*["\']|["\']/[^"\']*["\']|data-id=["\'][^"\']*["\']','',m))
                

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i","--input-directory",help="input directory",required=True)
    return(parser.parse_args())


def main() -> None:
    args = parse_arguments()
    extract_tags(args.input_directory)
                

if __name__ == '__main__':
    main()
