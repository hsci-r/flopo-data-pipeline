#!/usr/bin/env python3
import argparse
import csv
import ctypes
import json
import os

import regex


def extract_tags(input_directory: str):
    with open(os.path.join(input_directory, "assets_output.csv")) as assets_file:
        csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))
        csv_input = csv.reader(assets_file)
        next(csv_input)
        for row in csv_input:
            # id,resourcetype,startdate,modifieddate,title,data,custom,timestamp,nodeid,body,splitbody
            if row[1] == 'article':
                title = row[4]
                ingress = json.loads(row[5])['ingress']
                body = row[9]
                for match in regex.findall(r'</?[^>]*/?>|&\w+;', body):
                    print(regex.sub(
                        r'id=\'autoresize[^\']*\'|["\']http[^"\']*["\']|["\']/[^"\']*["\']|data-id=["\'][^"\']*["\']', '', match))
                for match in regex.findall(r'</?[^>]*/?>|&\w+;', ingress):
                    print(regex.sub(
                        r'id=\'autoresize[^\']*\'|["\']http[^"\']*["\']|["\']/[^"\']*["\']|data-id=["\'][^"\']*["\']', '', match))
                for match in regex.findall(r'</?[^>]*/?>|&\w+;', title):
                    print(regex.sub(
                        r'id=\'autoresize[^\']*\'|["\']http[^"\']*["\']|["\']/[^"\']*["\']|data-id=["\'][^"\']*["\']', '', match))


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input-directory", help="input directory", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    extract_tags(args.input_directory)


if __name__ == '__main__':
    main()
