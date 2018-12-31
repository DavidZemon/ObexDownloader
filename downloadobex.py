#!/usr/bin/python3
# @file    downloadobex
# @author  David Zemon
#
# Created with: PyCharm

import sys
import argparse
import concurrent.futures
import os
import time
import urllib.error
import urllib.request
import zipfile
from html.parser import HTMLParser, unescape
from typing import TextIO, Tuple, List, Dict

OBEX_LISTING_FILE_LINK = 'http://obex.parallax.com/projects/?field_category_tid=All&items_per_page=All'
DEFAULT_COMPLETE_OBEX_DIR = os.path.join(os.getcwd(), 'complete_obex')


class ObexListParser(HTMLParser):
    LINK_HEADER = 'Link'

    def __init__(self, output: TextIO = None):
        super().__init__()
        self._output = output
        self._inHeader = False
        self._inCell = False
        self._inLink = False
        self._table = []
        self._currentRow = None
        self._skip = True

    def feed(self, data) -> List[List[str]]:
        self._write_to_output('"%s",' % self.LINK_HEADER)
        super().feed(data)
        return self._table

    def handle_endtag(self, tag):
        if tag == 'tr':
            self._write_to_output('\n')
            if self._currentRow:
                self._table.append(self._currentRow)
                self._currentRow = None
        elif tag == 'td':
            self._inCell = False
            self._write_to_output(',')
        elif tag == 'th':
            self._inHeader = False

    def handle_data(self, data: str):
        if self._inCell:
            content = ' '.join(data.strip().split())
            if content:
                self._write_to_output('"{}"'.format(content))
                self._currentRow.append(content)

    def handle_starttag(self, tag, attrs):
        if tag == 'td' or tag == 'th':
            if tag == 'th':
                self._inHeader = True
            self._inCell = True
        if tag == 'tr':
            self._currentRow = []
            if not self._table:
                self._currentRow.append(self.LINK_HEADER)
        elif tag == 'a' and self._inCell and not self._inHeader:
            attribute_dict = dict(attrs)
            url = unescape(attribute_dict['href'])
            self._write_to_output('"{}",'.format(url))
            self._currentRow.append(url)

    def error(self, message):
        raise Exception(message)

    def _write_to_output(self, content):
        if self._output:
            self._output.write(content)


class ObexObjectParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._getReady = False
        self._wereAlmostThere = False
        self._weMadeIt = False
        self._links = []

    def feed(self, data) -> List[str]:
        super().feed(data)
        return self._links

    def handle_endtag(self, tag):
        if self._weMadeIt and tag == 'a':
            self._weMadeIt = False
            self._wereAlmostThere = False

    def handle_data(self, data):
        stripped_data = data.strip()
        if self._getReady and stripped_data == 'Attachment':
            self._wereAlmostThere = True
        if self._weMadeIt:
            self._links[-1].append(' '.join(stripped_data.split()))

    def handle_starttag(self, tag, attrs):
        if tag == 'th':
            self._getReady = True
        elif tag == 'a' and self._wereAlmostThere:
            self._weMadeIt = True
            attribute_dict = dict(attrs)
            self._links.append([unescape(attribute_dict['href'])])

    def error(self, message):
        raise Exception(message)


def run() -> None:
    args = parse_args()

    if args.table:
        table_file_path = os.path.expanduser(args.table)
    else:
        table_file_path = None
    output_directory = os.path.expanduser(args.output)

    if os.path.exists(output_directory):
        print('Can not proceed! The output directory (%s) already exists.' % output_directory, file=sys.stderr)
        exit(1)

    print('Downloading. Please wait...')
    start_time = time.time()
    listing = get_obex_listing(OBEX_LISTING_FILE_LINK)
    table = parse_obex_listing(listing, table_file_path)
    metadata = download_all_metadata(table)
    download_all_objects(metadata, output_directory)
    elapsed_time = time.time() - start_time
    print('All done! Download completed in %0.1f seconds.' % elapsed_time)


def get_obex_listing(link: str) -> str:
    response = urllib.request.urlopen(link)
    return response.read().decode()


def parse_obex_listing(html_content: str, table_file_path: str) -> List[List[str]]:
    if table_file_path:
        with open(table_file_path, 'w') as csv_file:
            parser = ObexListParser(csv_file)
            return parser.feed(html_content)
    else:
        return ObexListParser().feed(html_content)


def download_all_metadata(table: List[List[str]]) -> Dict[str, List[str]]:
    link_index = table[0].index(ObexListParser.LINK_HEADER)
    project_title_index = table[0].index('Project Title')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for row in table[1:-1]:
            link = row[link_index]
            project_title = row[project_title_index]
            futures.append(executor.submit(download_obex_object_metadata, link, project_title))
    results = [future.result() for future in futures]
    return dict(results)


def download_obex_object_metadata(link: str, project_title: str) -> Tuple[str, List[str]]:
    full_link = 'http://obex.parallax.com' + link
    with urllib.request.urlopen(full_link) as response:
        html = response.read().decode()
    object_parser = ObexObjectParser()
    object_links = object_parser.feed(html)
    return project_title, object_links


def download_all_objects(metadata: Dict[str, List[str]], obex_dir) -> None:
    os.makedirs(obex_dir, exist_ok=False)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for project_title, project_artifacts in metadata.items():
            project_dir_name = project_title.replace('/', '_')
            project_dir = os.path.join(obex_dir, project_dir_name)
            os.makedirs(project_dir)
            futures.append(executor.submit(download_object, project_dir, project_artifacts))
        [future.result() for future in futures]


def download_object(directory: str, artifacts: List[Tuple[str]]) -> None:
    """
    Download an object from the OBEX
    :param directory Output directory for the artifacts
    :param artifacts: List of name/url tuples of artifacts for the given OBEX object
    """
    for url, name in artifacts:
        try:
            output_path = os.path.join(directory, name)
            with open(output_path, 'wb') as output:
                with urllib.request.urlopen(url) as response:
                    output.write(response.read())

            zips = find_zips(directory)
            extracted_zips = []
            while set(zips) - set(extracted_zips):
                for z in zips:
                    extract(z, os.path.dirname(z))
                extracted_zips += zips
                zips = find_zips(directory)
        except Exception as e:
            print('Failed to download from {0}! Sorry about that :( -- {1}'.format(url, str(e)))


def find_zips(directory: str) -> List[str]:
    result = []
    for root, directories, files in os.walk(directory):
        for f in files:
            if f.lower().endswith('.zip'):
                result.append(os.path.join(root, f))
    return result


def extract(file_path: str, directory: str) -> None:
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extractall(directory)
    except zipfile.BadZipFile as e:
        raise Exception('Failed to extract ' + file_path, e)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument('-t', '--table', help='Output file for the CSV-formatted OBEX table. If not provided, it will '
                                              'only be stored temporarily in-memory and not written to disk.')
    parser.add_argument('-o', '--output', default=DEFAULT_COMPLETE_OBEX_DIR,
                        help='Output directory for the complete and uncompressed OBEX. The directory MUST NOT exist.')

    return parser.parse_args()


if '__main__' == __name__:
    run()
