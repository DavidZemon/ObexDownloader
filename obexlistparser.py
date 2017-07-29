#!/usr/bin/python3
# @file    obexparser
# @author  David Zemon
#
# Created with: PyCharm

import time
import argparse
import io
import os
import urllib.request
from html.parser import HTMLParser, unescape
import concurrent.futures

OBEX_LISTING_FILE_LINK = 'http://obex.parallax.com/projects/?field_category_tid=All&items_per_page=All'
DEFAULT_OBEX_LISTING_FILE = os.path.join(os.getcwd(), 'obex.html')
DEFAULT_TABLE_FILE = os.path.join(os.getcwd(), 'obex_table.csv')
DEFAULT_COMPLETE_OBEX_DIR = os.path.join(os.getcwd(), 'complete_obex')


class ObexListParser(HTMLParser):
    LINK_HEADER = 'Link'

    def __init__(self, output):
        """
        :param output: Output file
        :type output io.TextIOWrapper
        """
        super().__init__()
        self._output = output
        self._inCell = False
        self._inLink = False
        self._table = []
        self._currentRow = None

    def feed(self, data):
        self._output.write('"%s",' % self.LINK_HEADER)
        super().feed(data)
        return self._table

    def handle_endtag(self, tag):
        if tag == 'tr':
            self._output.write('\n')
            if self._currentRow:
                self._table.append(self._currentRow)
                self._currentRow = None
        elif tag == 'td':
            self._inCell = False
            self._output.write(',')

    def handle_data(self, data):
        """
        :param data:
        :type data str
        """
        if self._inCell:
            content = ' '.join(data.strip().split())
            if content:
                self._output.write('"%s"' % content)
                self._currentRow.append(content)

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self._inCell = True
        if tag == 'tr':
            self._currentRow = []
            if not self._table:
                self._currentRow.append(self.LINK_HEADER)
        elif tag == 'a' and self._inCell:
            attribute_dict = dict(attrs)
            url = unescape(attribute_dict['href'])
            self._output.write('"%s",' % url)
            self._currentRow.append(url)

    def error(self, message):
        raise Exception(message)


class ObexObjectParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._getReady = False
        self._wereAlmostThere = False
        self._weMadeIt = False
        self._links = []

    def feed(self, data):
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
            self._links.append(['http://obex.parallax.com' + unescape(attribute_dict['href'])])

    def error(self, message):
        raise Exception(message)


def run():
    args = parse_args()

    listing = os.path.expanduser(args.listing)
    table_file_path = os.path.expanduser(args.table)
    output_directory = os.path.expanduser(args.output)

    print('Downloading. Please wait...')
    start_time = time.time()
    table = parse_obex_listing(listing, table_file_path)
    metadata = download_all_metadata(table)
    download_all_objects(metadata, output_directory)
    elapsed_time = time.time() - start_time
    print('All done! Download completed in %0.1f seconds.' % elapsed_time)


def parse_obex_listing(listingFile, table_file_path):
    assert os.path.exists(listingFile) and os.path.isfile(listingFile)

    with open(listingFile, 'r') as html_file:
        html_content = html_file.read()

    with open(table_file_path, 'w') as csv_file:
        parser = ObexListParser(csv_file)
        return parser.feed(html_content)


def download_obex_object_metadata(link, project_title):
    with urllib.request.urlopen(link) as response:
        html = response.read().decode()
    object_parser = ObexObjectParser()
    object_links = object_parser.feed(html)
    return project_title, object_links


def download_all_metadata(table):
    link_index = table[0].index(ObexListParser.LINK_HEADER)
    project_title_index = table[0].index('Project Title')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for row in table[1:]:
            link = row[link_index]
            project_title = row[project_title_index]
            futures.append(executor.submit(download_obex_object_metadata, link, project_title))
    results = [future.result() for future in futures]
    return dict(results)


def download_all_objects(metadata, obex_dir):
    os.makedirs(obex_dir, exist_ok=False)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for project_title, project_artifacts in metadata.items():
            project_dir_name = project_title.replace('/', '_')
            project_dir = os.path.join(obex_dir, project_dir_name)
            os.makedirs(project_dir)
            for artifact in project_artifacts:
                futures.append(executor.submit(download_object, project_dir, artifact[0], artifact[1]))
        [future.result() for future in futures]


def download_object(directory, url, name):
    with open(os.path.join(directory, name), 'wb') as output:
        with urllib.request.urlopen(url) as response:
            output.write(response.read())


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-l', '--listing', default=DEFAULT_OBEX_LISTING_FILE,
                        help='Path to the complete OBEX listing file. Download it from here: ' + OBEX_LISTING_FILE_LINK)
    parser.add_argument('-t', '--table', default=DEFAULT_TABLE_FILE,
                        help='Output file for the CSV-formatted OBEX table.')
    parser.add_argument('-o', '--output', default=DEFAULT_COMPLETE_OBEX_DIR,
                        help='Output directory for the complete OBEX. The directory MUST NOT exist.')

    return parser.parse_args()


if '__main__' == __name__:
    run()
