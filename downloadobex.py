#!/usr/bin/python3
# @file    obexparser
# @author  David Zemon
#
# Created with: PyCharm

import sys
import argparse
import concurrent.futures
import io
import os
import time
import urllib.error
import urllib.request
import zipfile
from html.parser import HTMLParser, unescape

OBEX_LISTING_FILE_LINK = 'http://obex.parallax.com/projects/?field_category_tid=All&items_per_page=All'
DEFAULT_OBEX_LISTING_FILE = os.path.join(os.getcwd(), 'obex.html')
DEFAULT_TABLE_FILE = os.path.join(os.getcwd(), 'obex_table.csv')
DEFAULT_COMPLETE_OBEX_DIR = os.path.join(os.getcwd(), 'complete_obex')


class ObexListParser(HTMLParser):
    LINK_HEADER = 'Link'

    def __init__(self, output=None):
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

    def handle_data(self, data):
        """
        :param data:
        :type data str
        """
        if self._inCell:
            content = ' '.join(data.strip().split())
            if content:
                self._write_to_output('"%s"' % content)
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
            self._write_to_output('"%s",' % url)
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
    table = parse_obex_listing(listing, table_file_path)
    metadata = download_all_metadata(table)
    download_all_objects(metadata, output_directory)
    elapsed_time = time.time() - start_time
    print('All done! Download completed in %0.1f seconds.' % elapsed_time)


def parse_obex_listing(listingFile, table_file_path):
    assert os.path.exists(listingFile) and os.path.isfile(listingFile)

    with open(listingFile, 'r') as html_file:
        html_content = html_file.read()

    if table_file_path:
        with open(table_file_path, 'w') as csv_file:
            parser = ObexListParser(csv_file)
            return parser.feed(html_content)
    else:
        return ObexListParser().feed(html_content)


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
            futures.append(executor.submit(download_object, project_dir, project_artifacts))
        [future.result() for future in futures]


def download_object(directory, artifacts):
    """
    Download an object from the OBEX
    :param directory Output directory for the artifacts
    :type directory str
    :param artifacts: List of name/url tuples of artifacts for the given OBEX object
    :type artifacts tuple
    """
    for url, name in artifacts:
        try:
            output_path = os.path.join(directory, name)
            with open(output_path, 'wb') as output:
                with urllib.request.urlopen(url) as response:
                    output.write(response.read())

            zips = find_zips(directory)
            while zips:
                for z in zips:
                    extract_and_remove(z, os.path.dirname(z))
                zips = find_zips(directory)
        except urllib.error.HTTPError:
            print('Failed to download from %s! Sorry about that :(' % url)


def find_zips(directory):
    result = []
    for root, directories, files in os.walk(directory):
        for f in files:
            if f.lower().endswith('.zip'):
                result.append(os.path.join(root, f))
    return result


def extract_and_remove(file_path, directory):
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_file:
            zip_file.extractall(directory)
    except zipfile.BadZipFile as e:
        raise Exception('Failed to extract ' + file_path, e)
    os.remove(file_path)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-l', '--listing', default=DEFAULT_OBEX_LISTING_FILE,
                        help='Path to the complete OBEX listing file. Download it from here: ' + OBEX_LISTING_FILE_LINK)
    parser.add_argument('-t', '--table', help='Output file for the CSV-formatted OBEX table. If not provided, it will '
                                              'only be stored temporarily in-memory and not written to disk.')
    parser.add_argument('-o', '--output', default=DEFAULT_COMPLETE_OBEX_DIR,
                        help='Output directory for the complete and uncompressed OBEX. The directory MUST NOT exist.')

    return parser.parse_args()


if '__main__' == __name__:
    run()
