OBEX Downloader
===============

A simple script to download all objects from the Parallax Object
Exchange.

Requirements
------------

* Python 3
* An Internet connection
* Approximately 60 MB of disk space

Usage
-----

```
usage: downloadobex.py [-h] [-l LISTING] [-t TABLE] [-o OUTPUT]

optional arguments:
  -h, --help            show this help message and exit
  -l LISTING, --listing LISTING
                        Path to the complete OBEX listing file. Download it
                        from here: http://obex.parallax.com/projects/?field_ca
                        tegory_tid=All&items_per_page=All
  -t TABLE, --table TABLE
                        Output file for the CSV-formatted OBEX table.
  -o OUTPUT, --output OUTPUT
                        Output directory for the complete OBEX. The directory
                        MUST NOT exist.

```
