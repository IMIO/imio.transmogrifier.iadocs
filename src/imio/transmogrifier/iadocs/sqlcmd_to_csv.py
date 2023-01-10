# -*- coding: utf-8 -*-
"""Script to clean sqlcmd output files to double-quoted csv"""
from collections import OrderedDict
from imio.pyutils.system import read_dir
from imio.pyutils.system import stop

import argparse
import csv
import logging
import os
import re


logging.basicConfig()
logger = logging.getLogger('csv')
logger.setLevel(logging.INFO)
sqlcmd_ext = '.fcsv'
sqlcmd_sep = '|'


def main(input_dir, output_dir, input_filter=''):
    files = read_dir(input_dir, with_path=False, only_folders=False, only_files=True)
    for filename in files:
        if not filename.endswith(sqlcmd_ext) or (input_filter and not re.match(input_filter, filename)):
            continue
        input_name = os.path.join(input_dir, filename)
        output_name = os.path.join(output_dir, filename.replace(sqlcmd_ext, '.csv'))
        logger.info("Reading '{}'".format(input_name))
        with open(input_name) as ifh, open(output_name, 'wb') as ofh:
            csvh = csv.writer(ofh, quoting=csv.QUOTE_NONNUMERIC)
            # read first line to analyse cols
            cols = get_cols(ifh)
            csvh.writerow(cols.keys())
            # while get_values(cols, ifh):
            #     csvh.writerow(values)
            #     values = []


def get_values(cols, fh):
    """Reads row fields and return values and finished flag"""
    values = []
    for col, clen in cols.items():
        value = fh.read(clen)
        if not value:
            break  # end of file
        elif value.startswith(' ') and not value.endswith(' '):
            value = value.lstrip(' ')
            try:
                value = int(value)
            except Exception:
                pass
        else:
            value = value.strip()
        values.append(value)
        next_char = fh.read(1)
        if next_char and next_char not in (sqlcmd_sep, '\n'):
            logger.error("Found char '{}' after col {}".format(next_char, col))
            break
    else:
        return True, values
    return False, values


def get_cols(fh):
    """Gets columns and lengths"""
    header = fh.readline()
    if not header:
        stop('File is empty !', logger)
    header = header.rstrip('\n')
    parts = header.split(sqlcmd_sep)
    cols = OrderedDict()
    for part in parts:
        cols[part.strip()] = len(part)
    # check second row (does'nt yet contain \n)
    cont, values = get_values(cols, fh)
    for i, col in enumerate(cols):
        clen = cols[col]
        if values[i] != '-' * clen:
            stop("Wrong length in second line for col '{}'".format(col), logger)
    return cols


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert sqlcmd files to csv.')
    parser.add_argument('input_dir', help='Input directory.')
    parser.add_argument('-if', '--input_filter', dest='input_filter', help='Input filter.')
    parser.add_argument('-od', '--output_dir', dest='output_dir', help='Output directory.')
    ns = parser.parse_args()
    if not ns.output_dir:
        ns.output_dir = ns.input_dir
    main(ns.input_dir, ns.output_dir, ns.input_filter)
