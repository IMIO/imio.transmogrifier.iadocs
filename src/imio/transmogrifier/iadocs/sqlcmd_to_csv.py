# -*- coding: utf-8 -*-
"""Script to clean sqlcmd output files to double-quoted csv"""
from collections import OrderedDict
from imio.pyutils.system import read_dir
from imio.pyutils.system import stop
from imio.pyutils.utils import safe_encode

import argparse
import codecs
import csv
import logging
import os
import re


logging.basicConfig()
logger = logging.getLogger('csv')
logger.setLevel(logging.INFO)
sqlcmd_ext = '.fwf'
sqlcmd_sep = u'|'


def main(input_dir, output_dir, input_filter=''):
    files = read_dir(input_dir, with_path=False, only_folders=False, only_files=True)
    for filename in files:
        if not filename.endswith(sqlcmd_ext) or (input_filter and not re.match(input_filter, filename)):
            continue
        input_name = os.path.join(input_dir, filename)
        output_name = os.path.join(output_dir, filename.replace(sqlcmd_ext, '.csv'))
        logger.info("Reading '{}'".format(input_name))
        # with open(input_name) as ifh, open(output_name, 'wb') as ofh:
        with codecs.open(input_name, 'r', encoding='utf8') as ifh, open(output_name, 'wb') as ofh:
            csvh = csv.writer(ofh, quoting=csv.QUOTE_NONNUMERIC)
            rec_nb, last_rec_pos = get_records_info(ifh)
            cols = get_cols(ifh)
            csvh.writerow(cols.keys())
            ctn, values = get_values(cols, ifh, )
            writed = 0
            while ctn:
                csvh.writerow(values)
                writed += 1
                ctn, values = get_values(cols, ifh)


def get_values(cols, fh):
    """Reads row fields and return values and finished flag.

    :param cols: ordered dict containing column: width
    :param fh: input file handler
    :return: continue flag, columns values
    """
    values = []
    for col, clen in cols.items():
        value = fh.read(clen)
        if not value:
            break  # end of file
        elif value.startswith(u' ') and not value.endswith(u' '):
            value = value.lstrip(u' ')
            try:
                value = int(value)
            except Exception:
                pass
        else:
            value = value.strip(u' ')
        values.append(safe_encode(value))
        next_char = fh.read(1)
        if next_char and next_char not in (sqlcmd_sep, u'\n'):
            logger.error(u"Found char '{}' after col {}".format(next_char, col))
            break
    else:
        return True, values
    return False, values


def get_cols(fh):
    """Gets columns and lengths"""
    header = fh.readline()
    if not header:
        stop('File is empty !', logger)
    header = header.rstrip(u'\n')
    parts = header.split(sqlcmd_sep)
    cols = OrderedDict()
    for part in parts:
        cols[part.strip()] = len(part)
    # check second row (does'nt yet contain \n)
    cont, values = get_values(cols, fh)
    for i, col in enumerate(cols):
        clen = cols[col]
        if values[i] != u'-' * clen:
            stop(u"Wrong length in second line for col '{}'".format(col), logger)
    return cols


def get_records_info(fh):
    """Get records number and last record position."""
    fh.seek(0, 2)
    file_len = fh.tell()
    offset = -16
    fh.seek(offset, 2)  # just before ' rows'
    buf = fh.read()
    if buf != u' rows affected)\n':
        stop(u"End of file not as expected '{}'".format(buf))
    offset -= 1
    while not re.match(r'\n\(\d+ rows ', buf) and abs(offset) <= file_len:
        fh.seek(offset, 2)
        buf = fh.read()
        offset -= 1
    match = re.match(r'\n\((\d+) rows ', buf)
    fh.seek(0)  # start of file
    return match.group(1), file_len + offset  # offset is neg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert sqlcmd files to csv.')
    parser.add_argument('input_dir', help='Input directory.')
    parser.add_argument('-if', '--input_filter', dest='input_filter', help='Input filter.')
    parser.add_argument('-od', '--output_dir', dest='output_dir', help='Output directory.')
    ns = parser.parse_args()
    if not ns.output_dir:
        ns.output_dir = ns.input_dir
    main(ns.input_dir, ns.output_dir, ns.input_filter)
