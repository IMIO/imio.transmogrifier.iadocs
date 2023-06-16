# -*- coding: utf-8 -*-
"""Script to convert sqlcmd output files to double-quoted csv"""
from collections import OrderedDict
from datetime import datetime
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


def main(input_dir, output_dir, counter_col, input_filter, input_sep, only_new):
    start = datetime.now()
    logger.info("Start: {}".format(start.strftime('%Y%m%d-%H%M')))
    files = read_dir(input_dir, with_path=False, only_folders=False, only_files=True)
    for filename in files:
        if not filename.endswith(sqlcmd_ext) or (input_filter and not re.match(input_filter, filename)):
            continue
        input_name = os.path.join(input_dir, filename)
        output_name = os.path.join(output_dir, filename.replace(sqlcmd_ext, '.csv'))
        if only_new and os.path.exists(output_name):
            continue
        logger.info("Reading '{}'".format(input_name))
        with codecs.open(input_name, 'r', encoding='utf8') as ifh, open(output_name, 'wb') as ofh:
            csvh = csv.writer(ofh, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
            rec_nb, last_rec_pos = get_records_info(ifh)
            cols = get_cols(ifh, input_sep)
            if counter_col:
                csvh.writerow([u'Line'] + list(cols.keys()))
            else:
                csvh.writerow(list(cols.keys()))
            counters = {'read': 0, 'max': rec_nb}
            ctn, values = get_values(cols, ifh, counters, input_sep)
            writed = 0
            while ctn:
                if counter_col:
                    values.insert(0, counters['read'])
                csvh.writerow(values)
                writed += 1
                ctn, values = get_values(cols, ifh, counters, input_sep)
            if writed != rec_nb:
                logger.error("We don't have the correct records number: writed {}, must have {}".format(writed, rec_nb))
    logger.info("Script duration: %s" % (datetime.now() - start))


def get_values(cols, fh, count_dic, input_sep):
    """Reads row fields and return values and finished flag.

    :param cols: ordered dict containing column: width
    :param fh: input file handler
    :param count_dic: counters dict like {'read': 0, 'max': 10}
    :return: continue flag, columns values
    """
    values = []
    if count_dic['read'] >= count_dic['max']:
        return False, values
    for col, clen in cols.items():
        # if fh.tell() >= max_pos:  # tell is not correct after utf8 read
        #     break  # end of last record
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
            value = value.strip(u' ').replace(u'\r\n', u'\n')
        values.append(safe_encode(value))
        next_char = fh.read(1)
        if next_char and next_char not in (input_sep, u'\n'):
            break
    else:
        count_dic['read'] += 1
        return True, values
    return False, values


def get_cols(fh, input_sep):
    """Gets columns and lengths"""
    header = fh.readline()
    fh.seek(len(header))  # after readline, pointer is at the end of the file. We pos it correctly
    if not header:
        stop('File is empty !', logger)
    header = header.rstrip(u'\n')
    parts = header.split(input_sep)
    cols = OrderedDict()
    for part in parts:
        cols[part.strip()] = len(part)
    # check second row (does'nt yet contain \n)
    cont, values = get_values(cols, fh, {'read': 0, 'max': 1}, input_sep)
    for i, col in enumerate(cols):
        clen = cols[col]
        if values[i] != u'-' * clen:
            stop(u"Wrong length in second line for col '{}'".format(col), logger)
    return cols


def get_records_info(fh):
    """Get records number and last record position."""
    fh.seek(0, 2)  # at the end
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
    return int(match.group(1)), file_len + offset  # offset is neg


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert sqlcmd files to csv.')
    parser.add_argument('input_dir', help='Input directory.')
    parser.add_argument('-if', '--input_filter', dest='input_filter', help='Input filter.')
    parser.add_argument('-is', '--input_sep', dest='input_sep', help='Input delimiter. Default "|"', default='|')
    parser.add_argument('-od', '--output_dir', dest='output_dir', help='Output directory. Default: same as input')
    parser.add_argument('-oc', '--count_col', action='store_true', dest='count_col',
                        help='Add in output a counter column.')
    parser.add_argument('-on', '--only_new', dest='only_new', action='store_true',
                        help='Export only not existing csv files.')
    ns = parser.parse_args()
    if not ns.output_dir:
        ns.output_dir = ns.input_dir
    main(ns.input_dir, ns.output_dir, ns.count_col, ns.input_filter, ns.input_sep.decode(), ns.only_new)
