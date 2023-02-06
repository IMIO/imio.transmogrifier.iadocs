# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from collective.transmogrifier.utils import Expression
from collective.transmogrifier.utils import openFileReference
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import encode_list
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from Products.CMFPlone.utils import safe_unicode
from zope.annotation import IAnnotations
from zope.interface import classProvides
from zope.interface import implements

import csv
import os
import re
import six


class CSVReader(object):
    """Reads a csv file.

    Parameters:
        * filename = M, relative filename considering csvpath.
        * fieldnames = O, fieldnames.
        * ext_type = O, external type string representing csv
        * store_key = O, storing key for item. If defined, the item is not yielded but stored in storage[{ext_type}]
        * csv_headers = O, csv header line bool. Default: True
        * csv_encoding = O, csv encoding. Default: utf8
        * dialect = O, csv dialect. Default: excel
        * fmtparam-strict = O, raises exception on row error. Default False.
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        if not is_in_part(self, 'a'):
            return
        self.csv_headers = Condition(options.get('csv_headers', 'python:True'), transmogrifier, name, options)
        self.dialect = safe_unicode(options.get('dialect', 'excel'))
        self.csv_encoding = safe_unicode(options.get('csv_encoding', 'utf8'))
        self.roe = bool(int(options.get('raise_on_error', '1')))
        self.fmtparam = dict(
            (key[len('fmtparam-'):],
             Expression(value, transmogrifier, name, options)(
                 options, key=key[len('fmtparam-'):])) for key, value
            in six.iteritems(options) if key.startswith('fmtparam-'))
        fieldnames = safe_unicode(options.get('fieldnames', '')).split()
        self.filename = safe_unicode(options.get('filename', ''))
        if not self.filename:
            return
        if not os.path.isabs(self.filename):
            self.filename = os.path.join(self.storage['csvp'], self.filename)
        file_ = openFileReference(transmogrifier, self.filename)
        if file_ is None:
            raise Exception("Cannot open file '{}'".format(self.filename))
        store_key = safe_unicode(options.get('store_key'))
        self.ext_type = safe_unicode(options.get('ext_type', os.path.basename(self.filename)))
        if store_key:
            self.storage['data'][self.ext_type] = {}
        self.storage['csv'][self.ext_type] = {'fp': self.filename, 'fh': file_, 'fn': os.path.basename(self.filename),
                                              'fd': fieldnames, 'sk': store_key}

    def __iter__(self):
        for item in self.previous:
            yield item
        if not is_in_part(self, 'a') or not self.filename:
            return
        csv_d = self.storage['csv'][self.ext_type]
        fieldnames = csv_d['fd']
        store_key = csv_d['sk']
        o_logger.info(u"Reading '{}'".format(csv_d['fp']))
        reader = csv.DictReader(csv_d['fh'], dialect=self.dialect, fieldnames=fieldnames, restkey='_rest',
                                restval='__NO_CO_LU_MN__', **self.fmtparam)
        for item in reader:
            item['_etyp'] = self.ext_type
            item['_ln'] = reader.line_num
            # check fieldnames length on first line
            if reader.line_num == 1:
                reader.restval = u''
                if '_rest' in item:
                    log_error(item, u'STOPPING: some columns are not defined in fieldnames: {}'.format(item['_rest']),
                              level='critical')
                    if self.roe:
                        raise Exception(u'Some columns for {} are not defined in fieldnames: {}'.format(
                            csv_d['fn'], item['_rest']))
                    break
                extra_cols = [key for (key, val) in item.items() if val == '__NO_CO_LU_MN__']
                if extra_cols:
                    log_error(item, u'STOPPING: to much columns defined in fieldnames: {}'.format(extra_cols),
                              level='critical')
                    if self.roe:
                        raise Exception(u'To much columns for {} defined in fieldnames: {}'.format(
                            csv_d['fn'], extra_cols))
                    break
                # pass headers if any
                if self.csv_headers(None):
                    continue
            # removing useless keys as _A or _AB
            good_fieldnames = []
            for key in fieldnames:
                if re.match(r'_[A-Z]{1,2}$', key):
                    del item[key]
                else:
                    item[key] = safe_unicode(item[key].strip(' '), encoding=self.csv_encoding)
                    good_fieldnames.append(key)
            csv_d['fd'] = good_fieldnames

            if store_key:
                self.storage['data'][item.pop('_etyp')][item.pop(store_key)] = item
            else:
                yield item
        if csv_d['fh'] is not None:
            csv_d['fh'].close()
            csv_d['fh'] = None


def writerow(csv_d, item):
    """Write item in csv"""
    if csv_d['fh'] is None:
        try:
            csv_d['fh'] = open(csv_d['fp'], mode='wb')
        except IOError as m:
            raise Exception("Cannot create file '{}': {}".format(csv_d['fp'], m))
        o_logger.info(u"Writing '{}'".format(csv_d['fp']))
        csv_d['wh'] = csv.writer(csv_d['fh'], **csv_d['wp'])
        if csv_d['hd']:
            csv_d['wh'].writerow(encode_list(csv_d['hd'], csv_d['we']))
    csv_d['wh'].writerow(encode_list([item[fd] for fd in csv_d['fd']], csv_d['we']))


class CSVWriter(object):
    """Writes a csv file.

    Parameters:
        * filename = M, relative filename considering csvpath.
        * fieldnames = M, fieldnames.
        * headers = O, headers.
        * ext_type = O, type string representing csv.
        * store_key = O, storing key for item. If defined, the item is not yielded but stored in storage[{ext_type}].
        * store_key_sort = 0, field to sort on. Otherwise, the store key.
        * csv_encoding = O, csv encoding. Default: utf8.
        * dialect = O, csv dialect. Default: excel.
        * fmtparam-strict = O, raises exception on row error. Default False.
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        if not is_in_part(self, 'a'):
            return
        self.doit = Condition(options.get('condition', 'python:True'), transmogrifier, name, options)
        self.filename = safe_unicode(options.get('filename', ''))
        if not os.path.isabs(self.filename):
            self.filename = os.path.join(self.storage['csvp'], self.filename)
        fieldnames = safe_unicode(options.get('fieldnames', '')).split()
        headers = safe_unicode(options.get('headers', '')).split()
        csv_encoding = safe_unicode(options.get('csv_encoding', 'utf8'))
        fmtparam = dict(
            (key[len('fmtparam-'):],
             Expression(value, transmogrifier, name, options)(
                 options, key=key[len('fmtparam-'):])) for key, value
            in six.iteritems(options) if key.startswith('fmtparam-'))
        fmtparam['dialect'] = safe_unicode(options.get('dialect', 'excel'))
        self.store_key = safe_unicode(options.get('store_key'))
        self.ext_type = safe_unicode(options.get('ext_type'))
        self.sort_key = safe_unicode(options.get('store_key_sort') or '_no_special_sort_key_')
        self.storage['csv'][self.ext_type] = {'fp': self.filename, 'fh': None, 'fn': os.path.basename(self.filename),
                                              'wh': None, 'wp': fmtparam, 'we': csv_encoding,
                                              'fd': fieldnames, 'hd': headers}

    def __iter__(self):
        csv_d = self.storage['csv'].get(self.ext_type)
        for item in self.previous:
            if self.doit(item, storage=self.storage):
                if self.store_key:
                    if csv_d['fh'] is None:  # only doing one time
                        for (key, dv) in sorted(self.storage['data'][self.ext_type].items(),
                                                key=lambda tup: tup[1].get(self.sort_key, tup[0])):
                            extra_dic = dict(dv)
                            extra_dic.update({self.store_key: key})
                            writerow(self.storage['csv'][self.ext_type], extra_dic)
                else:
                    writerow(csv_d, item)
            yield item
        if csv_d.get('fh') is not None:
            csv_d['fh'].close()
            csv_d['fh'] = None
