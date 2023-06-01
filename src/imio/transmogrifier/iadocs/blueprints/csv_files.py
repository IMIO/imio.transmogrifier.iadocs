# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from collective.transmogrifier.utils import Expression
from collective.transmogrifier.utils import openFileReference
from imio.helpers.transmogrifier import get_obj_from_path  # noqa
from imio.pyutils.system import full_path
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import course_store
from imio.transmogrifier.iadocs.utils import encode_list
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from imio.transmogrifier.iadocs.utils import print_item  # noqa
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
        * b_condition = O, blueprint condition expression (available: filename)
        * filename = M, relative filename considering csvpath.
        * fieldnames = M, fieldnames.
        * bp_key = M, blueprint key representing csv
        * csv_headers = O, csv header line bool. Default: True
        * csv_encoding = O, csv encoding. Default: utf8
        * dialect = O, csv dialect. Default: excel
        * fmtparam-strict = O, raises exception on row error. Default False.
        * none_value = O, value to replace by None.
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.csv_headers = Condition(options.get('csv_headers') or 'python:True', transmogrifier, name, options)
        self.dialect = safe_unicode(options.get('dialect') or 'excel')
        self.csv_encoding = safe_unicode(options.get('csv_encoding') or 'utf8')
        self.none_value = safe_unicode(options.get('none_value') or transmogrifier['config'].get('none_value'))
        self.roe = bool(int(options.get('raise_on_error') or '1'))
        self.fmtparam = dict(
            (key[len('fmtparam-'):],
             Expression(value, transmogrifier, name, options)(
                 options, key=key[len('fmtparam-'):])) for key, value
            in six.iteritems(options) if key.startswith('fmtparam-'))
        fieldnames = safe_unicode(options['fieldnames']).split()
        self.filename = safe_unicode(options['filename'])
        if not self.filename:
            return
        self.filename = full_path(self.storage['csvp'], self.filename)
        b_condition = Condition(options.get('b_condition') or 'python:True', transmogrifier, name, options)
        if not b_condition(None, filename=self.filename, storage=self.storage):
            self.filename = None
            return
        file_ = openFileReference(transmogrifier, self.filename)
        if file_ is None:
            raise Exception("Cannot open file '{}'".format(self.filename))
        self.bp_key = safe_unicode(options['bp_key'])
        self.storage['csv'][self.bp_key] = {'fp': self.filename, 'fh': file_, 'fn': os.path.basename(self.filename),
                                            'fd': fieldnames}

    def __iter__(self):
        for item in self.previous:
            yield item
        if not is_in_part(self, self.parts) or not self.filename:
            return
        csv_d = self.storage['csv'][self.bp_key]
        fieldnames = csv_d['fd']
        o_logger.info(u"Reading '{}'".format(csv_d['fp']))
        reader = csv.DictReader(csv_d['fh'], dialect=self.dialect, fieldnames=fieldnames, restkey='_rest',
                                restval='__NO_CO_LU_MN__', **self.fmtparam)
        for item in reader:
            item['_bpk'] = self.bp_key
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
            course_store(self)
            # removing useless keys as _A or _AB
            good_fieldnames = []
            for key in fieldnames:
                if re.match(r'_[A-Z]{1,2}$', key):
                    del item[key]
                else:
                    item[key] = safe_unicode(item[key].strip(' '), encoding=self.csv_encoding)
                    if self.none_value and item[key] == self.none_value:
                        item[key] = None
                    good_fieldnames.append(key)
            csv_d['fd'] = good_fieldnames

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
        * b_condition = O, blueprint condition expression (available: storage)
        * condition = O, item condition expression (available: item, storage)
        * filename = M, relative filename considering csvpath.
        * fieldnames = M, fieldnames.
        * headers = O, headers.
        * bp_key = M, blueprint key representing csv.
        * store_key = O, storing key for item. If defined, we get the item from storage[{bp_key}].
        * store_key_sort = 0, field to sort on. Otherwise, the store key.
        * csv_encoding = O, csv encoding. Default: utf8.
        * dialect = O, csv dialect. Default: excel.
        * fmtparam-strict = O, raises exception on row error. Default False.
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
        * yield = O, flag to know if a yield must be done (0 or 1: default 1)
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.bp_key = safe_unicode(options['bp_key'])
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        doit = Condition(options.get('b_condition') or 'python:True', transmogrifier, name, options)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.filename = safe_unicode(options['filename'])
        if not os.path.isabs(self.filename):
            self.filename = os.path.join(self.storage['csvp'], self.filename)
        self.doit = doit(None, filename=self.filename, storage=self.storage)
        if not self.doit:
            return
        fieldnames = safe_unicode(options['fieldnames']).split()
        headers = safe_unicode(options.get('headers') or '').split()
        csv_encoding = safe_unicode(options.get('csv_encoding') or 'utf8')
        fmtparam = dict(
            (key[len('fmtparam-'):],
             Expression(value, transmogrifier, name, options)(
                 options, key=key[len('fmtparam-'):])) for key, value
            in six.iteritems(options) if key.startswith('fmtparam-'))
        fmtparam['dialect'] = safe_unicode(options.get('dialect') or 'excel')
        self.yld = bool(int(options.get('yield') or '1'))
        self.store_key = safe_unicode(options.get('store_key'))
        self.store_subkey = safe_unicode(options.get('store_subkey'))
        self.sort_key = safe_unicode(options.get('store_key_sort') or '_no_special_sort_key_')
        self.storage['csv'][self.bp_key] = {'fp': self.filename, 'fh': None, 'fn': os.path.basename(self.filename),
                                            'wh': None, 'wp': fmtparam, 'we': csv_encoding,
                                            'fd': fieldnames, 'hd': headers}

    def _row(self, dicv, extend):
        extra_dic = dict(dicv)
        extra_dic.update(extend)
        writerow(self.storage['csv'][self.bp_key], extra_dic)

    def __iter__(self):
        csv_d = self.storage['csv'].get(self.bp_key)
        for item in self.previous:
            if is_in_part(self, self.parts) and self.doit and self.condition(item, storage=self.storage):
                course_store(self)
                if self.store_key:
                    if csv_d['fh'] is None:  # only doing one time
                        for (key, dv) in sorted(self.storage['data'][self.bp_key].items(),
                                                key=lambda tup: tup[1].get(self.sort_key, tup[0])):
                            if self.store_subkey:
                                for (subkey, sdv) in sorted(dv.items(),
                                                            key=lambda tup: tup[1].get(self.sort_key, tup[0])):
                                    self._row(sdv, {self.store_key: key, self.store_subkey: subkey})
                            else:
                                self._row(dv, {self.store_key: key})
                else:
                    writerow(csv_d, item)
                if not self.yld:
                    continue
            yield item
        if csv_d is not None and csv_d.get('fh') is not None:
            csv_d['fh'].close()
            csv_d['fh'] = None
