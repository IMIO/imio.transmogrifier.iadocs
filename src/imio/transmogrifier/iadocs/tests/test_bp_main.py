# -*- coding: utf-8 -*-
"""Blueprints tests for this package."""
from datetime import date
from datetime import datetime
from imio.transmogrifier.iadocs.blueprints.main import CommonInputChecks
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa

import unittest


class TestBluePrintMain(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer['portal']
        self.storage = get_storage(self.portal)

    def test_common_input_checks(self):
        self.storage.update({'parts': 'a', 'csv': {'cip': {'fd': [u'1', u'2']}}})
        # without options
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip'}, None)
        bp.previous = [{u'1': u'xx', u'2': u'yy'}]
        self.assertDictEqual(next(iter(bp)), bp.previous[0])
        # check strip_chars
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'strip_chars': '1 $.'}, None)
        bp.previous = [{u'1': u'aa.$', u'2': u'bb'}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa', u'2': u'bb'})
        # check clean_value
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'clean_value':
                               '1 python:"\\n" " " "python:[r\'^["" ]+$\']" python:"\\r\\n"'}, None)
        self.assertListEqual(bp.cleans, [(u'1', u'\n', u' ', ['^[" ]+$'], u'\r\n')])
        bp.previous = [{u'1': u'aa\n" "\nbb', u'2': None}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa\r\nbb', u'2': None})
        # check hyphen_newline
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'hyphen_newline': '1 2'}, None)
        bp.previous = [{u'1': u'aa\n', u'2': u'aa\nbb'}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa', u'2': u'aa - bb'})
        # check invalids
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'invalids': '1 0 2 ",|0, 0|0, 23"'}, None)
        bp.previous = [{u'1': u'aa', u'2': u'0, 23'}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa', u'2': None})
        bp.previous = [{u'1': u'0', u'2': u''}]
        self.assertDictEqual(next(iter(bp)), {u'1': None, u'2': u''})
        # check split_text
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'split_text':
                               '1 11 2 0 python:\'\\n\' python:\'\\r\\n\' "Suite: "'}, None)
        self.assertListEqual(bp.splits, [(u'1', 11, u'2', 0, u'\n', u'\r\n', u'Suite: ')])
        bp.previous = [{u'1': u'aa bb cc dd ee', u'2': None}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa bb cc dd', u'2': u'Suite:  ee'})
        bp.previous = [{u'1': u'aa bb cc \ndd', u'2': u'Première\r\nDeuxième'}]
        bp.splits = [(u'1', 6, u'2', 50, u'\n', u'\r\n', u'Suite: ')]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa bb ', u'2': u'Première\r\nDeuxième\r\nSuite: cc \r\ndd'})
        # check booleans
        self.storage['csv']['cip']['fd'] = [u'1', u'2', u'3', u'4', u'5', u'6', u'7']
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'booleans': '1 2 3 4 5 6'}, None)
        bp.previous = [{u'_bpk': 'cip', u'_eid': u'0', u'1': u'', u'2': u'true', u'3': u'False', u'4': u'1', u'5': '0',
                        u'6': u'aa', u'7': u'aa'}]
        self.assertDictEqual(next(iter(bp)), {u'_bpk': 'cip', u'_eid': u'0', u'1': False, u'2': True, u'3': False,
                                              u'4': True, u'5': False, u'6': False, u'7': u'aa', u'_error': True})
        # check dates
        self.storage['csv']['cip']['fd'] = [u'1', u'2', u'3']
        bp = CommonInputChecks(self.portal, 'a__cip',
                               {'bp_key': 'cip', 'dates': '1 "%Y%m%d %H:%M" 0 2 %Y%m%d 1 3 "%Y%m%d" 1'}, None)
        bp.previous = [{u'_bpk': 'cip', u'_eid': u'0', u'1': u'19840212 12:01', u'2': u'20010212', u'3': u'2023 04 16'}]
        self.assertDictEqual(next(iter(bp)), {u'_bpk': 'cip', u'_eid': u'0', u'1': datetime(1984, 2, 12, 12, 1),
                                              u'2': date(2001, 2, 12), u'3': None, u'_error': True})
        # check combination
        self.storage['csv']['cip']['fd'] = [u'1', u'2', u'3']
        bp = CommonInputChecks(self.portal, 'a__cip', {'bp_key': 'cip', 'strip_chars': '1 !', 'hyphen_newline': '1',
                                                       'invalids': '1 néant', 'booleans': '1'}, None)
        bp.previous = [{u'1': u'!néant\n'}]
        self.assertDictEqual(next(iter(bp)), {u'1': False})
