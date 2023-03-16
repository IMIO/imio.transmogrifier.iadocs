# -*- coding: utf-8 -*-
"""Setup tests for this package."""
from datetime import date
from datetime import datetime
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs.blueprints.main import CommonInputChecks
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from zope.annotation import IAnnotations

import unittest


class TestBluePrintMain(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer['portal']
        annot = IAnnotations(self.portal)
        self.storage = annot.setdefault(ANNOTATION_KEY, {})

    def test_common_input_checks(self):
        self.storage.update({'parts': 'a', 'csv': {'cip': {'fd': [u'1', u'2']}}})
        # without options
        bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip'}, None)
        bp.previous = [{u'1': u'xx', u'2': u'yy'}]
        self.assertDictEqual(next(iter(bp)), bp.previous[0])
        # check strip_chars
        bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip', 'strip_chars': '1 $.'}, None)
        bp.previous = [{u'1': u'aa.$', u'2': u'bb'}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa', u'2': u'bb'})
        # check hyphen_newline
        bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip', 'hyphen_newline': '1 2'}, None)
        bp.previous = [{u'1': u'aa\n', u'2': u'aa\nbb'}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa', u'2': u'aa - bb'})
        # check invalids
        bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip', 'invalids': '1 0 2 ",|0, 0|0, 23"'}, None)
        bp.previous = [{u'1': u'aa', u'2': u'0, 23'}]
        self.assertDictEqual(next(iter(bp)), {u'1': u'aa', u'2': None})
        bp.previous = [{u'1': u'0', u'2': u''}]
        self.assertDictEqual(next(iter(bp)), {u'1': None, u'2': u''})
        # check booleans
        self.storage['csv']['cip']['fd'] = [u'1', u'2', u'3', u'4', u'5', u'6', u'7']
        bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip', 'booleans': '1 2 3 4 5 6'}, None)
        bp.previous = [{u'_bpk': 'cip', u'_eid': u'0', u'1': u'', u'2': u'true', u'3': u'False', u'4': u'1', u'5': '0',
                        u'6': u'aa', u'7': u'aa'}]
        self.assertDictEqual(next(iter(bp)), {u'_bpk': 'cip', u'_eid': u'0', u'1': False, u'2': True, u'3': False,
                                              u'4': True, u'5': False, u'6': False, u'7': u'aa', u'_error': True})
        # check dates
        self.storage['csv']['cip']['fd'] = [u'1', u'2', u'3']
        bp = CommonInputChecks(self.portal, 'a_cip',
                               {'bp_key': 'cip', 'dates': '1 %Y%m%d 0 2 %Y%m%d 1 3 %Y%m%d 1'}, None)
        bp.previous = [{u'_bpk': 'cip', u'_eid': u'0', u'1': u'19840212', u'2': u'20010212', u'3': u'2023 04 16'}]
        self.assertDictEqual(next(iter(bp)), {u'_bpk': 'cip', u'_eid': u'0', u'1': datetime(1984, 2, 12, 0, 0),
                                              u'2': date(2001, 2, 12), u'3': None, u'_error': True})
        # check combination
        self.storage['csv']['cip']['fd'] = [u'1', u'2', u'3']
        bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip', 'strip_chars': '1 !', 'hyphen_newline': '1',
                                                      'invalids': '1 néant', 'booleans': '1'}, None)
        bp.previous = [{u'1': u'!néant\n'}]
        self.assertDictEqual(next(iter(bp)), {u'1': False})