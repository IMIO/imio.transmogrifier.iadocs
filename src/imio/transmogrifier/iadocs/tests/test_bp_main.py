# -*- coding: utf-8 -*-
"""Setup tests for this package."""
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
        # check
        # bp = CommonInputChecks(self.portal, 'a_cip', {'bp_key': 'cip'}, None)
        # bp.previous = [{u'1': u'xx', u'2': u'yy'}]
        # self.assertDictEqual(next(iter(bp)), bp.previous[0])

