# -*- coding: utf-8 -*-
"""Setup tests for this package."""
from imio.transmogrifier.iadocs.utils import clean_value
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa

import unittest


class TestSetup(unittest.TestCase):
    """Test that imio.transmogrifier.iadocs is properly installed."""

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        """Custom shared utility setup for tests."""
        self.portal = self.layer['portal']

    def test_clean_value(self):
        self.assertEqual(clean_value(u'  strip  '), u'strip')
        self.assertEqual(clean_value(u' | strip  ', strip=u' |'), u'strip')
        self.assertEqual(clean_value(u' strip  \n  strip  '), u'strip\nstrip')
        self.assertEqual(clean_value(u' strip  |  strip  ', sep=u'|'), u'strip|strip')
        self.assertEqual(clean_value(u'  \n strip  \n '), u'strip')
        self.assertEqual(clean_value(u' strip  \n "', strip=u' "'), u'strip')
        self.assertEqual(clean_value(u' strip  \n "', patterns=[r'"']), u'strip')
        self.assertEqual(clean_value(u' strip  \n "\'"', patterns=[r'^["\']+$']), u'strip')
