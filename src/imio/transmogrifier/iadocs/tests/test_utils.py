# -*- coding: utf-8 -*-
"""Setup tests for this package."""
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import is_in_part

import unittest


class TestUtils(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.storage = get_storage(self.portal)

    def test_get_related_parts(self):
        self.assertEqual(get_related_parts(""), None)
        self.assertEqual(get_related_parts("a_xx"), None)
        self.assertEqual(get_related_parts("a__xx"), "a")
        self.assertEqual(get_related_parts("abc__xx"), "abc")

    def test_is_in_part(self):
        self.storage.update({"parts": "a"})
        self.assertFalse(is_in_part(self, None))
        self.assertFalse(is_in_part(self, "b"))
        self.assertTrue(is_in_part(self, "ab"))
