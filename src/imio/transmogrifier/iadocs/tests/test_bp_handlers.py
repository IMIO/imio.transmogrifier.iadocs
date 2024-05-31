# -*- coding: utf-8 -*-
"""Blueprints tests for this package."""
from imio.transmogrifier.iadocs.blueprints.various import EnhancedCondition
from imio.transmogrifier.iadocs.blueprints.various import EnhancedInserter
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa

import unittest


class TestBluePrintHandlers(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.portal.context = self.portal
        self.storage = get_storage(self.portal)

    def test_L1SenderAsTextSet(self):
        eid_key = "sid"
        item = {"sid": 12}
        eids = {}
        expr = "(item[eid_key] and item[eid_key] not in eids) and eid_key or ''"
        # sid value but not in eids
        self.assertEqual(eval(expr), eid_key)
        # sid value and in eids
        eids[12] = ""
        self.assertEqual(eval(expr), "")
        # not sid value
        item["sid"] = None
        self.assertEqual(eval(expr), "")
        del eids[12]
        self.assertEqual(eval(expr), "")
