# -*- coding: utf-8 -*-
"""Blueprints tests for this package."""
from imio.transmogrifier.iadocs.blueprints.handlers import get_contact_info
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

    def test_get_contact_info_desc_only(self):
        """When dest2 is dest1 (no data_transfer field), lines must not be duplicated."""
        section = self
        self.storage.setdefault("data", {})["e_contact"] = {
            u"1": {u"lastname": u"Dupont", u"firstname": u"Jean", u"_street": u"Rue Haute", u"_city": u"Colfontaine"}
        }
        item = {u"_cid": u"1", u"_free": u"Contact libre\nsuite"}
        # two destinations: name in both, address only in the secondary one
        desc, d_t = [], []
        self.assertTrue(get_contact_info(section, item, u"EXPÉDITEUR", u"_cid", u"_free", desc, d_t))
        self.assertEqual(desc, [u"EXPÉDITEUR: Dupont Jean.", u"EXPÉDITEUR LIBRE: Contact libre"])
        self.assertEqual(
            d_t,
            [
                u"EXPÉDITEUR: Dupont Jean.",
                u"ADRESSE EXPÉDITEUR: Rue Haute Colfontaine.",
                u"EXPÉDITEUR LIBRE: Contact libre, suite",
            ],
        )
        # one single destination: no duplicated line, full free text kept
        desc = []
        self.assertTrue(get_contact_info(section, item, u"EXPÉDITEUR", u"_cid", u"_free", desc, desc))
        self.assertEqual(
            desc,
            [
                u"EXPÉDITEUR: Dupont Jean.",
                u"ADRESSE EXPÉDITEUR: Rue Haute Colfontaine.",
                u"EXPÉDITEUR LIBRE: Contact libre, suite",
            ],
        )
