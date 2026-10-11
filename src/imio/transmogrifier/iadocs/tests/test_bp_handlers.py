# -*- coding: utf-8 -*-
"""Blueprints tests for this package."""
from Acquisition import aq_parent
from collections import OrderedDict
from collective.contact.plonegroup.config import get_registry_organizations
from collective.contact.plonegroup.config import set_registry_organizations
from collective.iconifiedcategory.utils import calculate_category_id
from datetime import date
from datetime import datetime
from ftw.labels.interfaces import ILabeling
from imio.helpers.transmogrifier import Condition
from imio.helpers.transmogrifier import relative_path
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.blueprints.handlers import AServiceUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import BMailtypesByType
from imio.transmogrifier.iadocs.blueprints.handlers import BMailtypeUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import ContactAsTextUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import ContactSet
from imio.transmogrifier.iadocs.blueprints.handlers import DefaultContactSet
from imio.transmogrifier.iadocs.blueprints.handlers import DOMSenderCreation
from imio.transmogrifier.iadocs.blueprints.handlers import DPersonnelCreation
from imio.transmogrifier.iadocs.blueprints.handlers import ECategoryUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import F1Level3Handler
from imio.transmogrifier.iadocs.blueprints.handlers import F2FolderSubfolderSplit
from imio.transmogrifier.iadocs.blueprints.handlers import F3TextCategoriesToId
from imio.transmogrifier.iadocs.blueprints.handlers import get_contact_info
from imio.transmogrifier.iadocs.blueprints.handlers import get_contact_name
from imio.transmogrifier.iadocs.blueprints.handlers import get_contact_phone
from imio.transmogrifier.iadocs.blueprints.handlers import HContactTypeUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import hp_dic
from imio.transmogrifier.iadocs.blueprints.handlers import I1ContactUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import I2ContactUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import J2ContactUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import L1RecipientGroupsSet
from imio.transmogrifier.iadocs.blueprints.handlers import L1SenderAsTextSet
from imio.transmogrifier.iadocs.blueprints.handlers import M1AssignedUserHandling
from imio.transmogrifier.iadocs.blueprints.handlers import MavalHandling
from imio.transmogrifier.iadocs.blueprints.handlers import ParentPathInsert
from imio.transmogrifier.iadocs.blueprints.handlers import person_dic
from imio.transmogrifier.iadocs.blueprints.handlers import POMSenderSet
from imio.transmogrifier.iadocs.blueprints.handlers import PostActions
from imio.transmogrifier.iadocs.blueprints.handlers import Q1RecipientsAsTextUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import R1RecipientGroupsUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import ReadLabelForRecipientGroup
from imio.transmogrifier.iadocs.blueprints.handlers import RsyncFileWrite
from imio.transmogrifier.iadocs.blueprints.handlers import S1ClassificationFoldersUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import S3ClassificationFoldersUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import T1DmsfileCreation
from imio.transmogrifier.iadocs.blueprints.handlers import WorkflowHistoryUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import X1ReplyToUpdate
from imio.transmogrifier.iadocs.blueprints.handlers import XmlContactStore
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import get_transmogrifier
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from imio.transmogrifier.iadocs.utils import get_categories
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_personnel
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import get_users
from imio.transmogrifier.iadocs.utils import get_users_groups
from imio.transmogrifier.iadocs.utils import MAILTYPES
from persistent.list import PersistentList
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from plone.dexterity.fti import DexterityFTIModificationDescription
from plone.dexterity.fti import ftiModified
from Products.CMFPlone.utils import safe_unicode
from z3c.relationfield import RelationValue
from zc.relation.interfaces import ICatalog
from zope.component import getUtility
from zope.event import notify
from zope.intid import IIntIds
from zope.lifecycleevent import ObjectModifiedEvent

import io
import logging
import os
import shutil
import six
import six.moves.cPickle as pickle
import tempfile
import transaction
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

    def test_get_contact_name(self):
        # person, without parent
        self.assertEqual(
            get_contact_name(
                {
                    u"lastname": u"Dupont",
                    u"firstname": u"Jean",
                    u"_email1": None,
                    u"_email2": u"j@d.be",
                    u"_function": u"Directeur",
                    u"enterprise_number": u"0123",
                },
                {},
            ),
            ([u"Dupont Jean", u"j@d.be", u"Directeur", u"NE:0123"], []),
        )
        # same values as parent are not repeated
        org = {u"lastname": u"Commune", u"_email1": u"info@c.be", u"enterprise_number": u"0999"}
        self.assertEqual(get_contact_name(dict(org), org), ([], [u"Commune", u"info@c.be", u"NE:0999"]))
        self.assertEqual(
            get_contact_name({u"lastname": u"Service", u"_email3": u"s@c.be", u"enterprise_number": u"0998"}, org),
            ([u"Service", u"s@c.be", u"NE:0998"], [u"Commune", u"info@c.be", u"NE:0999"]),
        )
        # customer 2: organization levels
        parent = {u"_organization": u"Commune", u"_division": u"Travaux", u"_service": u"Voiries", u"_level3": u""}
        dic = {u"_organization": u"", u"_division": u"", u"lastname": u"Martin", u"firstname": u"Paul"}
        self.assertEqual(
            get_contact_name(dic, parent), ([u"Martin, Paul"], [u"Commune, Division: Travaux, Service: Voiries"])
        )
        # second name used when no name
        self.assertEqual(
            get_contact_name({u"_name2": u"Service X"}, {u"_name2": u"Parent"}), ([u"Service X"], [u"Parent"])
        )
        self.assertEqual(get_contact_name({}, {}), ([], []))

    def test_get_contact_phone(self):
        self.assertListEqual(
            get_contact_phone({u"_phone2": u"1", u"_cell3": u"2"}, {u"_phone1": u"3", u"_cell1": u"4"}), [u"1", u"2"]
        )
        self.assertListEqual(get_contact_phone({u"_phone1": None}, {u"_phone1": u"3", u"_cell1": u"4"}), [u"3", u"4"])
        self.assertListEqual(get_contact_phone({}, {}), [])

    def test_get_contact_info(self):
        section = self
        self.storage.setdefault("data", {})["e_contact"] = {
            u"1": {
                u"lastname": u"Dupont",
                u"firstname": u"Jean",
                u"_parent_id": u"2",
                u"_function": u"Gérant",
                u"_company": u"Dupont SPRL",
                u"_other": u"Tél. après 17h",
            },
            u"2": {
                u"lastname": u"Électricité SA",
                u"_street": u"Rue Basse",
                u"_pc": u"7000",
                u"_city": u"Mons",
                u"_phone1": u"065/1",
                u"_cell1": u"0475/1",
            },
            u"3": {u"lastname": u"Dupont SPRL", u"_company": u"Dupont SPRL"},
        }
        # parent information, company, parent address and phones, complement
        desc, d_t = [], []
        item = {u"_cid": u"1", u"_free": None}
        self.assertTrue(get_contact_info(section, item, u"EXPÉDITEUR", u"_cid", u"_free", desc, d_t))
        self.assertListEqual(desc, [u"EXPÉDITEUR PARENT: Électricité SA.", u"EXPÉDITEUR: Dupont Jean, Gérant."])
        self.assertListEqual(
            d_t,
            [
                u"EXPÉDITEUR PARENT: Électricité SA.",
                u"EXPÉDITEUR: Dupont Jean, Gérant.",
                u"ORGANISATION: Dupont SPRL.",
                u"ADRESSE EXPÉDITEUR PARENT: Rue Basse 7000 Mons.",
                u"TÉL EXPÉDITEUR: 065/1, 0475/1.",
                u"COMPLÉMENT EXPÉDITEUR: Tél. après 17h",
            ],
        )
        self.assertTrue(all(isinstance(line, six.text_type) for line in desc + d_t))
        # empty related label, company equal to the name
        desc, d_t = [], []
        self.assertTrue(
            get_contact_info(section, {u"_cid": u"3"}, u"DESTINATAIRE", u"_cid", u"", desc, d_t, related_label=u"")
        )
        self.assertListEqual(desc, [u"DESTINATAIRE: Dupont SPRL."])
        self.assertListEqual(d_t, [u"DESTINATAIRE: Dupont SPRL."])
        # nothing: no contact id field, only quotes in free text
        desc, d_t = [], []
        self.assertFalse(
            get_contact_info(section, {u"_cid": u"1", u"_free": u'""\n'}, u"EXPÉDITEUR", u"", u"_free", desc, d_t)
        )
        self.assertFalse(get_contact_info(section, {u"_cid": None}, u"EXPÉDITEUR", u"_cid", u"", desc, d_t))
        self.assertListEqual(desc + d_t, [])


# Behaviors added by the Initialization section (config internal_number_behavior_types and
# data_transfer_behavior_types of data-transfer.cfg.1)
INB_TYPES = ("organization", "person", "held_position", "dmsincomingmail", "dmsincoming_email", "dmsoutgoingmail")
DTB_TYPES = ("dmsincomingmail", "dmsincoming_email", "dmsoutgoingmail")
INB = "collective.behavior.internalnumber.behavior.IInternalNumberBehavior"
DTB = "imio.dms.mail.content.behaviors.IDmsMailDataTransfer"
PLONEGROUP = "/contacts/plonegroup-organization"


class LogRecorder(logging.Handler):
    """Records (logger name, level, message) of the dti (e_logger) and dto (o_logger) loggers."""

    def __init__(self):
        logging.Handler.__init__(self)
        self.records = []

    def emit(self, record):
        self.records.append((record.name, record.levelname, record.getMessage()))


class DmsTestCase(unittest.TestCase):
    """Base class: ia.docs site configured as the Initialization section does it."""

    layer = IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.portal.portal_setup.runAllImportStepsFromProfile(
            "profile-collective.behavior.internalnumber:default", dependency_strategy="new"
        )
        for types, behavior in ((INB_TYPES, INB), (DTB_TYPES, DTB)):
            for typ in types:
                fti = getattr(self.portal.portal_types, typ)
                if behavior not in fti.behaviors:
                    old_bav = tuple(fti.behaviors)
                    fti.behaviors = old_bav + (behavior,)
                    description = DexterityFTIModificationDescription("behaviors", old_bav)
                    ftiModified(fti, ObjectModifiedEvent(fti, description))
        self.intids = getUtility(IIntIds)
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.logs = LogRecorder()
        for logger in (e_logger, o_logger):
            self.addCleanup(logger.setLevel, logger.level)
            self.addCleanup(logger.removeHandler, self.logs)
            logger.setLevel(logging.DEBUG)
            logger.addHandler(self.logs)

    def make_storage(self, parts, **data):
        """Storage as built by the Initialization section, completed with the given data dicts."""
        storage = {
            "course": OrderedDict(),
            "csv": {},
            "parts": parts,
            "commit": 0,
            "commit_nb": 0,
            "batch_nb": 0,
            "creation_date": datetime(2009, 9, 22, 12, 0),
            "desc_only": False,
            "lastsection": {"pkl_dump": []},
            "wp": self.tmp,
            "csvp": self.tmp,
            "filesp": self.tmp,
            "plone": {
                "firstname_first": True,
                "directory": self.portal["contacts"],
                "directory_path": "/contacts",
                "def_contact": self.portal["contacts"]["notencoded"],
            },
            "data": {},
        }
        p_data = storage["data"]
        p_data["p_orgs_all"], p_data["p_eid_to_orgs"] = get_plonegroup_orgs(self.portal)
        p_data["p_mailtype"] = get_mailtypes(self.portal)
        p_data["p_user"] = get_users(self.portal)
        p_data["p_userid_to_pers"], p_data["p_euid_to_pers"], p_data["p_hps"] = get_personnel(self.portal)
        p_data["p_category"] = get_categories(self.portal)
        p_data["p_user_service"] = get_users_groups(self.portal, p_data["p_user"])
        p_data.update(data)
        return storage

    def section(self, klass, name, options, items=(), storage=None, config=None, sections=None):
        """Instantiate a blueprint section as a pipeline does it.

        :param sections: other pipeline sections options (as read by `transmogrifier.get(section_name)`)
        """
        transmogrifier = get_transmogrifier(self.portal, config=config, storage=storage)
        transmogrifier._raw.update(sections or {})
        return klass(transmogrifier, name, options, items)

    def path(self, obj):
        """Path relative to the portal, as stored in the pipeline data dicts."""
        return relative_path(self.portal, "/".join(obj.getPhysicalPath()))

    def obj(self, path):
        return self.portal.unrestrictedTraverse(path.strip("/"))

    def mail(self, mail_id):
        """Example mail of the imio.dms.mail fixture (in a period folder depending on the run date)."""
        return self.portal.portal_catalog.unrestrictedSearchResults(getId=mail_id)[0]._unrestrictedGetObject()

    def uid(self, path):
        return self.obj(path).UID()

    def imported(self, *mails):
        """Classification fields set as an imported mail has them (the behavior adapter stores them on the mail
        without schema default: the example mails of the fixture don't have them)."""
        for mail in mails:
            for attr in ("classification_categories", "classification_folders"):
                if not hasattr(mail, attr):
                    setattr(mail, attr, None)

    def iid(self, path):
        return self.intids.getId(self.obj(path))


class TestAServiceUpdate(DmsTestCase):

    options = {"condition": "python: item.get('_bpk') == u'e_service'"}

    def test___init__(self):
        storage = self.make_storage("b", e_service_match={})
        bp = self.section(AServiceUpdate, "a__service_update", self.options, storage=storage)
        self.assertEqual(bp.parts, "a")
        self.assertIs(bp.storage, storage)
        self.assertFalse(hasattr(bp, "condition"))  # part not run: options not parsed
        storage = self.make_storage("a", e_service_match={u"10": {u"uid": u"x"}})
        bp = self.section(AServiceUpdate, "a__service_update", self.options, storage=storage)
        self.assertTrue(bp.condition({"_bpk": u"e_service"}))
        self.assertFalse(bp.condition({"_bpk": u"e_user"}))
        self.assertIs(bp.all_orgs, storage["data"]["p_orgs_all"])
        self.assertIs(bp.eid_to_orgs, storage["data"]["p_eid_to_orgs"])
        self.assertIs(bp.match, storage["data"]["e_service_match"])

    def test___iter__(self):
        secr_path = PLONEGROUP + "/direction-generale/secretariat"
        secr = self.obj(secr_path)
        grh = self.obj(PLONEGROUP + "/direction-generale/grh")
        grh.internal_number = u"5"  # already linked in a previous run
        # part not run: items are yielded unchanged
        bp = self.section(AServiceUpdate, "a__service_update", self.options, [{u"_eid": u"10"}], self.make_storage("b"))
        self.assertListEqual(list(bp), [{u"_eid": u"10"}])
        # empty match: all services are passed
        storage = self.make_storage("a", e_service_match={})
        items = [{u"_bpk": u"e_service", u"_eid": u"10"}, {u"_bpk": u"e_user", u"_eid": u"10"}]
        bp = self.section(AServiceUpdate, "a__service_update", self.options, items, storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_user", u"_eid": u"10"}])
        # with matching file
        match = {
            u"10": {u"etitle": u"Secrétariat communal", u"uid": secr.UID(), u"title": u"Secrétariat"},
            u"11": {u"etitle": u"Empty", u"uid": u"", u"title": u""},
            u"12": {u"etitle": u"Unknown", u"uid": u"abcd", u"title": u""},
        }
        storage = self.make_storage("a", e_service_match=match)
        self.assertDictEqual(storage["data"]["p_eid_to_orgs"], {u"5": {"uid": grh.UID()}})
        items = [
            {u"_bpk": u"e_service", u"_eid": u"5", u"_title": u"GRH"},
            {u"_bpk": u"e_service", u"_eid": u"9", u"_title": u"Not matched"},
            {u"_bpk": u"e_service", u"_eid": u"11", u"_title": u"Empty"},
            {u"_bpk": u"e_service", u"_eid": u"12", u"_title": u"Unknown"},
            {u"_bpk": u"e_service", u"_eid": u"10", u"_title": u"Secrétariat communal"},
            {u"_bpk": u"e_service", u"_eid": u"10", u"_title": u"Duplicated"},
            {u"_bpk": u"e_user", u"_eid": u"10"},
        ]
        bp = self.section(AServiceUpdate, "a__service_update", self.options, items, storage)
        gen = iter(bp)
        updated = next(gen)
        self.assertDictEqual(
            updated,
            {
                u"_bpk": u"e_service",
                u"_eid": u"10",
                u"_title": u"Secrétariat communal",
                u"_type": "organization",
                u"_path": secr_path,
                u"internal_number": u"10",
            },
        )
        self.assertIsInstance(updated[u"internal_number"], six.text_type)
        secr.internal_number = updated[u"internal_number"]  # stored by the next sections (schemaupdater)
        self.assertListEqual(
            list(gen),
            [{u"_bpk": u"e_service", u"_eid": u"10", u"_title": u"Duplicated"}, {u"_bpk": u"e_user", u"_eid": u"10"}],
        )
        self.assertNotIn(u"_error", items[0])
        self.assertTrue(items[1][u"_error"])
        self.assertTrue(items[2][u"_error"])
        self.assertTrue(items[3][u"_error"])
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", u"e_service: _eid 9, Not in matching file"),
                ("dti", "ERROR", u"e_service: _eid 11, Cannot find uid '' matched with service"),
                ("dti", "ERROR", u"e_service: _eid 12, Cannot find uid 'abcd' matched with service"),
            ],
        )
        # plonegroup services are reloaded in storage at the end
        self.assertIn(("dto", "INFO", "Part a: some services have been updated."), self.logs.records)
        self.assertDictEqual(storage["data"]["p_eid_to_orgs"], {u"5": {"uid": grh.UID()}, u"10": {"uid": secr.UID()}})
        self.assertListEqual(storage["data"]["p_orgs_all"][secr.UID()]["eids"], [u"10"])
        self.assertDictEqual(dict(storage["course"]), {"a__service_update": 6})


class TestBMailtypesByType(DmsTestCase):

    options = {
        "condition": "python: not storage['data'].get('e_mailtype_match') and item.get('_bpk') == 'e_mailtype'",
        "related_storage": "e_used_mailtype",
    }

    def test___init__(self):
        bp = self.section(BMailtypesByType, "b__1_mailtype_by_type", self.options, storage=self.make_storage("a"))
        self.assertEqual(bp.parts, "b")
        self.assertFalse(hasattr(bp, "related_storage"))
        bp = self.section(BMailtypesByType, "b__1_mailtype_by_type", self.options, storage=self.make_storage("b"))
        self.assertEqual(bp.related_storage, u"e_used_mailtype")
        self.assertIsInstance(bp.related_storage, six.text_type)

    def test___iter__(self):
        # e_used_mailtype: _source_id => _etype (E: incoming, S: outgoing) => used mails
        used = {u"5": {u"E": {u"_eid": u"100"}, u"S": {u"_eid": u"101"}}, u"6": {u"E": {u"_eid": u"102"}}}
        items = [
            {u"_bpk": u"e_mailtype", u"_eid": u"5", u"_etitle": u"Courrier"},
            {u"_bpk": u"e_mailtype", u"_eid": u"6", u"_etitle": u"Facture"},
            {u"_bpk": u"e_service", u"_eid": u"7"},
        ]
        bp = self.section(
            BMailtypesByType, "b__1_mailtype_by_type", self.options, items, self.make_storage("b", e_used_mailtype=used)
        )
        # _etype is read when yielded: the same dict is yielded once per type
        yielded = [(item[u"_eid"], item.get(u"_etype"), item) for item in bp]
        self.assertListEqual(sorted(tup[:2] for tup in yielded[:2]), [(u"5", u"E"), (u"5", u"S")])
        self.assertListEqual([tup[:2] for tup in yielded[2:]], [(u"6", u"E"), (u"7", None)])
        # Plone 4 behaviour: the same dict object is yielded for each type and keeps the last one
        self.assertIs(yielded[0][2], yielded[1][2])
        self.assertIn(items[0][u"_etype"], (u"E", u"S"))
        self.assertDictEqual(items[1], {u"_bpk": u"e_mailtype", u"_eid": u"6", u"_etitle": u"Facture", u"_etype": u"E"})
        self.assertDictEqual(items[2], {u"_bpk": u"e_service", u"_eid": u"7"})
        self.assertDictEqual(dict(bp.storage["course"]), {"b__1_mailtype_by_type": 2})
        # the matching file is already completed: items are passed unchanged
        items = [{u"_bpk": u"e_mailtype", u"_eid": u"5"}]
        storage = self.make_storage("b", e_used_mailtype=used, e_mailtype_match={u"5": {}})
        bp = self.section(BMailtypesByType, "b__1_mailtype_by_type", self.options, items, storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mailtype", u"_eid": u"5"}])


class TestBMailtypeUpdate(DmsTestCase):

    options = {"condition": "python: item['_bpk'] == u'e_mailtype_match'"}

    def test___init__(self):
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, storage=self.make_storage("a"))
        self.assertDictEqual(bp.to_add, {})
        self.assertFalse(hasattr(bp, "condition"))
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, storage=self.make_storage("b"))
        self.assertTrue(bp.condition({"_bpk": u"e_mailtype_match"}))

    def test___iter__(self):
        te_before = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["te"])]
        ts_before = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["ts"])]

        def match(eid, c_type, key, title, active=True):
            return {
                u"_bpk": u"e_mailtype_match",
                u"_eid": eid,
                u"_etype": u"E",
                u"_etitle": title,
                u"_enature": u"",
                u"_esource": u"",
                u"_c_type": c_type,
                u"_key": key,
                u"_title": title,
                u"_active": active,
            }

        items = [
            match(u"1", u"te", u"courrier", u"Courrier"),  # already existing
            match(u"2", u"te", u"zebre", u"Zèbre", False),
            match(u"3", u"te", u"avis", u"Avis"),
            match(u"4", u"te", u"avis", u"Avis bis"),  # same key: first one kept
            match(u"5", u"ts", u"note", u"Note"),
            match(u"6", u"te", u"", u"Not matched"),
            {u"_bpk": u"e_mailtype", u"_eid": u"7"},
        ]
        storage = self.make_storage("b")
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, items, storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mailtype", u"_eid": u"7"}])
        self.assertTrue(items[5][u"_error"])
        self.assertIn(("dti", "WARNING", u"e_mailtype_match: _eid 6, Empty match: we pass _eid 6"), self.logs.records)
        # new values are added in the registry, sorted by title
        te_after = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["te"])]
        self.assertListEqual(
            te_after,
            te_before
            + [
                {u"value": u"avis", u"dtitle": u"Avis", u"active": True},
                {u"value": u"zebre", u"dtitle": u"Zèbre", u"active": False},
            ],
        )
        ts_after = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["ts"])]
        self.assertListEqual(ts_after, ts_before + [{u"value": u"note", u"dtitle": u"Note", u"active": True}])
        # storage is updated
        self.assertDictEqual(storage["data"]["p_mailtype"], get_mailtypes(self.portal))
        self.assertIn(u"avis", storage["data"]["p_mailtype"]["te"])
        self.assertDictEqual(dict(storage["course"]), {"b__mailtype_update": 6})
        # nothing to add: registry not modified
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, items[:1], self.make_storage("b"))
        self.assertListEqual(list(bp), [])
        self.assertListEqual([dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["te"])], te_after)


class TestContactAsTextUpdate(DmsTestCase):

    # data-transfer.cfg.1 q__1_recipients_as_text
    options = {
        "bp_key": "e_mail_recipient",
        "condition": "python: item.get('_bpk') == u'e_mail_recip_o'",
        "mail_store_key": "e_mail_o",
        "contact_store_key": "e_contact_path",
        "mail_id_key": "_mail_id",
        "contact_id_key": "_contact_id",
        "contact_label": "DESTINATAIRE",
        "related_label": "",
        "contact_free_key": "_contact_text",
        "original_item": "0",
        "yield_original": "0",
        "skip_real_contact": "1",
        "skip_contact_user": "1",
    }
    # data-transfer.cfg.2 m__2_sender_as_text
    options2 = {
        "bp_key": "e_mail_i_sender",
        "condition": "python: item.get('_bpk') == u'e_mail_recip_i'",
        "mail_store_key": "e_mail_i",
        "contact_store_key": "e_contact",
        "mail_id_key": "_mail_id",
        "contact_id_key": "_sender_id",
        "contact_label": "EXPÉDITEUR",
        "related_label": "",
        "contact_free_key": "",
        "yield_original": "0",
        "original_item": "0",
        "skip_real_contact": "0",
        "skip_contact_user": "0",
    }
    e_contact = {
        u"7": {u"lastname": u"Courant", u"firstname": u"Jean", u"_email1": u"jean@electrabel.be"},
        u"8": {
            u"lastname": u"Dupont",
            u"firstname": u"Jean",
            u"_street": u"Rue Haute",
            u"_pc": u"7340",
            u"_city": u"Colfontaine",
            u"_phone1": u"065/123456",
            u"_email1": u"jean@dupont.be",
        },
        u"9": {u"lastname": u"Électricien", u"_parent_id": u"10", u"_function": u"Gérant"},
        u"10": {u"lastname": u"Électricité SA", u"_street": u"Rue Basse", u"_cell1": u"0475/123456"},
    }

    def test___init__(self):
        storage = self.make_storage("l", e_mail_o={}, e_contact_path={})
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", self.options, storage=storage)
        self.assertEqual(bp.parts, "q")
        self.assertFalse(hasattr(bp, "bp_key"))
        self.assertNotIn("e_mail_recipient", storage["data"])
        storage = self.make_storage("q", e_mail_o={u"1": {}}, e_contact_path={u"7": {}})
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", self.options, storage=storage)
        self.assertEqual(bp.bp_key, u"e_mail_recipient")
        self.assertEqual(bp.contact_label, u"DESTINATAIRE")
        self.assertEqual(bp.related_label, u"")
        self.assertEqual(
            (bp.original_item, bp.yield_original, bp.skip_real_contact, bp.skip_contact_user),
            (False, False, True, True),
        )
        self.assertIs(bp.mail_paths, storage["data"]["e_mail_o"])
        self.assertIs(bp.e_c, storage["data"]["e_contact_path"])
        self.assertIs(bp.batch_store, storage["data"]["e_mail_recipient"])
        # non ascii label, related label prefixed by a space, optional mail store
        options = dict(self.options2, related_label="PARENT")
        del options["skip_contact_user"]
        storage = self.make_storage("m", e_contact={})
        bp = self.section(ContactAsTextUpdate, "m__2_sender_as_text", options, storage=storage)
        self.assertEqual(bp.contact_label, u"EXPÉDITEUR")
        self.assertIsInstance(bp.contact_label, six.text_type)
        self.assertEqual(bp.related_label, u" PARENT")
        self.assertFalse(bp.skip_contact_user)
        self.assertDictEqual(bp.mail_paths, {})

    def test___iter__(self):
        om = self.mail("reponse1")
        om_path = self.path(om)
        storage = self.make_storage(
            "q",
            e_mail_o={u"100": {u"path": om_path}, u"101": {u"path": u"/outgoing-mail/unknown"}},
            e_contact_path={u"7": {u"path": u"/contacts/jeancourant", u"_is_user": False}},
            e_contact=self.e_contact,
        )

        def recip(eid, mail_id, contact_id=None, text=None, **kw):
            dic = {
                u"_bpk": u"e_mail_recip_o",
                u"_eid": eid,
                u"_mail_id": mail_id,
                u"_contact_id": contact_id,
                u"_contact_text": text,
            }
            dic.update(kw)
            return dic

        items = [
            recip(u"1", u"100", u"7"),  # contact already created as object: passed
            recip(u"2", u"100"),  # no contact: passed
            recip(
                u"3", u"100", u"8", u"Remis en main propre", _action=u"pour suite", _message=u"urgent", _response=None
            ),
            recip(u"4", u"101", u"8"),  # mail not found
            recip(u"5", u"100", None, u"Service X\nRue Y 2"),
            {u"_bpk": u"e_mail_o", u"_eid": u"6"},
        ]
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", self.options, items, storage)
        result = list(bp)
        self.assertListEqual(
            result,
            [
                {
                    "_eid": u"3",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": u"e_mail_recipient",
                    "_act": "U",
                    "description": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"DESTINATAIRE LIBRE: Remis en main propre",
                    "data_transfer": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.\r\n"
                    u"COMPLÉMENT DESTINATAIRE: pour suite, message=urgent",
                    "modification_date": om.creation_date,
                },
                {
                    "_eid": u"5",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": u"e_mail_recipient",
                    "_act": "U",
                    "description": u"DESTINATAIRE LIBRE: Service X",
                    "data_transfer": u"DESTINATAIRE LIBRE: Service X, Rue Y 2",
                    "modification_date": om.creation_date,
                },
                {u"_bpk": u"e_mail_o", u"_eid": u"6"},
            ],
        )
        self.assertIsInstance(result[0]["description"], six.text_type)
        self.assertIsInstance(result[0]["data_transfer"], six.text_type)
        # the mail is not modified: next sections will update it
        self.assertEqual(om.description, u"")
        self.assertDictEqual(storage["data"]["e_mail_recipient"], {u"1": 0, u"2": 0, u"3": 0, u"4": 0, u"5": 0})
        self.assertIn(("dto", "WARNING", "mail 101: path '/outgoing-mail/unknown' not found"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"q__1_recipients_as_text": 5})
        # yield_original, contact user skipped, description only (no data_transfer field)
        options = dict(self.options, yield_original="1", skip_real_contact="0")
        storage = self.make_storage(
            "q",
            e_mail_o={u"100": {u"path": om_path}},
            e_contact_path={
                u"7": {u"path": u"/contacts/jeancourant", u"_is_user": True},
                u"8": {u"path": u"/contacts/jeancourant", u"_is_user": False},
            },
            e_contact=self.e_contact,
            desc_only=True,
        )
        storage["desc_only"] = True
        items = [recip(u"1", u"100", u"7"), recip(u"2", u"100"), recip(u"3", u"100", u"8")]
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                recip(u"2", u"100"),
                recip(u"3", u"100", u"8"),
                {
                    "_eid": u"3",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": u"e_mail_recipient",
                    "_act": "U",
                    "description": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.",
                    # Plone 4 behaviour: data_transfer is also set with desc_only (the field doesn't exist)
                    "data_transfer": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.",
                    "modification_date": om.creation_date,
                },
            ],
        )
        # data-transfer.cfg.2 m__2_sender_as_text: contact with parent, im existing description kept
        im = self.mail("courrier1")
        im.data_transfer = u"AGENT: Agent Fred."
        storage = self.make_storage("m", e_mail_i={u"200": {u"path": self.path(im)}}, e_contact=self.e_contact)
        items = [
            {u"_bpk": u"e_mail_recip_i", u"_eid": u"1", u"_mail_id": u"200", u"_sender_id": u"9"},
            {u"_bpk": u"e_mail_recip_i", u"_eid": u"2", u"_mail_id": u"200", u"_sender_id": u"0"},  # empty contact
        ]
        storage["data"]["e_contact"][u"0"] = {u"lastname": u""}
        self.addCleanup(storage["data"]["e_contact"].pop, u"0")
        bp = self.section(ContactAsTextUpdate, "m__2_sender_as_text", self.options2, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_eid": u"1",
                    "_path": self.path(im),
                    "_type": "dmsincomingmail",
                    "_bpk": u"e_mail_i_sender",
                    "_act": "U",
                    "description": u"Ceci est la description du courrier 1\r\nEXPÉDITEUR: Électricité SA.\r\n"
                    u"EXPÉDITEUR: Électricien, Gérant.",
                    "data_transfer": u"AGENT: Agent Fred.\r\nEXPÉDITEUR: Électricité SA.\r\n"
                    u"EXPÉDITEUR: Électricien, Gérant.\r\nADRESSE EXPÉDITEUR: Rue Basse.\r\n"
                    u"TÉL EXPÉDITEUR: 0475/123456.",
                    "modification_date": im.creation_date,
                },
            ],
        )
        # original item mode: the item itself is completed and always yielded
        options = dict(self.options, original_item="1")
        storage = self.make_storage(
            "q",
            e_contact_path={u"7": {u"path": u"/contacts/jeancourant", u"_is_user": False}},
            e_contact=self.e_contact,
        )
        items = [
            recip(u"1", u"100", u"7"),  # skipped real contact
            recip(u"2", u"100"),  # no contact
            recip(u"3", u"100", u"8", description=u"Desc", data_transfer=u"DT"),
            recip(u"4", u"100", text=u'""'),  # only quotes: no contact info
        ]
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                recip(u"1", u"100", u"7"),
                recip(u"2", u"100"),
                recip(
                    u"3",
                    u"100",
                    u"8",
                    description=u"Desc\r\nDESTINATAIRE: Dupont Jean, jean@dupont.be.",
                    data_transfer=u"DT\r\nDESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.",
                ),
                recip(u"4", u"100", text=u'""'),
            ],
        )
        self.assertNotIn(u"_eid", storage["data"]["e_mail_recipient"])  # no batch store with original item
        # part not run
        bp = self.section(
            ContactAsTextUpdate, "q__1_recipients_as_text", self.options, items[:1], self.make_storage("p")
        )
        self.assertListEqual(list(bp), [recip(u"1", u"100", u"7")])


class TestContactSet(DmsTestCase):

    # data-transfer.cfg.1 l__contact_set
    options = {
        "bp_key": "e_mail_sender",
        "fieldname": "sender",
        "is_list": "1",
        "contact_id_key": "_sender_id",
        "condition": "python: item.get('_bpk') == u'e_mail_i'",
        "contact_store_key": "e_contact_path",
        "original_item": "1",
        "skip_contact_user": "0",
    }
    # data-transfer.cfg.1 q__1_recipients
    options2 = {
        "bp_key": "e_mail_recipient",
        "condition": "python: item.get('_bpk') == u'e_mail_recip_o'",
        "fieldname": "recipients",
        "is_list": "1",
        "mail_id_key": "_mail_id",
        "mail_store_key": "e_mail_o",
        "contact_id_key": "_contact_id",
        "contact_store_key": "e_contact_path",
        "original_item": "0",
        "skip_contact_user": "1",
        "yield": "1",
    }

    def test___init__(self):
        storage = self.make_storage("q")
        bp = self.section(ContactSet, "l__contact_set", self.options, storage=storage)
        self.assertEqual(bp.bp_key, u"e_mail_sender")  # set before the part check
        self.assertFalse(hasattr(bp, "fieldname"))
        bp = self.section(ContactSet, "l__contact_set", self.options, storage=self.make_storage("l"))
        self.assertEqual(bp.fieldname, u"sender")
        self.assertEqual(bp.mail_id_key, u"_mail_id")
        self.assertEqual(bp.contact_id_key, u"_sender_id")
        self.assertTrue(bp.is_list)
        self.assertTrue(bp.original_item)
        self.assertFalse(bp.skip_contact_user)
        self.assertFalse(bp.yld)
        self.assertDictEqual(bp.mail_paths, {})
        self.assertDictEqual(bp.e_c, {})  # created if missing
        self.assertIs(bp.e_c, bp.storage["data"]["e_contact_path"])
        self.assertIs(bp.batch_store, bp.storage["data"]["e_mail_sender"])
        # default contact from storage (Initialization default_contact)
        self.assertEqual(bp.def_ctct_iid, self.iid("/contacts/notencoded"))
        # default contact from option
        options = dict(self.options2, default_contact="contacts/jeancourant", is_list="0")
        bp = self.section(ContactSet, "q__1_recipients", options, storage=self.make_storage("q", e_mail_o={}))
        self.assertEqual(bp.def_ctct_iid, self.iid("/contacts/jeancourant"))
        self.assertFalse(bp.is_list)
        self.assertFalse(bp.original_item)
        self.assertTrue(bp.skip_contact_user)
        self.assertTrue(bp.yld)
        self.assertIs(bp.mail_paths, bp.storage["data"]["e_mail_o"])
        # no default contact
        storage = self.make_storage("l")
        storage["plone"]["def_contact"] = None
        bp = self.section(ContactSet, "l__contact_set", self.options, storage=storage)
        self.assertIsNone(bp.def_ctct_iid)

    def test___iter__(self):
        def_iid = self.iid("/contacts/notencoded")
        jc_iid = self.iid("/contacts/jeancourant")
        el_iid = self.iid("/contacts/electrabel")
        e_c = {
            u"7": {u"path": u"/contacts/jeancourant", u"_is_user": False},
            u"8": {u"path": u"/contacts/electrabel", u"_is_user": True},
            u"9": {u"path": u"/contacts/unknown", u"_is_user": False},
        }
        # original item (l__contact_set)
        storage = self.make_storage("l", e_contact_path=e_c)
        items = [
            {u"_bpk": u"e_mail_i", u"_eid": u"1", u"_sender_id": u"7"},
            {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_sender_id": u"6"},  # not created contact: default contact
            {u"_bpk": u"e_mail_i", u"_eid": u"3", u"_sender_id": u"9"},  # contact path not found: default contact
            {u"_bpk": u"e_mail_o", u"_eid": u"4", u"_sender_id": u"7"},
        ]
        bp = self.section(ContactSet, "l__contact_set", self.options, items, storage)
        result = list(bp)
        self.assertListEqual([item[u"_eid"] for item in result], [u"1", u"2", u"3", u"4"])
        self.assertListEqual([rel.to_id for rel in result[0][u"sender"]], [jc_iid])
        self.assertListEqual([rel.to_id for rel in result[1][u"sender"]], [def_iid])
        self.assertListEqual([rel.to_id for rel in result[2][u"sender"]], [def_iid])
        self.assertDictEqual(result[3], {u"_bpk": u"e_mail_o", u"_eid": u"4", u"_sender_id": u"7"})
        self.assertDictEqual(storage["data"]["e_mail_sender"], {})
        self.assertDictEqual(dict(storage["course"]), {"l__contact_set": 3})
        # existing value: contact added and default contact removed. Not a list field.
        items = [
            {u"_bpk": u"e_mail_i", u"_eid": u"1", u"_sender_id": u"7", u"sender": [RelationValue(def_iid)]},
            {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_sender_id": u"7", u"sender": [RelationValue(jc_iid)]},
        ]
        bp = self.section(ContactSet, "l__contact_set", self.options, items, self.make_storage("l", e_contact_path=e_c))
        result = list(bp)
        self.assertListEqual([rel.to_id for rel in result[0][u"sender"]], [jc_iid])
        self.assertListEqual([rel.to_id for rel in result[1][u"sender"]], [jc_iid])
        options = dict(self.options, is_list="0", skip_contact_user="1")
        items = [
            {u"_bpk": u"e_mail_i", u"_eid": u"1", u"_sender_id": u"7"},
            {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_sender_id": u"8"},  # user contact skipped: default contact
        ]
        bp = self.section(ContactSet, "l__contact_set", options, items, self.make_storage("l", e_contact_path=e_c))
        result = list(bp)
        self.assertIsInstance(result[0][u"sender"], RelationValue)
        self.assertEqual(result[0][u"sender"].to_id, jc_iid)
        self.assertEqual(result[1][u"sender"].to_id, def_iid)
        # no default contact: item yielded unchanged when the contact is not found
        storage = self.make_storage("l", e_contact_path=e_c)
        storage["plone"]["def_contact"] = None
        items = [{u"_bpk": u"e_mail_i", u"_eid": u"1", u"_sender_id": u"6"}]
        bp = self.section(ContactSet, "l__contact_set", self.options, items, storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mail_i", u"_eid": u"1", u"_sender_id": u"6"}])
        # mail modified (q__1_recipients): reponse1 has a recipient
        om = self.mail("reponse1")
        om_path = self.path(om)
        orig_recipients = [rel.to_id for rel in om.recipients]
        storage = self.make_storage(
            "q", e_contact_path=e_c, e_mail_o={u"100": {u"path": om_path}, u"101": {u"path": u"/outgoing-mail/no"}}
        )
        items = [
            {u"_bpk": u"e_mail_recip_o", u"_eid": u"1", u"_mail_id": u"100", u"_contact_id": u"7"},
            {u"_bpk": u"e_mail_recip_o", u"_eid": u"2", u"_mail_id": u"101", u"_contact_id": u"7"},
            {u"_bpk": u"e_mail_recip_o", u"_eid": u"3", u"_mail_id": u"100", u"_contact_id": u"8"},
        ]
        bp = self.section(ContactSet, "q__1_recipients", self.options2, items, storage)
        # relations read when yielded
        result = [(item, [rel.to_id for rel in item.get("recipients", [])]) for item in bp]
        self.assertListEqual(
            [dict((k, v) for k, v in item.items() if k != "recipients") for item, rels in result],
            [
                {
                    "_eid": u"1",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": u"e_mail_recipient",
                    "_act": "U",
                    "modification_date": om.creation_date,
                },
                {u"_bpk": u"e_mail_recip_o", u"_eid": u"1", u"_mail_id": u"100", u"_contact_id": u"7"},
                # mail not found: original item not yielded
                {
                    "_eid": u"3",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": u"e_mail_recipient",
                    "_act": "U",
                    "modification_date": om.creation_date,
                },
                {u"_bpk": u"e_mail_recip_o", u"_eid": u"3", u"_mail_id": u"100", u"_contact_id": u"8"},
            ],
        )
        self.assertListEqual(result[0][1], orig_recipients + [jc_iid])
        # user contact skipped: default contact used but removed because there is another contact
        self.assertListEqual(result[2][1], orig_recipients + [jc_iid])
        # Plone 4 behaviour (bug?): the mail field list is modified in place (the yielded list for eid 1 is the
        # mail list) and the default contact appended to it is not removed (only the yielded copy is filtered)
        self.assertIs(result[0][0]["recipients"], om.recipients)
        self.assertListEqual([rel.to_id for rel in om.recipients], orig_recipients + [jc_iid, def_iid])
        self.assertDictEqual(storage["data"]["e_mail_recipient"], {u"1": 0, u"2": 0, u"3": 0})
        self.assertIn(("dto", "WARNING", "mail 101: path '/outgoing-mail/no' not found"), self.logs.records)
        self.assertListEqual(orig_recipients, [el_iid])  # fixture: reponse1 recipient is electrabel
        # no contact found and no default contact: original item yielded following yield option
        storage = self.make_storage("q", e_contact_path=e_c, e_mail_o={u"100": {u"path": om_path}})
        storage["plone"]["def_contact"] = None
        items = [{u"_bpk": u"e_mail_recip_o", u"_eid": u"1", u"_mail_id": u"100", u"_contact_id": u"6"}]
        bp = self.section(ContactSet, "q__1_recipients", self.options2, items, storage)
        self.assertListEqual(
            list(bp), [{u"_bpk": u"e_mail_recip_o", u"_eid": u"1", u"_mail_id": u"100", u"_contact_id": u"6"}]
        )
        bp = self.section(ContactSet, "q__1_recipients", dict(self.options2, **{"yield": "0"}), items, storage)
        self.assertListEqual(list(bp), [])
        # part not run
        bp = self.section(ContactSet, "q__1_recipients", self.options2, items, self.make_storage("l"))
        self.assertListEqual(list(bp), items)


class TestDefaultContactSet(DmsTestCase):

    # data-transfer.cfg.1 p__default_contact_set
    options = {"fieldname": "recipients", "is_list": "1", "condition": "python: item.get('_bpk') == u'e_mail_o'"}

    def test___init__(self):
        bp = self.section(DefaultContactSet, "p__default_contact_set", self.options, storage=self.make_storage("l"))
        self.assertEqual(bp.parts, "p")
        self.assertFalse(hasattr(bp, "fieldname"))
        bp = self.section(DefaultContactSet, "p__default_contact_set", self.options, storage=self.make_storage("p"))
        self.assertEqual(bp.fieldname, u"recipients")
        self.assertTrue(bp.is_list)
        self.assertEqual(bp.def_ctct_iid, self.iid("/contacts/notencoded"))
        bp = self.section(
            DefaultContactSet, "p__default_contact_set", dict(self.options, is_list="0"), storage=self.make_storage("p")
        )
        self.assertFalse(bp.is_list)

    def test___iter__(self):
        def_iid = self.iid("/contacts/notencoded")
        items = [{u"_bpk": u"e_mail_o", u"_eid": u"1"}, {u"_bpk": u"e_mail_i", u"_eid": u"2"}]
        storage = self.make_storage("p")
        bp = self.section(DefaultContactSet, "p__default_contact_set", self.options, items, storage)
        result = list(bp)
        self.assertListEqual([rel.to_id for rel in result[0][u"recipients"]], [def_iid])
        self.assertDictEqual(result[1], {u"_bpk": u"e_mail_i", u"_eid": u"2"})
        self.assertDictEqual(dict(storage["course"]), {"p__default_contact_set": 1})
        items = [{u"_bpk": u"e_mail_o", u"_eid": u"1"}]
        options = dict(self.options, is_list="0")
        bp = self.section(DefaultContactSet, "p__default_contact_set", options, items, self.make_storage("p"))
        self.assertEqual(next(iter(bp))[u"recipients"].to_id, def_iid)
        # part not run
        items = [{u"_bpk": u"e_mail_o", u"_eid": u"1"}]
        bp = self.section(DefaultContactSet, "p__default_contact_set", self.options, items, self.make_storage("l"))
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mail_o", u"_eid": u"1"}])


class DTestCase(DmsTestCase):
    """Data of part d (personnel)."""

    def d_storage(self):
        e_user_match = {
            u"u1": {u"_nom": u"Agent", u"_prenom": u"Fred", u"_p_userid": u"agent", u"_fullname": u"Fred Agent"},
            u"u2": {u"_nom": u"Scanner", u"_prenom": u"Jean", u"_p_userid": u"scanner", u"_fullname": u"Jean Scanner"},
            u"u3": {u"_nom": u"Dupuis", u"_prenom": u"Éric", u"_p_userid": u"", u"_fullname": u""},
        }
        e_contacts_sender = {u"20": {u"_user_id": u"u1"}, u"21": {u"_user_id": u"u2"}, u"22": {u"_user_id": u"u3"}}
        dg_uid = self.uid(PLONEGROUP + "/direction-generale")
        return self.make_storage(
            "d", e_user_match=e_user_match, e_contacts_sender=e_contacts_sender, p_eid_to_orgs={u"S1": {"uid": dg_uid}}
        )

    def new_person(self, pdic, storage):
        """Simulates the next sections on a new person item: constructor and post_actions."""
        api.content.create(
            container=self.obj("/contacts/personnel-folder"),
            type="person",
            id=pdic["_path"].split("/")[-1],
            lastname=pdic["lastname"],
        )
        list(self.section(PostActions, "post_actions", {}, [pdic], storage))

    def new_person_dic(self, path, eid, euid, title, bpk=u"person_sender", **kwargs):
        cdate = datetime(2009, 9, 22, 12, 0)
        dic = {
            "_type": "person",
            "_path": path,
            u"internal_number": euid,
            "use_parent_address": False,
            u"_bpk": bpk,
            u"_eid": eid,
            u"_deactivate": True,
            u"creation_date": cdate,
            "_act": "N",
            u"modification_date": cdate,
            u"title": title,
            u"_post_actions": {u"store_internal_person_info": ""},
        }
        dic.update(kwargs)
        return dic

    def new_hp_dic(self, path, eid):
        cdate = datetime(2009, 9, 22, 12, 0)
        return {
            "_type": "held_position",
            "_path": path,
            "use_parent_address": True,
            "internal_number": u"",
            u"_bpk": u"hp_sender",
            u"_eid": eid,
            u"_deactivate": True,
            u"creation_date": cdate,
            "_act": "N",
            u"modification_date": cdate,
        }


class TestHandlersFunctions(DTestCase):
    """Module functions of handlers.py needing an ia.docs site."""

    def test_person_dic(self):
        storage = self.d_storage()
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", {}, storage=storage)
        lecteur = self.obj("/contacts/personnel-folder/lecteur")
        lecteur.internal_number = u"x1"
        lecteur.firstname = None
        lecteur.userid = None
        # existing person: fields completed if empty
        res = person_dic(
            bp,
            u"personnel",
            u"u9",
            {u"_eid": u"1"},
            u"lecteur",
            "lecteur",
            u"Existing",
            firstname=u"Jef",
            lastname=u"Lecteur",
        )
        self.assertListEqual(
            res,
            [
                {
                    "_type": "person",
                    "_path": "/contacts/personnel-folder/lecteur",
                    u"internal_number": u"x1,u9",
                    u"_bpk": u"personnel",
                    u"_eid": u"1",
                    u"title": u"Existing",
                    "_act": "U",
                    u"firstname": u"Jef",
                    "userid": u"lecteur",
                }
            ],
        )
        self.assertIsInstance(res[0][u"internal_number"], six.text_type)
        self.assertTrue(bp.change)
        self.assertEqual(storage["data"]["p_euid_to_pers"][u"u9"], lecteur.UID())
        # already handled external user
        self.assertListEqual(person_dic(bp, u"personnel", u"u9", {u"_eid": u"2"}, None, "lecteur", u"Existing"), [])
        # external user id already stored on the person
        lecteur.internal_number = u"u8"
        res = person_dic(bp, u"personnel", u"u8", {u"_eid": u"3"}, None, "lecteur", u"Existing")
        self.assertEqual(res[0][u"internal_number"], u"u8")
        self.assertNotIn("userid", res[0])
        # new person, without userid nor firstname and without deactivation
        res = person_dic(
            bp, u"personnel", u"u7", {u"_eid": u"4"}, None, "dupont", u"Unmatched", lastname=u"Dupont", transitions=()
        )
        self.assertListEqual(
            res,
            [
                self.new_person_dic(
                    "/contacts/personnel-folder/dupont",
                    u"4",
                    u"u7",
                    u"Unmatched",
                    bpk=u"personnel",
                    _deactivate=False,
                    lastname=u"Dupont",
                )
            ],
        )
        self.assertNotIn(u"u7", storage["data"]["p_euid_to_pers"])  # stored by post_actions after creation

    def test_hp_dic(self):
        storage = self.d_storage()
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", {}, storage=storage)
        agent = self.obj("/contacts/personnel-folder/agent")
        dg = self.obj(PLONEGROUP + "/direction-generale")
        secr = self.obj(PLONEGROUP + "/direction-generale/secretariat")
        storage["data"]["p_euid_to_pers"][u"u1"] = agent.UID()
        storage["data"]["p_eid_to_orgs"][u"S2"] = {"uid": secr.UID()}
        # existing held position
        self.assertListEqual(hp_dic(bp, u"hp_sender", u"u1", {u"_eid": u"1", u"_service": u"S2"}), [])
        self.assertFalse(bp.change)
        # new held position
        res = hp_dic(bp, u"hp_sender", u"u1", {u"_eid": u"2", u"_service": u"S1"}, transitions=())
        self.assertEqual(res[0].pop("position").to_id, self.intids.getId(dg))
        path = "/contacts/personnel-folder/agent/{}".format(dg.UID())
        self.assertListEqual(res, [dict(self.new_hp_dic(path, u"2"), _deactivate=False)])
        self.assertTrue(bp.change)
        self.assertDictEqual(
            storage["data"]["p_hps"][agent.UID()]["hps"][dg.UID()], {"path": path, "state": "deactivated"}
        )


class TestDOMSenderCreation(DTestCase):

    options = {"condition": "python: item.get('_bpk') == u'e_mail_sender'"}

    def test___init__(self):
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", self.options, storage=self.make_storage("l"))
        self.assertFalse(bp.change)
        self.assertFalse(hasattr(bp, "condition"))
        storage = self.d_storage()
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", self.options, storage=storage)
        self.assertIs(bp.puid_to_pers, storage["data"]["p_userid_to_pers"])
        self.assertIs(bp.euid_to_pers, storage["data"]["p_euid_to_pers"])
        self.assertIs(bp.p_hps, storage["data"]["p_hps"])
        self.assertIs(bp.eid_to_orgs, storage["data"]["p_eid_to_orgs"])
        self.assertIs(bp.e_c_s, storage["data"]["e_contacts_sender"])
        self.assertIs(bp.e_u_m, storage["data"]["e_user_match"])
        bp = self.section(
            DOMSenderCreation,
            "d__om_sender_creation",
            dict(self.options, sender_user_dic="e_contact"),
            storage=self.make_storage("d"),
        )
        self.assertDictEqual(bp.e_c_s, {})
        self.assertDictEqual(bp.e_u_m, {})

    def test___iter__(self):
        storage = self.d_storage()
        agent = self.obj("/contacts/personnel-folder/agent")
        dg = self.obj(PLONEGROUP + "/direction-generale")
        dg_iid = self.intids.getId(dg)
        items = [
            {u"_bpk": u"e_mail_sender", u"_eid": u"100", u"_sender_id": u"20", u"_service": u"S1"},  # existing person
            {u"_bpk": u"e_mail_sender", u"_eid": u"101", u"_sender_id": u"20", u"_service": u"S1"},  # already done
            {u"_bpk": u"e_mail_sender", u"_eid": u"102", u"_sender_id": u"21", u"_service": u"S1"},  # matched user
            {u"_bpk": u"e_mail_sender", u"_eid": u"103", u"_sender_id": u"22", u"_service": u"S1"},  # not matched
            {u"_bpk": u"e_mail_sender", u"_eid": u"104", u"_sender_id": None, u"_service": u"S1"},  # no sender
            {u"_bpk": u"e_mail_o", u"_eid": u"105"},
        ]
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", self.options, items, storage)
        gen = iter(bp)
        # plone user with a person
        self.assertDictEqual(
            next(gen),
            {
                "_type": "person",
                "_path": "/contacts/personnel-folder/agent",
                u"internal_number": u"u1",
                u"_bpk": u"person_sender",
                u"_eid": u"100",
                u"title": u"Existing",
                "_act": "U",
            },
        )
        self.assertEqual(storage["data"]["p_euid_to_pers"][u"u1"], agent.UID())
        hpdic = next(gen)
        self.assertEqual(hpdic.pop("position").to_id, dg_iid)
        self.assertDictEqual(hpdic, self.new_hp_dic("/contacts/personnel-folder/agent/" + dg.UID(), u"100"))
        # plone user without person
        pdic = next(gen)
        path = "/contacts/personnel-folder/scanner"
        self.assertDictEqual(
            pdic,
            self.new_person_dic(
                path, u"102", u"u2", u"Matched", userid=u"scanner", firstname=u"Jean", lastname=u"Scanner"
            ),
        )
        self.new_person(pdic, storage)
        hpdic = next(gen)
        self.assertEqual(hpdic.pop("position").to_id, dg_iid)
        self.assertDictEqual(hpdic, self.new_hp_dic(path + "/" + dg.UID(), u"102"))
        # external user not matched with a plone user
        pdic = next(gen)
        path = "/contacts/personnel-folder/eric-dupuis"
        self.assertDictEqual(
            pdic, self.new_person_dic(path, u"103", u"u3", u"Unmatched", firstname=u"Éric", lastname=u"Dupuis")
        )
        self.new_person(pdic, storage)
        self.assertEqual(next(gen)["_path"], path + "/" + dg.UID())
        # no sender: generic person
        pdic = next(gen)
        path = "/contacts/personnel-folder/reprise-donnees"
        self.assertDictEqual(
            pdic, self.new_person_dic(path, u"104", u"None", u"None", firstname=u"", lastname=u"Reprise données")
        )
        self.new_person(pdic, storage)
        self.assertEqual(next(gen)["_path"], path + "/" + dg.UID())
        self.assertListEqual(list(gen), [{u"_bpk": u"e_mail_o", u"_eid": u"105"}])
        self.assertIn(
            ("dto", "INFO", "Part d: some internal persons or/and held positions have been added."), self.logs.records
        )
        self.assertDictEqual(dict(storage["course"]), {"d__om_sender_creation": 5, "post_actions": 3})
        # nothing to add
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", self.options, items[:2], storage)
        self.assertListEqual(list(bp), [])
        self.assertFalse(bp.change)
        # part not run
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", self.options, items[:1], self.make_storage("l"))
        self.assertListEqual(list(bp), items[:1])


class TestDPersonnelCreation(DTestCase):

    options = {"condition": "python: item.get('_bpk') == u'e_contacts_sender'"}

    def test___init__(self):
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, storage=self.make_storage("l"))
        self.assertFalse(bp.change)
        self.assertFalse(hasattr(bp, "condition"))
        storage = self.d_storage()
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, storage=storage)
        self.assertIs(bp.puid_to_pers, storage["data"]["p_userid_to_pers"])
        self.assertIs(bp.euid_to_pers, storage["data"]["p_euid_to_pers"])
        self.assertIs(bp.e_u_m, storage["data"]["e_user_match"])
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, storage=self.make_storage("d"))
        self.assertDictEqual(bp.e_u_m, {})

    def test___iter__(self):
        storage = self.d_storage()
        agent = self.obj("/contacts/personnel-folder/agent")
        items = [
            {u"_bpk": u"e_contacts_sender", u"_eid": u"20", u"_user_id": u"u1"},  # existing person
            {u"_bpk": u"e_contacts_sender", u"_eid": u"21", u"_user_id": u"u2"},  # matched user
            {u"_bpk": u"e_contacts_sender", u"_eid": u"22", u"_user_id": u"u3"},  # not matched: nothing
            {u"_bpk": u"e_contacts_sender", u"_eid": u"23", u"_user_id": u"u1"},  # already done
            {u"_bpk": u"e_mail_sender", u"_eid": u"24"},
        ]
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_type": "person",
                    "_path": "/contacts/personnel-folder/agent",
                    u"internal_number": u"u1",
                    u"_bpk": u"personnel",
                    u"_eid": u"20",
                    u"title": u"Existing",
                    "_act": "U",
                },
                self.new_person_dic(
                    "/contacts/personnel-folder/scanner",
                    u"21",
                    u"u2",
                    u"Matched",
                    bpk=u"personnel",
                    userid=u"scanner",
                    firstname=u"Jean",
                    lastname=u"Scanner",
                ),
                {u"_bpk": u"e_mail_sender", u"_eid": u"24"},
            ],
        )
        self.assertEqual(storage["data"]["p_euid_to_pers"][u"u1"], agent.UID())
        self.assertIn(
            ("dto", "INFO", "Part d: some internal persons or/and held positions have been added."), self.logs.records
        )
        self.assertDictEqual(dict(storage["course"]), {"d__personnel_creation": 4})
        # last name first
        storage = self.d_storage()
        storage["plone"]["firstname_first"] = False
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, items[1:2], storage)
        res = list(bp)
        self.assertEqual((res[0][u"firstname"], res[0][u"lastname"]), (u"Scanner", u"Jean"))
        # part not run
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, items[:1], self.make_storage("l"))
        self.assertListEqual(list(bp), items[:1])


class TestECategoryUpdate(DmsTestCase):

    # data-transfer.cfg.1 e__category_update
    options = {
        "b_condition": "python: modules['os'].path.exists(modules['imio.pyutils.system'].full_path(storage['csvp'], "
        "transmogrifier.get('e__category_match_read', {}).get('filename', '_not_found_')))",
        "condition": "python: item.get('_bpk') == 'e_category_match'",
        "bp_key": "e_category_update",
    }
    # data-transfer.cfg.1 e__category_create
    options2 = {
        "b_condition": "python: len(transmogrifier.storage['data']['p_category']) == 0",
        "condition": "python: item.get('_bpk') == 'e_category'",
        "bp_key": "e_category_create",
    }

    def test___init__(self):
        bp = self.section(ECategoryUpdate, "e__category_update", self.options, storage=self.make_storage("a"))
        self.assertFalse(hasattr(bp, "b_cond"))
        storage = self.make_storage("e")
        bp = self.section(ECategoryUpdate, "e__category_update", self.options, storage=storage)
        self.assertFalse(bp.b_cond)  # matching file not found
        self.assertTrue(bp.replace_slash)
        self.assertTrue(bp.decimal_import)
        self.assertDictEqual(bp.parent_relation, {})
        self.assertDictEqual(bp.category_code_eid, {})
        self.assertEqual(bp.prefix, "")
        self.assertIs(bp.p_category, storage["data"]["p_category"])
        io.open(os.path.join(self.tmp, "1_category_match.csv"), "w").close()
        sections = {"e__category_match_read": {"filename": "1_category_match.csv"}}
        bp = self.section(ECategoryUpdate, "e__category_update", self.options, storage=storage, sections=sections)
        self.assertTrue(bp.b_cond)
        # creation only when there is no category in Plone
        bp = self.section(ECategoryUpdate, "e__category_create", self.options2, storage=storage)
        self.assertFalse(bp.b_cond)
        storage = self.make_storage("e", e_category_parent={u"A1": {u"_parent_id": u"-1"}}, e_category_code={u"x": 1})
        storage["data"]["p_category"] = {}
        options = dict(
            self.options2,
            title_replace_slash="0",
            decimal_import="0",
            parent_relation="e_category_parent",
            category_code_eid="e_category_code",
            prefix="-1",
        )
        bp = self.section(ECategoryUpdate, "e__category_create", options, storage=storage)
        self.assertTrue(bp.b_cond)
        self.assertFalse(bp.replace_slash)
        self.assertFalse(bp.decimal_import)
        self.assertIs(bp.parent_relation, storage["data"]["e_category_parent"])
        self.assertIs(bp.category_code_eid, storage["data"]["e_category_code"])
        self.assertEqual(bp.prefix, "-1")

    def test___iter__(self):  # noqa C901
        # update existing categories (match file)
        storage = self.make_storage("e")
        cats = storage["data"]["p_category"]
        cats[u"-1.753"]["obj"].title = u"-1.753"  # title not defined
        options = dict(self.options, b_condition="python: True")

        def match(eid, ecode, etitle, pcode, ptitle, **kw):
            dic = {
                u"_bpk": u"e_category_match",
                u"_eid": eid,
                u"_ecode": ecode,
                u"_etitle": etitle,
                u"_eactive": True,
                u"_pcode": pcode,
                u"_puid": u"",
                u"_ptitle": ptitle,
                u"_pactive": False,
            }
            dic.update(kw)
            return dic

        items = [
            match(u"1", u"1.753", u"Armes / munitions", u"-1.753", u"-1.753"),
            match(u"2", u"1.7", u"Police", u"-1.7", u"Tâches de police."),
            match(u"3", u"9", u"Unknown", u"-9", u""),
            {u"_bpk": u"e_category", u"_eid": u"4"},
        ]
        bp = self.section(ECategoryUpdate, "e__category_update", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                match(
                    u"1",
                    u"1.753",
                    u"Armes / munitions",
                    u"-1.753",
                    u"Armes - munitions",
                    _pactive=True,
                    _act="U",
                    _type="ClassificationCategory",
                    title=u"Armes - munitions",
                ),
                match(
                    u"2",
                    u"1.7",
                    u"Police",
                    u"-1.7",
                    u"Tâches de police.",
                    _type="ClassificationCategory",
                    title=u"Tâches de police.",
                ),
                {u"_bpk": u"e_category", u"_eid": u"4"},
            ],
        )
        node = get_categories(self.portal)[u"-1.753"]
        self.assertEqual((node["title"], node["enabled"]), (u"Armes - munitions", True))
        self.assertEqual((cats[u"-1.753"]["title"], cats[u"-1.753"]["enabled"]), (u"Armes - munitions", True))
        self.assertTrue(items[2][u"_error"])
        self.assertIn(
            ("dti", "ERROR", u"e_category_match: _eid 3, The _pcode '-9' is not in the loaded categories. We pass it"),
            self.logs.records,
        )
        self.assertDictEqual(dict(storage["course"]), {"e__category_update": 3})
        # b_condition False: items passed unchanged
        bp = self.section(ECategoryUpdate, "e__category_update", self.options, items[1:2], self.make_storage("e"))
        self.assertListEqual(list(bp), [items[1]])

        # create categories (decimal codes), parents are created if necessary
        def cat(eid, ecode, etitle, eactive=True, **kw):
            dic = {u"_bpk": u"e_category", u"_eid": eid, u"_ecode": ecode, u"_etitle": etitle, u"_eactive": eactive}
            dic.update(kw)
            return dic

        # e_category is stored by _ecode and _eid (e__category_store)
        del self.logs.records[:]
        storage = self.make_storage("e", e_category={u"-1.759": {u"51": {u"_etitle": u"Foires", u"_eactive": True}}})
        cats = storage["data"]["p_category"]
        options = dict(self.options2, b_condition="python: True")
        items = [
            cat(u"50", u"-1.759.1", u"Marchés / foires"),
            cat(u"51", u"-1.759", u"Foires"),  # already created as parent
            cat(u"52", u"-1.753", u"Armes", False),  # existing code
        ]
        bp = self.section(ECategoryUpdate, "e__category_create", options, items, storage)
        result = list(bp)
        tree = get_categories(self.portal)
        self.assertListEqual(
            result,
            [
                cat(
                    u"50",
                    u"-1.759.1",
                    u"Marchés / foires",
                    _pcode=u"-1.759.1",
                    _ptitle=u"Marchés - foires",
                    _puid=tree[u"-1.759.1"]["uid"],
                    _pactive=True,
                    _act="N",
                    _type="ClassificationCategory",
                    title=u"Marchés - foires",
                ),
                cat(u"51", u"-1.759", u"Foires", _error=True, _type="ClassificationCategory", title=u""),
                cat(
                    u"52",
                    u"-1.753",
                    u"Armes",
                    False,
                    _pcode=u"-1.753-a",
                    _ptitle=u"Armes",
                    _puid=tree[u"-1.753-a"]["uid"],
                    _pactive=False,
                    _act="N",
                    _error=True,
                    _type="ClassificationCategory",
                    title=u"Armes",
                ),
            ],
        )
        # Plone 4 behaviour (bug?): the parent title is not found in e_category (stored by code and eid): code used
        self.assertEqual((tree[u"-1.759"]["title"], tree[u"-1.759"]["enabled"]), (u"-1.759", True))
        self.assertEqual(cats[u"-1.759"]["case"], "parent")
        self.assertEqual(aq_parent(tree[u"-1.759"]["obj"]).identifier, u"-1.75")
        self.assertEqual(aq_parent(tree[u"-1.759.1"]["obj"]).identifier, u"-1.759")
        self.assertEqual(aq_parent(tree[u"-1.753-a"]["obj"]).identifier, u"-1.75")
        self.assertEqual((tree[u"-1.759.1"]["title"], tree[u"-1.759.1"]["enabled"]), (u"Marchés - foires", True))
        self.assertIsInstance(tree[u"-1.759.1"]["title"], six.text_type)
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                (
                    "dti",
                    "ERROR",
                    u"e_category: _eid 51, Same code '-1.759' with title 'Foires' previously created "
                    u"with title '-1.759'",
                ),
                (
                    "dti",
                    "ERROR",
                    u"e_category: _eid 52, Same code '-1.753' with title 'Armes' previously created "
                    u"with title 'Armes - munitions'",
                ),  # title updated above
            ],
        )
        # prefix: the prefix part is not a parent, title kept with slash
        storage = self.make_storage("e", e_category={})
        options = dict(self.options2, b_condition="python: True", prefix="-1", title_replace_slash="0")
        bp = self.section(ECategoryUpdate, "e__category_create", options, [cat(u"53", u"-1.8", u"A / B")], storage)
        self.assertEqual(next(iter(bp))[u"title"], u"A / B")
        node = get_categories(self.portal)[u"-1.8"]["obj"]
        self.assertEqual(aq_parent(node).getPhysicalPath(), self.portal.tree.getPhysicalPath())
        # not decimal codes: parent relation
        e_category_parent = {u"A1": {u"_parent_id": u"-1"}, u"A2": {u"_parent_id": u"ZZ"}}
        storage = self.make_storage("e", e_category={}, e_category_parent=e_category_parent)
        options = dict(
            self.options2, b_condition="python: True", decimal_import="0", parent_relation="e_category_parent"
        )
        items = [cat(u"54", u"A1", u"A un"), cat(u"55", u"A2", u"A deux"), cat(u"56", u"A3", u"A trois")]
        bp = self.section(ECategoryUpdate, "e__category_create", options, items, storage)
        self.assertListEqual([item[u"_pcode"] for item in bp], [u"A1", u"A2", u"A3"])
        tree = get_categories(self.portal)
        self.assertEqual(aq_parent(tree[u"A1"]["obj"]).identifier, u"-1")
        self.assertEqual(aq_parent(tree[u"A2"]["obj"]).getPhysicalPath(), self.portal.tree.getPhysicalPath())
        self.assertEqual(aq_parent(tree[u"A3"]["obj"]).getPhysicalPath(), self.portal.tree.getPhysicalPath())
        self.assertTrue(items[1][u"_error"])
        self.assertIn(
            ("dti", "ERROR", u"e_category: _eid 55, The parent 'ZZ' is not in the created categories."),
            self.logs.records,
        )
        # category code eid: e_category and parent relation stored by eid
        storage = self.make_storage(
            "e",
            e_category={u"61": {u"_ecode": u"-1.7"}},
            e_category_code={u"-1.7": {}},
            e_category_parent={u"60": {u"_parent_id": u"61"}},
        )
        options = dict(
            self.options2,
            b_condition="python: True",
            decimal_import="0",
            parent_relation="e_category_parent",
            category_code_eid="e_category_code",
        )
        bp = self.section(ECategoryUpdate, "e__category_create", options, [cat(u"60", u"B1", u"B un")], storage)
        list(bp)
        self.assertEqual(aq_parent(get_categories(self.portal)[u"B1"]["obj"]).identifier, u"-1.7")
        # part not run
        bp = self.section(
            ECategoryUpdate, "e__category_create", options, [cat(u"62", u"B2", u"B")], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [cat(u"62", u"B2", u"B")])


class TestF1Level3Handler(DmsTestCase):

    # data-transfer.cfg.1 f__1_level3_handler
    options = {"condition": "python: item['_level'] == 3"}

    def test___init__(self):
        bp = self.section(F1Level3Handler, "f__1_level3_handler", self.options, storage=self.make_storage("a"))
        self.assertEqual(bp.parts, "f")
        self.assertTrue(bp.condition({u"_level": 3}))  # always parsed
        self.assertFalse(bp.condition({u"_level": 2}))

    def test___iter__(self):
        storage = self.make_storage(
            "f",
            e_f_parent_relation={u"3": {u"_parent_id": u"2"}, u"2": {u"_parent_id": u"1"}},
            e_folder={u"3": {u"title": u"Sous-dossier"}},
        )
        items = [
            {u"_bpk": u"e_folder", u"_eid": u"4", u"_parent_id": u"3", u"title": u"Pièces", u"_level": 3},
            {u"_bpk": u"e_folder", u"_eid": u"3", u"_parent_id": u"2", u"title": u"Sous-dossier", u"_level": 2},
        ]
        bp = self.section(F1Level3Handler, "f__1_level3_handler", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_folder",
                    u"_eid": u"4",
                    u"_parent_id": u"2",
                    u"title": u"Sous-dossier | Pièces",
                    u"_level": 3,
                    u"_type": "ClassificationSubfolder",
                    u"_error": True,
                },
                {u"_bpk": u"e_folder", u"_eid": u"3", u"_parent_id": u"2", u"title": u"Sous-dossier", u"_level": 2},
            ],
        )
        self.assertIn(("dti", "ERROR", u"e_folder: _eid 4, Modifying level3 folder"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"f__1_level3_handler": 1})
        # part not run
        item = {u"_bpk": u"e_folder", u"_eid": u"4", u"_parent_id": u"3", u"title": u"Pièces", u"_level": 3}
        bp = self.section(F1Level3Handler, "f__1_level3_handler", self.options, [item], self.make_storage("a"))
        self.assertListEqual(
            list(bp), [{u"_bpk": u"e_folder", u"_eid": u"4", u"_parent_id": u"3", u"title": u"Pièces", u"_level": 3}]
        )


class TestF2FolderSubfolderSplit(DmsTestCase):

    # data-transfer.cfg.2 f__2_folder_subfolder
    options = {
        "condition": "python: item.get('_bpk') == u'e_folder' and item.get('_s__eid')",
        "bp_key": "e_folder_subfolder",
        "prefix": "_s_",
    }

    def test___init__(self):
        bp = self.section(F2FolderSubfolderSplit, "f__2_folder_subfolder", self.options, storage=self.make_storage("a"))
        self.assertEqual(bp.bp_key, u"e_folder_subfolder")
        self.assertEqual(bp.prefix, u"_s_")
        self.assertIsInstance(bp.prefix, six.text_type)

    def test___iter__(self):
        storage = self.make_storage("f")
        items = [
            {
                u"_bpk": u"e_folder",
                u"_eid": u"1",
                u"title": u"Dossier",
                u"_s__eid": u"1-1",
                u"_s_title": u"Farde",
                u"_s__parent_id": u"1",
            },
            {u"_bpk": u"e_folder", u"_eid": u"2", u"title": u"Dossier 2", u"_s__eid": None},
        ]
        bp = self.section(F2FolderSubfolderSplit, "f__2_folder_subfolder", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_folder",
                    u"_eid": u"1",
                    u"title": u"Dossier",
                    u"_s__eid": u"1-1",
                    u"_s_title": u"Farde",
                    u"_s__parent_id": u"1",
                },
                # prefixed keys are kept, bp_key is not used
                {
                    u"_bpk": u"e_folder",
                    u"_eid": u"1-1",
                    u"title": u"Farde",
                    u"_parent_id": u"1",
                    u"_s__eid": u"1-1",
                    u"_s_title": u"Farde",
                    u"_s__parent_id": u"1",
                },
                {u"_bpk": u"e_folder", u"_eid": u"2", u"title": u"Dossier 2", u"_s__eid": None},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"f__2_folder_subfolder": 1})
        # part not run
        bp = self.section(
            F2FolderSubfolderSplit, "f__2_folder_subfolder", self.options, items[:1], self.make_storage("a")
        )
        self.assertEqual(len(list(bp)), 1)


class TestF3TextCategoriesToId(DmsTestCase):

    # data-transfer.cfg.2 f__3_category_id_insert
    options = {
        "condition": "python: item.get('_bpk') == u'e_folder' and item['_category_id']",
        "bp_key": "p_category",
        "categories_field_name": "_category_id",
        "txt_field_name": "classification_informations",
        "separator": "/",
        "part_pattern": r"^-[.\d]+$",
    }

    def test___init__(self):
        storage = self.make_storage("a")
        bp = self.section(F3TextCategoriesToId, "f__3_category_id_insert", self.options, storage=storage)
        self.assertIs(bp.categories, storage["data"]["p_category"])  # read even if the part is not run
        self.assertEqual(bp.cat_fld_name, "_category_id")
        self.assertEqual(bp.fld_name, u"classification_informations")
        self.assertEqual(bp.sep, u"/")
        self.assertEqual(bp.pattern, r"^-[.\d]+$")
        options = {"bp_key": "p_category", "txt_field_name": "data_transfer"}
        bp = self.section(F3TextCategoriesToId, "l__3_category_id_insert", options, storage=storage)
        self.assertEqual(bp.cat_fld_name, "_classification_categories_text")
        self.assertEqual(bp.sep, u"")
        self.assertEqual(bp.pattern, u"")

    def test___iter__(self):
        storage = self.make_storage("f")
        cats = storage["data"]["p_category"]
        storage["plone"]["def_category"] = cats[u"-1"]["uid"]

        def folder(eid, cat_id, **kw):
            dic = {u"_bpk": u"e_folder", u"_eid": eid, u"_category_id": cat_id}
            dic.update(kw)
            return dic

        items = [
            folder(u"1", u"-1.7/ -1.75"),
            folder(u"2", u"-1.7/-9.9", classification_informations=u"Info"),
            folder(u"3", u"-1.7/abc"),  # not matching pattern: whole text
            folder(u"4", u"-1.753"),
            folder(u"5", None),
        ]
        bp = self.section(F3TextCategoriesToId, "f__3_category_id_insert", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                folder(u"1", u"-1.7/ -1.75", classification_categories=[cats[u"-1.7"]["uid"], cats[u"-1.75"]["uid"]]),
                folder(
                    u"2",
                    u"-1.7/-9.9",
                    classification_categories=[cats[u"-1"]["uid"]],
                    _error=True,
                    classification_informations=u"Info\r\nCLASSEMENT: -1.7/-9.9",
                ),
                folder(
                    u"3",
                    u"-1.7/abc",
                    classification_categories=[cats[u"-1"]["uid"]],
                    _error=True,
                    classification_informations=u"CLASSEMENT: -1.7/abc",
                ),
                folder(u"4", u"-1.753", classification_categories=[cats[u"-1.753"]["uid"]]),
                folder(u"5", None),
            ],
        )
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", u"e_folder: _eid 2, Category part '-9.9' not found ('-1.7/-9.9')"),
                ("dti", "ERROR", u"e_folder: _eid 3, Category part '-1.7/abc' not found ('-1.7/abc')"),
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"f__3_category_id_insert": 4})
        # without separator
        options = dict(self.options)
        del options["separator"]
        bp = self.section(
            F3TextCategoriesToId, "f__3_category_id_insert", options, [folder(u"1", u"-1.7/-1.75")], storage
        )
        self.assertListEqual(next(iter(bp))[u"classification_categories"], [cats[u"-1"]["uid"]])
        # part not run
        bp = self.section(
            F3TextCategoriesToId,
            "f__3_category_id_insert",
            self.options,
            [folder(u"1", u"-1.7")],
            self.make_storage("a"),
        )
        self.assertListEqual(list(bp), [folder(u"1", u"-1.7")])


class TestHContactTypeUpdate(DmsTestCase):

    # data-transfer.cfg.1 h__contact_type_update
    options = {"condition": "python: item['_bpk'] == u'e_contact_type_match'", "clean_unused": "1"}

    def h_storage(self):
        storage = self.make_storage("h")
        directory = storage["plone"]["directory"]
        for typ in ("types", "levels"):  # as the Initialization section
            key = "p_dir_org_{}".format(typ)
            storage["data"][key] = OrderedDict(
                [
                    (safe_unicode(t["token"]), {"name": safe_unicode(t["name"])})
                    for t in getattr(directory, "organization_%s" % typ)
                ]
            )
            storage["data"]["{}_len".format(key)] = len(storage["data"][key])
        return storage

    def test___init__(self):
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", self.options, storage=self.make_storage("a"))
        self.assertDictEqual(bp.new_val, {})
        self.assertFalse(hasattr(bp, "clean_unused"))
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", self.options, storage=self.h_storage())
        self.assertTrue(bp.clean_unused)
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", {}, storage=self.h_storage())
        self.assertFalse(bp.clean_unused)

    def test___iter__(self):
        directory = self.portal["contacts"]
        types_before = [dict(dic) for dic in directory.organization_types]
        self.assertListEqual(
            [dic["token"] for dic in types_before],
            [u"non-defini", u"sa", u"commune", u"cpas", u"intercommunale", u"zp", u"zs"],
        )

        def match(eid, pid, ptitle):
            return {
                u"_bpk": u"e_contact_type_match",
                u"_eid": eid,
                u"_etitle": ptitle,
                u"_pid": pid,
                u"_ptitle": ptitle,
            }

        items = [
            match(u"1", u"commune", u"Commune"),
            match(u"2", u"asbl", u"ASBL"),
            match(u"3", u"", u""),
            match(u"4", u"sa", u"Société anonyme"),
            {u"_bpk": u"e_contact", u"_eid": u"5"},
        ]
        storage = self.h_storage()
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", self.options, items, storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_contact", u"_eid": u"5"}])
        self.assertTrue(items[2][u"_error"])
        self.assertIn(("dti", "ERROR", u"e_contact_type_match: _eid 3, Empty match: we pass _eid 3"), self.logs.records)
        # unused types are removed, except non-defini
        self.assertListEqual(
            [dict(dic) for dic in directory.organization_types],
            [
                {u"token": u"non-defini", u"name": u"Non défini"},
                {u"token": u"sa", u"name": u"Société anonyme"},
                {u"token": u"commune", u"name": u"Commune"},
                {u"token": u"asbl", u"name": u"ASBL"},
            ],
        )
        self.assertListEqual(list(storage["data"]["p_dir_org_types"]), [u"non-defini", u"sa", u"commune", u"asbl"])
        self.assertDictEqual(dict(storage["course"]), {"h__contact_type_update": 4})
        # without cleaning
        directory.organization_types = types_before
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", {}, items[1:2], self.h_storage())
        list(bp)
        self.assertListEqual(
            [dict(dic) for dic in directory.organization_types], types_before + [{u"token": u"asbl", u"name": u"ASBL"}]
        )
        # Plone 4 behaviour (bug?): no new type, the directory is not updated (renamed or unused types)
        directory.organization_types = types_before
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", self.options, items[3:4], self.h_storage())
        list(bp)
        self.assertListEqual([dict(dic) for dic in directory.organization_types], types_before)


class TestI1ContactUpdate(DmsTestCase):
    def test___init__(self):
        bp = self.section(I1ContactUpdate, "i__1_contact_update", {}, storage=self.make_storage("a"))
        self.assertEqual(bp.parts, "i")
        self.assertFalse(hasattr(bp, "condition"))
        bp = self.section(I1ContactUpdate, "i__1_contact_update", {}, storage=self.make_storage("i"))
        self.assertTrue(bp.condition({}))

    def test___iter__(self):
        def address(**kw):
            dic = {
                u"_pc": u"",
                u"_city": u"",
                u"_number": u"",
                u"_box": u"",
                u"_street": u"",
                u"_imp_addr": u"",
                u"_imp_pc": u"",
                u"_imp_city": u"",
                u"_imp_country": u"",
                u"_phone": u"",
                u"_cell": u"",
                u"_fax": u"",
                u"_website": u"",
                u"_email": u"",
            }
            dic.update(kw)
            return dic

        def contact(eid, **kw):
            dic = {
                u"_bpk": u"e_contact",
                u"_eid": eid,
                u"lastname": u"Dupont",
                u"firstname": u"Jean",
                u"_ptitle": None,
                u"_name2": None,
            }
            dic.update(kw)
            return dic

        e_address = {
            u"A1": address(
                _pc=u"7340",
                _city=u"Colfontaine",
                _number=u"12",
                _box=u"b",
                _street=u"Rue Haute",
                _imp_addr=u"Rue Haute 12b",
                _imp_country=u"France",
                _phone=u"065/1",
                _cell=u"0475/1",
                _fax=u"065/2",
                _website=u"www.dupont.be",
                _email=u"j@dupont.be",
            ),
            u"A2": address(
                _pc=u"7000", _city=u"Mons", _box=u"3", _imp_addr=u"Grand-Place\n 1 ", _imp_country=u"Belgique"
            ),
            u"A3": address(_imp_addr=u"Rue X 1", _imp_pc=u"1000", _imp_city=u"Bruxelles"),
        }
        items = [
            contact(u"1", _addr_id=u"A1"),
            contact(u"2", _addr_id=u"A2"),
            contact(u"3", _addr_id=u"A3", _phone2=u"02/1", _cell3=u"0476/1", _email1=u"x@y.be", _web=u"www.y.be"),
            contact(
                u"4",
                _street=u"Rue Basse\n2",
                _street_nb=u"5",
                _street_nb_box=u"a",
                _pc=u"7340",
                _city=u"Colfontaine",
                _country=u"belgique",
                _fax=u"065/3",
            ),
            contact(u"5", _street_nb_box=u"a", _country=u"France"),
            contact(u"6", _parent_id=u"1", lastname=u"", _ptitle=u"Directeur"),
            contact(u"7", lastname=u"", _name2=u"Service travaux"),
            {u"_bpk": u"e_service", u"_eid": u"8"},
        ]
        storage = self.make_storage("i", e_address=e_address)
        options = {"condition": "python: item['_bpk'] == u'e_contact'"}
        bp = self.section(I1ContactUpdate, "i__1_contact_update", options, items, storage)
        empty = {u"phone": None, u"cell_phone": None, u"email": None, u"use_parent_address": False}
        self.assertListEqual(
            list(bp),
            [
                contact(
                    u"1",
                    _addr_id=u"A1",
                    zip_code=u"7340",
                    city=u"Colfontaine",
                    number=u"12 bte b",
                    street=u"Rue Haute",
                    country=u"France",
                    phone=u"065/1",
                    cell_phone=u"0475/1",
                    fax=u"065/2",
                    website=u"www.dupont.be",
                    email=u"j@dupont.be",
                    use_parent_address=False,
                ),
                contact(
                    u"2", _addr_id=u"A2", zip_code=u"7000", city=u"Mons", number=u"3", street=u"Grand-Place, 1", **empty
                ),
                contact(
                    u"3",
                    _addr_id=u"A3",
                    _phone2=u"02/1",
                    _cell3=u"0476/1",
                    _email1=u"x@y.be",
                    _web=u"www.y.be",
                    street=u"Rue X 1",
                    zip_code=u"1000",
                    city=u"Bruxelles",
                    phone=u"02/1",
                    cell_phone=u"0476/1",
                    email=u"x@y.be",
                    website=u"www.y.be",
                    use_parent_address=False,
                ),
                contact(
                    u"4",
                    _street=u"Rue Basse\n2",
                    _street_nb=u"5",
                    _street_nb_box=u"a",
                    _pc=u"7340",
                    _city=u"Colfontaine",
                    _country=u"belgique",
                    _fax=u"065/3",
                    street=u"Rue Basse, 2",
                    number=u"5 a",
                    zip_code=u"7340",
                    city=u"Colfontaine",
                    fax=u"065/3",
                    **empty
                ),
                # Plone 4 behaviour (bug?): box without number gives a leading space
                contact(u"5", _street_nb_box=u"a", _country=u"France", number=u" a", country=u"France", **empty),
                # no address: parent address, empty lastname replaced by title
                dict(
                    contact(u"6", _parent_id=u"1", lastname=u"Directeur", _ptitle=None),
                    **dict(empty, use_parent_address=True)
                ),
                contact(u"7", lastname=u"Service travaux", _name2=None, **empty),
                {u"_bpk": u"e_service", u"_eid": u"8"},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"i__1_contact_update": 7})
        # part not run
        bp = self.section(I1ContactUpdate, "i__1_contact_update", options, [contact(u"1")], self.make_storage("a"))
        self.assertListEqual(list(bp), [contact(u"1")])


class TestI2ContactUpdate(DmsTestCase):

    # data-transfer.cfg.2 i__2_contact_update
    options = {"paths_key": "e_contact_path", "plonegroup_org_id": "-3"}

    def i_storage(self, **data):
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        dic = {
            "e_contact": {},
            "e_contact_path": {},
            "e_contact_i": {},
            "e_user_match": {u"Agent_Fred": {u"_p_userid": u"agent"}, u"Lecteur_Jef": {u"_p_userid": u"unknown"}},
            "e_service_match": {u"S5": {u"uid": secr_uid}},
        }
        dic.update(data)
        return self.make_storage("i", **dic)

    def test___init__(self):
        storage = self.make_storage("a")
        bp = self.section(I2ContactUpdate, "i__2_contact_update", self.options, storage=storage)
        self.assertFalse(hasattr(bp, "plonegroup_org_id"))
        storage = self.i_storage()
        bp = self.section(I2ContactUpdate, "i__2_contact_update", self.options, storage=storage)
        self.assertIs(bp.contacts, storage["data"]["e_contact"])
        self.assertIs(bp.eids, storage["data"]["e_contact_path"])
        self.assertEqual(bp.plonegroup_org_id, "-3")
        self.assertIs(bp.e_u_m, storage["data"]["e_user_match"])
        self.assertIs(bp.done, storage["data"]["e_contact_i"])
        # the plonegroup organization is added in the service matching
        self.assertDictEqual(storage["data"]["e_service_match"]["-3"], {"uid": self.uid(PLONEGROUP)})
        storage = self.make_storage("i", e_contact={}, e_contact_path={}, e_contact_i={})
        bp = self.section(I2ContactUpdate, "i__2_contact_update", self.options, storage=storage)
        self.assertDictEqual(bp.e_u_m, {})
        self.assertDictEqual(bp.e_s_m, {"-3": {"uid": self.uid(PLONEGROUP)}})

    def test___iter__(self):  # noqa C901
        e_contact = {
            u"S5": {u"_organization": u"Secrétariat", u"_type": u"organization"},
            u"12": {u"_organization": u"Commune", u"_type": u"organization"},
            u"40": {u"lastname": u"Durand", u"_pc": u"7000", u"_city": u"Mons", u"_street": u"Rue Neuve"},
        }
        storage = self.i_storage(e_contact=e_contact)
        cdate = storage["creation_date"]

        def org(eid, title, division=u"", **kw):
            dic = {
                u"_bpk": u"e_contact",
                u"_eid": eid,
                u"_organization": title,
                u"_division": division,
                u"_service": u"",
                u"_pc": u"7340",
                u"_city": u"Colfontaine",
                u"_street": u"Rue Haute",
                u"_email1": u"info@commune.be",
                u"_phone1": u"065/1",
                u"_phone2": u"065/2",
            }
            dic.update(kw)
            return dic

        def pers(eid, parent_id, lastname, firstname, **kw):
            dic = {
                u"_bpk": u"e_contact",
                u"_eid": eid,
                u"_parent_id": parent_id,
                u"lastname": lastname,
                u"firstname": firstname,
                u"_cell1": u"0475/1",
                u"_phone1": u"065/3",
                u"_phone2": None,
                u"_email1": u"p@x.be",
            }
            dic.update(kw)
            return dic

        new = {u"use_parent_address": False, u"creation_date": cdate, u"modification_date": cdate}
        items = [
            org(u"S5", u"Secrétariat"),  # internal organization
            org(u"12", u"Commune"),
            org(u"13", u"Commune", u"Travaux", _service=u"Voiries"),
            org(u"14", u""),  # just an address: passed
            pers(u"30", u"12", u"Dupont", u"Jean"),  # held position in an external organization
            pers(u"31", u"S5", u"Agent", u"Fred"),  # existing internal held position
            pers(u"32", u"S5", u"Lecteur", u"Jef"),  # new internal person
            pers(u"33", u"40", u"Durand", u"Marie"),  # person with a person parent
            pers(u"34", None, u"Martin", u"Paul"),
            {u"_bpk": u"e_service", u"_eid": u"S6"},
        ]
        options = dict(self.options, condition="python: item['_bpk'] == u'e_contact'")
        bp = self.section(I2ContactUpdate, "i__2_contact_update", options, items, storage)
        gen = iter(bp)
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk=u"e_contact",
                _eid=u"12",
                _division=u"",
                _service=u"",
                _id=u"12",
                _type="organization",
                _parenth=u"/contacts",
                internal_number=u"12",
                zip_code=u"7340",
                city=u"Colfontaine",
                street=u"Rue Haute",
                title=u"Commune",
                email=u"info@commune.be",
                phone=u"065/1",
                fax=u"065/2",
            ),
        )
        self.assertEqual(
            storage["data"]["e_contact_path"][u"S5"], {"path": PLONEGROUP + "/direction-generale/secretariat"}
        )
        self.assertIn(("dti", "INFO", u"Passed internal organization: S5"), self.logs.records)
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk=u"e_contact",
                _eid=u"13",
                _organization=u"Commune",
                _division=u"Travaux",
                _service=u"Voiries",
                _id=u"13",
                _type="organization",
                _parenth=u"/contacts",
                internal_number=u"13",
                zip_code=u"7340",
                city=u"Colfontaine",
                street=u"Rue Haute",
                title=u"Commune - Travaux - Voiries",
                email=u"info@commune.be",
                phone=u"065/1",
                fax=u"065/2",
            ),
        )
        # the organization "12" has been created by the next sections (an existing organization is used here)
        storage["data"]["e_contact_path"][u"12"] = {"path": "/contacts/electrabel"}
        pdic = next(gen)
        self.assertDictEqual(
            pdic,
            {
                "_bpk": u"e_contact",
                "_eid": u"30a",
                "_type": "person",
                "lastname": u"Dupont",
                "firstname": u"Jean",
                "_parenth": u"/contacts",
            },
        )
        pdic["_path"] = u"/contacts/30a"  # set by the next sections (path_insert)
        hp = next(gen)
        self.assertEqual(hp.pop(u"position").to_id, self.iid("/contacts/electrabel"))
        self.assertDictEqual(
            hp,
            dict(
                new,
                _bpk=u"e_contact",
                _eid=u"30",
                _parent_id=u"12",
                lastname=u"Dupont",
                firstname=u"Jean",
                _type="held_position",
                _parenth=u"/contacts/30a",
                use_parent_address=True,
                _is_user=False,
                cell_phone=u"0475/1",
                phone=u"065/3",
                fax=None,
                email=u"p@x.be",
                _id=u"30",
                internal_number=u"30",
            ),
        )
        # existing internal person and held position: nothing yielded
        pdic = next(gen)
        self.assertDictEqual(
            pdic,
            {
                "_bpk": u"e_contact",
                "_eid": u"32a",
                "_type": "person",
                "lastname": u"Lecteur",
                "firstname": u"Jef",
                "_parenth": u"/contacts/personnel-folder",
            },
        )
        self.assertDictEqual(
            storage["data"]["e_contact_path"][u"31"],
            {"path": "/contacts/personnel-folder/agent/agent-secretariat", "_is_user": True},
        )
        self.assertIn(("dti", "INFO", u"Passed internal person: 31"), self.logs.records)
        self.assertIn(("dti", "INFO", u"Passed internal hp: 31"), self.logs.records)
        pdic["_path"] = u"/contacts/personnel-folder/32a"
        hp = next(gen)
        self.assertEqual(hp.pop(u"position").to_id, self.iid(PLONEGROUP + "/direction-generale/secretariat"))
        self.assertEqual(
            (hp[u"_type"], hp[u"_parenth"], hp[u"_is_user"]), ("held_position", u"/contacts/personnel-folder/32a", True)
        )
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk=u"e_contact",
                _eid=u"33",
                _parent_id=u"40",
                lastname=u"Durand",
                firstname=u"Marie",
                _type="person",
                _parenth=u"/contacts",
                _is_user=False,
                zip_code=u"7000",
                city=u"Mons",
                street=u"Rue Neuve",
                cell_phone=u"0475/1",
                phone=u"065/3",
                fax=None,
                email=u"p@x.be",
                _id=u"33",
                internal_number=u"33",
            ),
        )
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk=u"e_contact",
                _eid=u"34",
                _parent_id=None,
                lastname=u"Martin",
                firstname=u"Paul",
                _type="person",
                _parenth=u"/contacts",
                _is_user=False,
                cell_phone=u"0475/1",
                phone=u"065/3",
                fax=None,
                email=u"p@x.be",
                _id=u"34",
                internal_number=u"34",
            ),
        )
        self.assertListEqual(list(gen), [{u"_bpk": u"e_service", u"_eid": u"S6"}])
        self.assertListEqual(
            sorted(storage["data"]["e_contact_i"]),
            sorted([u"S5", u"12", u"13", u"14", u"30", u"31", u"32", u"33", u"34"]),
        )
        self.assertDictEqual(dict(storage["course"]), {"i__2_contact_update": 9})
        # part not run
        bp = self.section(
            I2ContactUpdate, "i__2_contact_update", options, [org(u"S5", u"Secrétariat")], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [org(u"S5", u"Secrétariat")])


class TestJ2ContactUpdate(DmsTestCase):

    # data-transfer.cfg.2 j__2_contact_update
    options = {"paths_key": "e_contact_path"}

    def test___init__(self):
        bp = self.section(J2ContactUpdate, "j__2_contact_update", self.options, storage=self.make_storage("a"))
        self.assertFalse(hasattr(bp, "eids"))
        storage = self.make_storage("j", e_contact_path={}, e_contact_j={})
        bp = self.section(J2ContactUpdate, "j__2_contact_update", self.options, storage=storage)
        self.assertIs(bp.eids, storage["data"]["e_contact_path"])
        self.assertIs(bp.p_hps, storage["data"]["p_hps"])
        self.assertIs(bp.done, storage["data"]["e_contact_j"])

    def test___iter__(self):
        def contact(eid, o_eid=None, o_title=None, **kw):
            dic = {
                u"_bpk": u"e_contact_c",
                u"_eid": eid,
                u"lastname": u"Dupont",
                u"firstname": u"Jean",
                u"_function": u"Directeur",
                u"person_title": u"Monsieur",
                u"phone": u"065/1",
                u"_phone2": None,
                u"fax": u"",
                u"email": u"jean@dupont.be",
                u"cell_phone": None,
                u"_p_enabled": u"1",
                u"_o_eid": o_eid,
                u"_o_title": o_title,
                u"street": u"Rue Haute",
                u"number": u"5",
                u"_box": u"b",
                u"zip_code": u"7340",
                u"city": u"Colfontaine",
                u"_o_phone": u"065/0",
                u"_o_fax": None,
                u"_o_email": u"info@commune.be",
                u"enterprise_number": u"0123",
                u"_o_enabled": u"1",
                u"country": u"Belgique",
            }
            dic.update(kw)
            return dic

        eids = {
            u"12": {"path": u"/contacts/electrabel"},
            u"34": {"path": u"/contacts/jeancourant/agent-electrabel"},
            u"36": {"path": u"/contacts/sergerobinet"},
        }
        storage = self.make_storage("j", e_contact_path=eids, e_contact_j={})
        items = [
            contact(u"34", u"12", u"Commune"),
            contact(u"35", u"99", u"Unknown"),  # organization not found
            contact(u"33", u"12", u"Commune", country=u"France"),  # held position not found
            contact(u"36"),  # person
            contact(u"37"),  # person not found
            {u"_bpk": u"e_contact", u"_eid": u"38"},
        ]
        options = dict(self.options, condition="python: item['_bpk'] == u'e_contact_c'")
        bp = self.section(J2ContactUpdate, "j__2_contact_update", options, items, storage)
        base = {
            "_bpk": u"e_contact_c",
            "street": u"Rue Haute",
            "number": u"5 b",
            "city": u"Colfontaine",
            "zip_code": u"7340",
            "country": u"",
        }
        org = dict(
            base,
            _eid=u"12",
            _type="organization",
            title=u"Commune",
            enterprise_number=u"0123",
            phone=u"065/0",
            email=u"info@commune.be",
            _path=u"/contacts/electrabel",
        )
        self.assertListEqual(
            list(bp),
            [
                org,
                # Plone 4 behaviour (bug?): updated held position and person have no _path
                dict(
                    base,
                    _eid=u"34",
                    _type="held_position",
                    label=u"Directeur",
                    phone=u"065/1",
                    fax=u"",
                    email=u"jean@dupont.be",
                ),
                dict(org, country=u"France"),
                dict(base, _eid=u"36", _type="person", phone=u"065/1", fax=u"", email=u"jean@dupont.be"),
                {u"_bpk": u"e_contact", u"_eid": u"38"},
            ],
        )
        # Plone 4 behaviour (bug?): the path is stored as a tuple on the original item
        self.assertEqual(items[0][u"_path"], (u"/contacts/jeancourant/agent-electrabel",))
        self.assertEqual(items[3][u"_path"], (u"/contacts/sergerobinet",))
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", u"e_contact_c: _eid 35, Organization not found: 99"),
                ("dti", "ERROR", u"e_contact_c: _eid 33, HP not found: 33"),
                ("dti", "ERROR", u"e_contact_c: _eid 37, Person not found: 37"),
            ],
        )
        self.assertListEqual(sorted(storage["data"]["e_contact_j"]), [u"33", u"34", u"35", u"36", u"37"])
        self.assertDictEqual(dict(storage["course"]), {"j__2_contact_update": 5})
        # part not run
        bp = self.section(J2ContactUpdate, "j__2_contact_update", options, items[3:4], self.make_storage("a"))
        self.assertEqual(len(list(bp)), 1)


class TestL1RecipientGroupsSet(DmsTestCase):

    # data-transfer.cfg.1 l__1_recipient_groups_set
    options = {
        "condition": "python: item.get('_bpk') == u'e_mail_i'",
        "global_recipient_service": "Reprise de données",
        "global_recipient_tg_exceptions": "",
    }

    def add_generic_service(self):
        """As the Initialization section does it (config generic_service)."""
        obj = api.content.create(
            container=self.obj(PLONEGROUP), type="organization", id="reprise-de-donnees", title=u"Reprise de données"
        )
        set_registry_organizations(get_registry_organizations() + [obj.UID()])
        return obj

    def test___init__(self):
        # Plone 4 behaviour (bug?): log_error is called with an empty dict, raising a KeyError on '_eid'
        self.assertRaises(
            KeyError,
            self.section,
            L1RecipientGroupsSet,
            "l__1_recipient_groups_set",
            self.options,
            storage=self.make_storage("l"),
        )
        grs = self.add_generic_service()
        bp = self.section(
            L1RecipientGroupsSet, "l__1_recipient_groups_set", self.options, storage=self.make_storage("a")
        )
        self.assertEqual(bp.grs_title, u"Reprise de données")
        self.assertIsInstance(bp.grs_title, six.text_type)
        self.assertEqual(bp.grs_uid, grs.UID())  # no part check
        self.assertListEqual(bp.gr_tg_exc, [])
        options = dict(self.options, global_recipient_tg_exceptions="333 Personnel 336 DirecteurFinancier")
        bp = self.section(L1RecipientGroupsSet, "p__1_recipient_groups_set", options, storage=self.make_storage("p"))
        self.assertListEqual(bp.gr_tg_exc, [u"333", u"336"])
        options = dict(self.options, global_recipient_tg_exceptions="333 Personnel 336")
        self.assertRaises(
            ValueError,
            self.section,
            L1RecipientGroupsSet,
            "l__1_recipient_groups_set",
            options,
            storage=self.make_storage("l"),
        )

    def test___iter__(self):
        grs_uid = self.add_generic_service().UID()
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        options = dict(self.options, global_recipient_tg_exceptions="333 Personnel")
        items = [
            {u"_bpk": u"e_mail_i", u"_eid": u"1", u"_service_id": u"10"},
            {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_service_id": u"11", u"recipient_groups": [secr_uid]},
            {u"_bpk": u"e_mail_i", u"_eid": u"3", u"_service_id": u"333"},
            {u"_bpk": u"e_mail_i", u"_eid": u"4", u"_service_id": u"12", u"recipient_groups": [grs_uid]},
            {u"_bpk": u"e_mail_o", u"_eid": u"5", u"_service_id": u"10"},
        ]
        storage = self.make_storage("l")
        bp = self.section(L1RecipientGroupsSet, "l__1_recipient_groups_set", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {u"_bpk": u"e_mail_i", u"_eid": u"1", u"_service_id": u"10", u"recipient_groups": [grs_uid]},
                {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_service_id": u"11", u"recipient_groups": [secr_uid, grs_uid]},
                {u"_bpk": u"e_mail_i", u"_eid": u"3", u"_service_id": u"333"},
                {u"_bpk": u"e_mail_i", u"_eid": u"4", u"_service_id": u"12", u"recipient_groups": [grs_uid]},
                {u"_bpk": u"e_mail_o", u"_eid": u"5", u"_service_id": u"10"},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"l__1_recipient_groups_set": 4})


class TestL1SenderAsTextSet(DmsTestCase):

    # data-transfer.cfg.1 l__1_sender_as_text_set
    options = {"condition": "python: item.get('_bpk') == u'e_mail_i'"}

    def test___init__(self):
        storage = self.make_storage("a", e_contact_path={u"7": {}})
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, storage=storage)
        self.assertEqual(bp.eid_key, "_sender_id")
        self.assertIs(bp.eids, storage["data"]["e_contact_path"])  # no part check
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, storage=self.make_storage("l"))
        self.assertIsNone(bp.eids)

    def test___iter__(self):
        e_contact = TestContactAsTextUpdate.e_contact
        storage = self.make_storage(
            "l", e_contact_path={u"7": {u"path": u"/contacts/jeancourant"}}, e_contact=e_contact
        )
        items = [
            {u"_bpk": u"e_mail_i", u"_eid": u"1", u"_sender_id": u"8", u"_sender": u"Lettre de M. Dupont"},
            {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_sender_id": u"7", u"_sender": None, u"description": u"Desc"},
            {
                u"_bpk": u"e_mail_i",
                u"_eid": u"3",
                u"_sender_id": None,
                u"_sender": u"Anonyme\nRue X",
                u"data_transfer": u"DT",
            },
            {u"_bpk": u"e_mail_o", u"_eid": u"4", u"_sender_id": u"8", u"_sender": None},
        ]
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_mail_i",
                    u"_eid": u"1",
                    u"_sender_id": u"8",
                    u"_sender": u"Lettre de M. Dupont",
                    u"description": u"EXPÉDITEUR: Dupont Jean, jean@dupont.be.\r\nEXPÉDITEUR LIBRE: Lettre de M. Dupont",
                    u"data_transfer": u"EXPÉDITEUR: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE EXPÉDITEUR: Rue Haute 7340 Colfontaine.\r\nTÉL EXPÉDITEUR: 065/123456.",
                },
                # contact created as object: no text
                {u"_bpk": u"e_mail_i", u"_eid": u"2", u"_sender_id": u"7", u"_sender": None, u"description": u"Desc"},
                {
                    u"_bpk": u"e_mail_i",
                    u"_eid": u"3",
                    u"_sender_id": None,
                    u"_sender": u"Anonyme\nRue X",
                    u"description": u"EXPÉDITEUR LIBRE: Anonyme",
                    u"data_transfer": u"DT\r\nEXPÉDITEUR LIBRE: Anonyme, Rue X",
                },
                {u"_bpk": u"e_mail_o", u"_eid": u"4", u"_sender_id": u"8", u"_sender": None},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"l__1_sender_as_text_set": 3})
        # no data_transfer field: everything in description
        storage["desc_only"] = True
        items = [
            {
                u"_bpk": u"e_mail_i",
                u"_eid": u"3",
                u"_sender_id": None,
                u"_sender": u"Anonyme\nRue X",
                u"data_transfer": u"DT",
            }
        ]
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_mail_i",
                    u"_eid": u"3",
                    u"_sender_id": None,
                    u"_sender": u"Anonyme\nRue X",
                    u"data_transfer": u"DT",
                    u"description": u"EXPÉDITEUR LIBRE: Anonyme, Rue X",
                }
            ],
        )


class TestM1AssignedUserHandling(DmsTestCase):

    # data-transfer.cfg.1 m__1_assigned_user
    options = {"condition": "python: item.get('_bpk') == u'e_mail_recip_i'", "store_key": "e_mail_i"}
    # data-transfer.cfg.2 p__2_assigned_user
    options2 = {
        "condition": "python: item.get('_bpk') == u'e_mail_o'",
        "original_item": "1",
        "contact_sender_key": "e_user_match",
        "store_key": "e_mail_o",
        "functions": "OM",
        "contact_id_key": "_sender_id",
    }

    def m_storage(self, parts="m", **data):
        e_user_match = {
            u"u1": {u"_nom": u"Agent", u"_prenom": u"Fred", u"_p_userid": u"agent", u"_user_id": u"u1"},
            u"u4": {u"_nom": u"Inconnu", u"_prenom": u"", u"_p_userid": u"", u"_user_id": u"u4"},
            u"u5": {u"_nom": u"Chef", u"_prenom": u"Michel", u"_p_userid": u"chef", u"_user_id": u"u5"},
        }
        dic = {
            "e_mail_i": {},
            "e_mail_o": {},
            "e_contacts_sender": {u"20": {u"_user_id": u"u1"}, u"21": {u"_user_id": u"u4"}},
            "e_user_match": e_user_match,
        }
        dic.update(data)
        return self.make_storage(parts, **dic)

    def test___init__(self):
        bp = self.section(M1AssignedUserHandling, "m__1_assigned_user", self.options, storage=self.make_storage("a"))
        self.assertFalse(bp.original_item)  # parsed before the part check
        self.assertEqual(bp.contact_id_key, "_contact_id")
        self.assertFalse(hasattr(bp, "im_paths"))
        storage = self.m_storage()
        bp = self.section(M1AssignedUserHandling, "m__1_assigned_user", self.options, storage=storage)
        self.assertIs(bp.im_paths, storage["data"]["e_mail_i"])
        self.assertIs(bp.contacts, storage["data"]["e_contacts_sender"])
        self.assertIs(bp.user_match, storage["data"]["e_user_match"])
        self.assertIs(bp.p_user_service, storage["data"]["p_user_service"])
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        df_uid = self.uid(PLONEGROUP + "/direction-financiere")
        # IM editor services: editeur and n_plus functions
        self.assertIn(secr_uid, bp.p_u_s_editor["agent"])
        self.assertListEqual(bp.p_u_s_editor["chef"], [])
        self.assertListEqual(sorted(bp.p_u_s_editor["encodeur"]), [secr_uid])
        # OM editor services: encodeur and n_plus functions
        bp = self.section(M1AssignedUserHandling, "p__2_assigned_user", self.options2, storage=self.m_storage("p"))
        self.assertTrue(bp.original_item)
        self.assertEqual(bp.contact_id_key, "_sender_id")
        self.assertIs(bp.contacts, bp.storage["data"]["e_user_match"])
        self.assertIs(bp.im_paths, bp.storage["data"]["e_mail_o"])
        self.assertIn(df_uid, bp.p_u_s_editor["chef"])

    def test___iter__(self):
        im2, im1, im3, im4 = (self.mail(mid) for mid in ("courrier2", "courrier1", "courrier3", "courrier4"))
        im2.assigned_user = u"encodeur"
        im4.data_transfer = u"DESTINATAIRE: Agent Fred"
        paths = {
            u"200": {u"path": self.path(im2)},
            u"300": {u"path": self.path(im1)},
            u"201": {u"path": self.path(im3)},
            u"202": {u"path": u"/incoming-mail/unknown"},
            u"203": {u"path": self.path(im4)},
        }
        storage = self.m_storage(e_mail_i=paths)

        def recip(eid, mail_id, contact_id, principal=False, **kw):
            dic = {
                u"_bpk": u"e_mail_recip_i",
                u"_eid": eid,
                u"_mail_id": mail_id,
                u"_contact_id": contact_id,
                u"_principal": principal,
            }
            dic.update(kw)
            return dic

        def upd(eid, mail, **kw):
            dic = {
                "_eid": eid,
                "_path": self.path(mail),
                "_type": "dmsincomingmail",
                "_bpk": "i_assigned_user",
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            recip(u"1", u"200", u"20", True, _action=u"Pour suite", _message=u"Urgent", _response=None),
            recip(u"2", u"300", u"20"),  # user not editor in the treating group
            recip(u"3", u"200", u"99"),  # contact not a user
            recip(u"4", u"202", u"20"),  # mail not found
            recip(u"5", u"201", u"21"),  # user not matched
            recip(u"6", u"200", u"20"),  # mail already assigned
            recip(u"7", u"203", u"20"),  # nothing to do
            {u"_bpk": u"e_mail_i", u"_eid": u"8"},
        ]
        bp = self.section(M1AssignedUserHandling, "m__1_assigned_user", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                upd(
                    u"1",
                    im2,
                    data_transfer=u"DESTINATAIRE PRINCIPAL: Agent Fred, Pour suite, message=Urgent",
                    assigned_user=u"agent",
                ),
                upd(u"2", im1, data_transfer=u"DESTINATAIRE: Agent Fred"),
                upd(u"5", im3, data_transfer=u"DESTINATAIRE: Inconnu"),
                upd(u"6", im2, data_transfer=u"DESTINATAIRE: Agent Fred"),
                {u"_bpk": u"e_mail_i", u"_eid": u"8"},
            ],
        )
        self.assertIn(("dto", "WARNING", "eid '3', contact id not a user '99'"), self.logs.records)
        self.assertIn(("dto", "WARNING", "mail 202: path '/incoming-mail/unknown' not found"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"m__1_assigned_user": 7})
        # no data_transfer field: information stored in description
        storage = self.m_storage(e_mail_i=paths)
        storage["desc_only"] = True
        bp = self.section(M1AssignedUserHandling, "m__1_assigned_user", self.options, items[1:2], storage)
        self.assertListEqual(
            list(bp), [upd(u"2", im1, description=u"Ceci est la description du courrier 1\r\nDESTINATAIRE: Agent Fred")]
        )
        # original item (p__2_assigned_user): om sender
        df_uid = self.uid(PLONEGROUP + "/direction-financiere")
        info_uid = self.uid(PLONEGROUP + "/direction-generale/informatique")  # not a selected service
        items = [
            {u"_bpk": u"e_mail_o", u"_eid": u"1", u"_sender_id": u"u5", u"treating_groups": df_uid},
            {u"_bpk": u"e_mail_o", u"_eid": u"2", u"_sender_id": u"u5", u"treating_groups": info_uid},
        ]
        bp = self.section(M1AssignedUserHandling, "p__2_assigned_user", self.options2, items, self.m_storage("p"))
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_mail_o",
                    u"_eid": u"1",
                    u"_sender_id": u"u5",
                    u"treating_groups": df_uid,
                    u"assigned_user": u"chef",
                },
                {u"_bpk": u"e_mail_o", u"_eid": u"2", u"_sender_id": u"u5", u"treating_groups": info_uid},
            ],
        )
        # part not run
        bp = self.section(M1AssignedUserHandling, "m__1_assigned_user", self.options, items[:1], self.make_storage("a"))
        self.assertEqual(len(list(bp)), 1)


class TestPOMSenderSet(DmsTestCase):

    # data-transfer.cfg.1 p__1_sender_set
    options = {"condition": "python: item.get('_bpk') == u'e_mail_o'"}

    def test___init__(self):
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, storage=self.make_storage("a"))
        self.assertFalse(hasattr(bp, "condition"))
        storage = self.make_storage("p", e_contact={}, e_user_match={})
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, storage=storage)
        self.assertIs(bp.puid_to_pers, storage["data"]["p_userid_to_pers"])
        self.assertIs(bp.euid_to_pers, storage["data"]["p_euid_to_pers"])
        self.assertIs(bp.p_hps, storage["data"]["p_hps"])
        self.assertIs(bp.eid_to_orgs, storage["data"]["p_eid_to_orgs"])
        self.assertIs(bp.e_c_s, storage["data"]["e_contact"])
        self.assertIs(bp.e_u_m, storage["data"]["e_user_match"])
        bp = self.section(
            POMSenderSet, "p__1_sender_set", dict(self.options, contact_sender_key="e_user_match"), storage=storage
        )
        self.assertIs(bp.e_c_s, storage["data"]["e_user_match"])
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, storage=self.make_storage("p"))
        self.assertDictEqual(bp.e_c_s, {})
        self.assertDictEqual(bp.e_u_m, {})

    def test___iter__(self):
        agent_uid = self.uid("/contacts/personnel-folder/agent")
        chef_uid = self.uid("/contacts/personnel-folder/chef")  # stands for the generic "reprise-donnees" person
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        e_contact = {
            u"20": {u"_user_id": u"u1", u"lastname": u"Agent", u"firstname": u"Fred"},
            u"21": {u"_user_id": None, u"lastname": u"Dupont", u"firstname": u"Jean"},
        }
        storage = self.make_storage(
            "p",
            e_contact=e_contact,
            p_eid_to_orgs={u"S1": {"uid": secr_uid}},
            p_euid_to_pers={u"u1": agent_uid, u"None": chef_uid},
        )
        items = [
            {u"_bpk": u"e_mail_o", u"_eid": u"1", u"_sender_id": u"20", u"_sender": None, u"_service_id": u"S1"},
            {
                u"_bpk": u"e_mail_o",
                u"_eid": u"2",
                u"_sender_id": u"21",
                u"_sender": u"Libre",
                u"_service_id": u"S1",
                u"description": u"Desc",
            },
            {u"_bpk": u"e_mail_o", u"_eid": u"3", u"_sender_id": None, u"_sender": None, u"_service_id": u"S1"},
            {u"_bpk": u"e_mail_i", u"_eid": u"4", u"_sender_id": u"20"},
        ]
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, items, storage)
        agent_hp = self.uid("/contacts/personnel-folder/agent/agent-secretariat")
        chef_hp = self.uid("/contacts/personnel-folder/chef/responsable-secretariat")
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_mail_o",
                    u"_eid": u"1",
                    u"_sender_id": u"20",
                    u"_sender": None,
                    u"_service_id": u"S1",
                    u"sender": agent_hp,
                },
                # not a user: generic person and contact in text
                {
                    u"_bpk": u"e_mail_o",
                    u"_eid": u"2",
                    u"_sender_id": u"21",
                    u"_sender": u"Libre",
                    u"_service_id": u"S1",
                    u"sender": chef_hp,
                    u"description": u"Desc\r\nEXPÉDITEUR: Dupont Jean.\r\nEXPÉDITEUR LIBRE: Libre",
                    u"data_transfer": u"EXPÉDITEUR: Dupont Jean.",
                },
                {
                    u"_bpk": u"e_mail_o",
                    u"_eid": u"3",
                    u"_sender_id": None,
                    u"_sender": None,
                    u"_service_id": u"S1",
                    u"sender": chef_hp,
                },
                {u"_bpk": u"e_mail_i", u"_eid": u"4", u"_sender_id": u"20"},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"p__1_sender_set": 3})
        # part not run
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, items[:1], self.make_storage("a"))
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"e_mail_o",
                    u"_eid": u"1",
                    u"_sender_id": u"20",
                    u"_sender": None,
                    u"_service_id": u"S1",
                    u"sender": agent_hp,
                }
            ],
        )


class TestParentPathInsert(DmsTestCase):
    def test___init__(self):
        bp = self.section(ParentPathInsert, "l__1_parent_insert", {}, storage=self.make_storage("a"))
        self.assertEqual(bp.parts, "l")
        self.assertIsNone(bp.bp_key)
        self.assertEqual(bp.im_folder, self.portal["incoming-mail"])
        self.assertEqual(bp.om_folder, self.portal["outgoing-mail"])
        bp = self.section(
            ParentPathInsert, "f__1_parent_insert", {"bp_key": "e_folder_p"}, storage=self.make_storage("f")
        )
        self.assertEqual(bp.bp_key, u"e_folder_p")
        self.assertIsInstance(bp.bp_key, six.text_type)

    def test___iter__(self):
        cdate = datetime(2009, 9, 22, 12, 0)
        storage = self.make_storage(
            "lpfi", e_parent={u"F1": {"path": u"/folders/f1"}, u"O1": {"path": u"/contacts/o1"}}
        )
        items = [
            {u"_type": "dmsincomingmail", u"creation_date": cdate},
            {u"_type": "dmsincoming_email", u"creation_date": cdate},
            {u"_type": "dmsoutgoingmail", u"creation_date": cdate},
            {u"_type": "ClassificationFolder"},
            {u"_type": "ClassificationSubfolder", u"_parent_id": u"F1"},
            {u"_bpk": u"e_folder", u"_eid": u"2", u"_type": "ClassificationSubfolder", u"_parent_id": u"F9"},
            {u"_type": "person"},
            {u"_type": "organization", u"_parent_id": u"O1"},
            {u"_bpk": u"e_contact", u"_eid": u"3", u"_type": "organization", u"_parent_id": u"O9"},
            {u"_type": "organization", u"_parent_id": None},
            {u"_type": "held_position"},
            {u"_type": "dmsincomingmail", u"_path": u"/incoming-mail/x"},
            {u"_type": "dmsincomingmail", u"_parenth": u"/incoming-mail/y"},
            {u"_bpk": u"e_mail_i"},
        ]
        bp = self.section(ParentPathInsert, "lpfi__parent_insert", {"bp_key": "e_parent"}, items, storage)
        result = list(bp)
        self.assertListEqual(
            result,
            [
                {u"_type": "dmsincomingmail", u"creation_date": cdate, u"_parenth": u"/incoming-mail/200938"},
                {u"_type": "dmsincoming_email", u"creation_date": cdate, u"_parenth": u"/incoming-mail/200938"},
                {u"_type": "dmsoutgoingmail", u"creation_date": cdate, u"_parenth": u"/outgoing-mail/200938"},
                {u"_type": "ClassificationFolder", u"_parenth": u"/folders"},
                {u"_type": "ClassificationSubfolder", u"_parent_id": u"F1", u"_parenth": u"/folders/f1"},
                {
                    u"_bpk": u"e_folder",
                    u"_eid": u"2",
                    u"_type": "ClassificationSubfolder",
                    u"_parent_id": u"F9",
                    u"_error": True,
                },
                {u"_type": "person", u"_parenth": u"/contacts"},
                {u"_type": "organization", u"_parent_id": u"O1", u"_parenth": u"/contacts/o1"},
                {
                    u"_bpk": u"e_contact",
                    u"_eid": u"3",
                    u"_type": "organization",
                    u"_parent_id": u"O9",
                    u"_parenth": u"/contacts",
                    u"_error": True,
                },
                {u"_type": "organization", u"_parent_id": None, u"_parenth": u"/contacts"},
                {u"_type": "held_position"},
                {u"_type": "dmsincomingmail", u"_path": u"/incoming-mail/x"},
                {u"_type": "dmsincomingmail", u"_parenth": u"/incoming-mail/y"},
                {u"_bpk": u"e_mail_i"},
            ],
        )
        self.assertIsInstance(result[0][u"_parenth"], six.text_type)
        # period folders are created
        self.assertIn("200938", self.portal["incoming-mail"])
        self.assertIn("200938", self.portal["outgoing-mail"])
        self.assertEqual(api.content.get_state(self.portal["incoming-mail"]["200938"]), "internally_published")
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", u"e_folder: _eid 2, Parent id not found 'F9'"),
                ("dti", "ERROR", u"e_contact: _eid 3, Parent id not found 'O9'"),
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"lpfi__parent_insert": 11})
        # part not run
        bp = self.section(ParentPathInsert, "l__1_parent_insert", {}, [{u"_type": "person"}], self.make_storage("a"))
        self.assertListEqual(list(bp), [{u"_type": "person"}])


class TestPostActions(DTestCase):
    def test___init__(self):
        storage = self.make_storage("a")
        bp = self.section(PostActions, "post_actions", {}, storage=storage)
        self.assertIs(bp.storage, storage)
        self.assertEqual(bp.portal, self.portal)

    def test___iter__(self):
        storage = self.d_storage()
        agent = self.obj("/contacts/personnel-folder/agent")
        agent_hps = dict(storage["data"]["p_hps"][agent.UID()])
        dupont = api.content.create(
            container=self.obj("/contacts/personnel-folder"), type="person", id="dupont", lastname=u"Dupont"
        )
        post = {u"store_internal_person_info": ""}
        items = [
            {
                u"_bpk": u"person_sender",
                u"_eid": u"1",
                u"_path": "/contacts/personnel-folder/dupont",
                u"internal_number": u"u7",
                u"_post_actions": post,
            },
            {
                u"_bpk": u"personnel",
                u"_eid": u"2",
                u"_path": "/contacts/personnel-folder/agent",
                u"internal_number": u"u1",
                u"_post_actions": post,
            },
            {u"_bpk": u"e_mail_i", u"_eid": u"3"},
        ]
        bp = self.section(PostActions, "post_actions", {}, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    u"_bpk": u"person_sender",
                    u"_eid": u"1",
                    u"_path": "/contacts/personnel-folder/dupont",
                    u"internal_number": u"u7",
                    u"_post_actions": {u"store_internal_person_info": ""},
                },
                {
                    u"_bpk": u"personnel",
                    u"_eid": u"2",
                    u"_path": "/contacts/personnel-folder/agent",
                    u"internal_number": u"u1",
                    u"_post_actions": {u"store_internal_person_info": ""},
                },
                {u"_bpk": u"e_mail_i", u"_eid": u"3"},
            ],
        )
        self.assertEqual(storage["data"]["p_euid_to_pers"][u"u7"], dupont.UID())
        self.assertEqual(storage["data"]["p_euid_to_pers"][u"u1"], agent.UID())
        self.assertDictEqual(
            storage["data"]["p_hps"][dupont.UID()],
            {"path": "/contacts/personnel-folder/dupont", "hps": {}, "state": "deactivated"},
        )
        self.assertDictEqual(storage["data"]["p_hps"][agent.UID()], agent_hps)  # not replaced
        self.assertDictEqual(dict(storage["course"]), {"post_actions": 2})
        # commit every commit_nb items, then pickle dumps
        commits = []
        self.addCleanup(setattr, transaction, "commit", transaction.commit)
        transaction.commit = lambda: commits.append(True)  # a real commit would break the test isolation
        tm = get_transmogrifier(self.portal)
        dumped = os.path.join(self.tmp, "2_e_mail_i.pkl")
        storage.update(
            {
                "commit": 1,
                "commit_nb": 2,
                "count": {"commit_count": {"": {"c": 4}}},
                "lastsection": {
                    "pkl_dump": [
                        (dumped, "e_mail_i", Condition("python: True", tm, "l__pickle_mail_i", {})),
                        (os.path.join(self.tmp, "2_no.pkl"), "e_mail_i", Condition("python: False", tm, "x", {})),
                        ("", "e_mail_i", Condition("python: True", tm, "y", {})),
                    ]
                },
            }
        )
        storage["data"]["e_mail_i"] = {u"100": {u"path": u"/incoming-mail/x/100"}}
        bp = self.section(PostActions, "post_actions", {}, [{u"_bpk": u"e_mail_i", u"_eid": u"100"}], storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mail_i", u"_eid": u"100"}])
        self.assertListEqual(commits, [True])
        self.assertIn(("dto", "INFO", u"Commit in 'e_mail_i' at 4"), self.logs.records)
        self.assertIn(("dto", "INFO", u"Dumping '{}'".format(dumped)), self.logs.records)
        with io.open(dumped, "rb") as fh:
            self.assertDictEqual(pickle.load(fh), {u"100": {u"path": u"/incoming-mail/x/100"}})
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "2_no.pkl")))
        # not a multiple of commit_nb
        storage["count"]["commit_count"][""]["c"] = 5
        bp = self.section(PostActions, "post_actions", {}, [{u"_bpk": u"e_mail_i", u"_eid": u"100"}], storage)
        list(bp)
        self.assertListEqual(commits, [True])


class TestQ1RecipientsAsTextUpdate(DmsTestCase):

    options = {"condition": "python: item.get('_bpk') == u'e_mail_recip_o'", "store_key": "e_mail_o"}

    def test___init__(self):
        bp = self.section(
            Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, storage=self.make_storage("a")
        )
        self.assertFalse(hasattr(bp, "om_paths"))
        storage = self.make_storage("q", e_mail_o={}, e_contact={})
        bp = self.section(Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, storage=storage)
        self.assertIs(bp.om_paths, storage["data"]["e_mail_o"])
        self.assertIs(bp.e_c, storage["data"]["e_contact"])
        self.assertEqual(bp.contact_id_key, "_contact_id")
        # contact type matching file exists: contacts are imported, contact id not used
        io.open(os.path.join(self.tmp, "1_contact_type_match.csv"), "w").close()
        sections = {"h__contact_type_match_read": {"filename": "1_contact_type_match.csv"}}
        bp = self.section(
            Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, storage=storage, sections=sections
        )
        self.assertEqual(bp.contact_id_key, "")

    def test___iter__(self):
        om = self.mail("reponse1")
        om_path = self.path(om)
        storage = self.make_storage(
            "q",
            e_mail_o={u"100": {u"path": om_path}, u"101": {u"path": u"/outgoing-mail/no"}},
            e_contact=TestContactAsTextUpdate.e_contact,
        )
        items = [
            {
                u"_bpk": u"e_mail_recip_o",
                u"_eid": u"1",
                u"_mail_id": u"100",
                u"_contact_id": u"8",
                u"_comment": u"Copie",
                u"_action": u"Pour info",
            },
            {u"_bpk": u"e_mail_recip_o", u"_eid": u"2", u"_mail_id": u"100", u"_contact_id": None, u"_comment": None},
            {u"_bpk": u"e_mail_recip_o", u"_eid": u"3", u"_mail_id": u"101", u"_contact_id": u"8", u"_comment": None},
            {u"_bpk": u"e_mail_o", u"_eid": u"4"},
        ]
        bp = self.section(Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_eid": u"1",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "o_recipients",
                    "_act": "U",
                    "description": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\nDESTINATAIRE LIBRE: Copie",
                    # Plone 4 behaviour (bug?): separator added before the complement
                    "data_transfer": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.\r\n"
                    u"COMPLÉMENT DESTINATAIRE: , Pour info",
                },
                {u"_bpk": u"e_mail_o", u"_eid": u"4"},
            ],
        )
        self.assertIn(("dto", "WARNING", "mail 101: path '/outgoing-mail/no' not found"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"q__1_recipients_as_text": 3})
        # description only
        storage["desc_only"] = True
        bp = self.section(Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, items[:1], storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_eid": u"1",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "o_recipients",
                    "_act": "U",
                    "description": u"DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    u"ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.\r\n"
                    u"DESTINATAIRE LIBRE: Copie",
                }
            ],
        )
        # part not run
        bp = self.section(
            Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, items[3:], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mail_o", u"_eid": u"4"}])


class TestR1RecipientGroupsUpdate(DmsTestCase):

    # data-transfer.cfg.1 r__1_recipient_groups_update
    options = {
        "condition": "python: modules['imio.transmogrifier.iadocs.utils'].is_in_part(transmogrifier, 'r') "
        "and item.get('_bpk') == u'e_mail_rec_groups'",
        "store_key": "e_mail",
    }

    def test___init__(self):
        bp = self.section(
            R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, storage=self.make_storage("a")
        )
        self.assertFalse(hasattr(bp, "paths"))
        storage = self.make_storage("r", e_mail={})
        bp = self.section(R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, storage=storage)
        self.assertIs(bp.paths, storage["data"]["e_mail"])
        bp = self.section(
            R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, storage=self.make_storage("r")
        )
        self.assertIsNone(bp.paths)

    def test___iter__(self):
        im2, im3, om1 = self.mail("courrier2"), self.mail("courrier3"), self.mail("reponse1")
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        grh_uid = self.uid(PLONEGROUP + "/direction-generale/grh")
        comm_uid = self.uid(PLONEGROUP + "/direction-generale/communication")
        im3.recipient_groups = [secr_uid]
        paths = {
            u"200": {u"path": self.path(im2)},
            u"201": {u"path": self.path(im3)},
            u"202": {u"path": u"/incoming-mail/no"},
            u"203": {u"path": self.path(om1)},
        }
        e_s_m = {u"S1": {u"uid": secr_uid}, u"S2": {u"uid": grh_uid}, u"S4": {u"uid": comm_uid}}
        storage = self.make_storage("r", e_mail=paths, e_service_match=e_s_m, recipient_groups={})

        def rec(eid, mail_id, service_id):
            return {u"_bpk": u"e_mail_rec_groups", u"_eid": eid, u"_mail_id": mail_id, u"_service_id": service_id}

        def upd(eid, mail_id, service_id, mail, **kw):
            dic = {
                "_eid": eid,
                "_mail_id": mail_id,
                "_service_id": service_id,
                "_bpk": u"recipient_groups",
                "_path": self.path(mail),
                "_type": mail.portal_type,
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            rec(u"1", u"200", u"S1"),  # treating group: stored
            rec(u"2", u"200", u"S2"),
            rec(u"3", u"202", u"S2"),  # mail not found
            rec(u"4", u"203", u"S2"),
            rec(u"5", u"201", u"S1"),  # already a recipient group
            rec(u"6", u"201", u"S4"),
            {u"_bpk": u"e_mail_i", u"_eid": u"7"},
        ]
        bp = self.section(R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                upd(u"2", u"200", u"S2", im2, recipient_groups=[grh_uid], _suid=grh_uid),
                upd(u"4", u"203", u"S2", om1, recipient_groups=[grh_uid], _suid=grh_uid),
                upd(u"5", u"201", u"S1", im3, _suid=secr_uid),  # useful when correcting
                upd(u"6", u"201", u"S4", im3, recipient_groups=[secr_uid, comm_uid], _suid=comm_uid),
                {u"_bpk": u"e_mail_i", u"_eid": u"7"},
            ],
        )
        self.assertDictEqual(storage["data"]["recipient_groups"], {u"200": {u"S1": {u"_eid": u"1"}}})
        self.assertListEqual(im3.recipient_groups, [secr_uid])  # not modified
        self.assertIn(("dto", "WARNING", "mail 202: path '/incoming-mail/no' not found"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"r__1_recipient_groups_update": 6})
        # already treated in a previous batch
        storage["data"]["recipient_groups"][u"201"] = {u"S1": {u"_eid": u"5"}}
        bp = self.section(R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, items[4:5], storage)
        self.assertListEqual(list(bp), [])
        # part not run
        bp = self.section(
            R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, items[:1], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [rec(u"1", u"200", u"S1")])


class TestReadLabelForRecipientGroup(DmsTestCase):

    # data-transfer.cfg.1 r__read_label_for_recipient_group
    options = {
        "condition": "python: item.get('_bpk') == u'recipient_groups' and item.get('_suid') "
        "and item['_type'] != 'dmsoutgoingmail'"
    }

    def test___init__(self):
        bp = self.section(
            ReadLabelForRecipientGroup,
            "r__read_label_for_recipient_group",
            self.options,
            storage=self.make_storage("a"),
        )
        self.assertFalse(hasattr(bp, "condition"))
        bp = self.section(
            ReadLabelForRecipientGroup,
            "r__read_label_for_recipient_group",
            self.options,
            storage=self.make_storage("r"),
        )
        self.assertTrue(bp.condition({u"_bpk": u"recipient_groups", u"_suid": u"x", u"_type": "dmsincomingmail"}))

    def test___iter__(self):
        im2 = self.mail("courrier2")
        grh_uid = self.uid(PLONEGROUP + "/direction-generale/grh")
        ILabeling(im2).storage["lu"] = PersistentList(["agent", "chef"])
        items = [
            {
                u"_bpk": u"recipient_groups",
                u"_eid": u"1",
                u"_path": self.path(im2),
                u"_type": "dmsincomingmail",
                u"_suid": grh_uid,
            },
            {
                u"_bpk": u"recipient_groups",
                u"_eid": u"2",
                u"_path": u"/incoming-mail/no",
                u"_type": "dmsincomingmail",
                u"_suid": grh_uid,
            },
            {
                u"_bpk": u"recipient_groups",
                u"_eid": u"3",
                u"_path": self.path(im2),
                u"_type": "dmsoutgoingmail",
                u"_suid": grh_uid,
            },
        ]
        storage = self.make_storage("r")
        bp = self.section(ReadLabelForRecipientGroup, "r__read_label_for_recipient_group", self.options, items, storage)
        self.assertListEqual([item[u"_eid"] for item in bp], [u"1", u"3"])
        # readers of the recipient group are added as having read the mail
        self.assertListEqual(list(ILabeling(im2).storage["lu"]), ["agent", "chef", "lecteur"])
        self.assertTrue(items[1][u"_error"])
        self.assertIn(
            ("dti", "ERROR", u"recipient_groups: _eid 2, Cannot find mail with path '/incoming-mail/no'"),
            self.logs.records,
        )
        self.assertDictEqual(dict(storage["course"]), {"r__read_label_for_recipient_group": 2})
        # part not run
        bp = self.section(
            ReadLabelForRecipientGroup,
            "r__read_label_for_recipient_group",
            self.options,
            items[1:2],
            self.make_storage("a"),
        )
        self.assertEqual(len(list(bp)), 1)


class TestRsyncFileWrite(DmsTestCase):

    # data-transfer.cfg.1 1__rsync_file_write
    options = {
        "b_condition": "python: modules['os'].path.exists(modules['imio.pyutils.system'].full_path(storage['csvp'], "
        "transmogrifier.get('1tu__pickle_f_r', {}).get('filename', '_not_found_')))",
        "filename": "2_rsync_files.txt",
        "bp_key": "e_files",
    }
    sections = {"1tu__pickle_f_r": {"filename": "2_files.pkl"}}

    def test___init__(self):
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, storage=self.make_storage("a"))
        self.assertIsNone(bp.fh)
        self.assertFalse(hasattr(bp, "filename"))
        # files pickle not found
        bp = self.section(
            RsyncFileWrite, "1__rsync_file_write", self.options, storage=self.make_storage("1"), sections=self.sections
        )
        self.assertEqual(bp.filename, os.path.join(self.tmp, u"2_rsync_files.txt"))
        self.assertFalse(bp.doit)
        self.assertFalse(hasattr(bp, "bp_key"))
        io.open(os.path.join(self.tmp, "2_files.pkl"), "wb").close()
        storage = self.make_storage("1", e_files={})
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, storage=storage, sections=self.sections)
        self.assertTrue(bp.doit)
        self.assertEqual(bp.bp_key, u"e_files")
        self.assertIs(bp.files, storage["data"]["e_files"])
        self.assertEqual(bp.unused_filename, u"")
        self.assertEqual(bp.used, set())
        options = dict(self.options, filename="/tmp/rsync.txt", unused_filename="3_unused.txt")
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", options, storage=storage, sections=self.sections)
        self.assertEqual(bp.filename, u"/tmp/rsync.txt")
        self.assertEqual(bp.unused_filename, os.path.join(self.tmp, u"3_unused.txt"))

    def test___iter__(self):
        io.open(os.path.join(self.tmp, "2_files.pkl"), "wb").close()
        e_files = {
            u"1000": {"f": [(u".pdf", u"PDF_0-999")]},
            u"1001": {"f": [(u".doc", u"DOC"), (u".pdf", u"PDF")]},
            u"1002": {"f": [(u".pdf", u"PDF"), (u".doc", u"DOC")]},
            u"1005": {"f": [(u".pdf", u"PDF")]},
        }
        storage = self.make_storage("1", e_files=e_files)
        items = [
            {u"_bpk": u"e_file", u"_eid": u"1000"},
            {u"_bpk": u"e_file", u"_eid": u"1003", u"_fs_path": u"scans/1005.pdf"},
            {u"_bpk": u"e_file", u"_eid": u"1001"},
            {u"_bpk": u"e_file", u"_eid": u"1004"},  # not on disk
        ]
        options = dict(self.options, unused_filename="2_unused_files.txt")
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", options, items, storage, sections=self.sections)
        self.assertListEqual(list(bp), [])  # items are consumed
        with io.open(os.path.join(self.tmp, "2_rsync_files.txt"), encoding="utf8") as fh:
            self.assertEqual(fh.read(), u"PDF_0-999/1000.pdf\nscans/1005.pdf\nDOC/1001.doc\nPDF/1001.pdf\n")
        with io.open(os.path.join(self.tmp, "2_unused_files.txt"), encoding="utf8") as fh:
            self.assertEqual(fh.read(), u"DOC/1002.doc\nPDF/1002.pdf\n")
        self.assertSetEqual(bp.used, {u"1000", u"1003", u"1005", u"1001", u"1004"})
        self.assertIsNone(bp.fh)
        self.assertIn(
            ("dto", "INFO", u"Writing '{}'".format(os.path.join(self.tmp, u"2_rsync_files.txt"))), self.logs.records
        )
        self.assertDictEqual(dict(storage["course"]), {"1__rsync_file_write": 4})
        # file cannot be created
        options = dict(self.options, filename=os.path.join(self.tmp, "unknown", "rsync.txt"))
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", options, items, storage, sections=self.sections)
        self.assertRaises(Exception, list, bp)
        # b_condition False or part not run: items yielded
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, items[:1], storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_file", u"_eid": u"1000"}])
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, items[:1], self.make_storage("a"))
        self.assertListEqual(list(bp), [{u"_bpk": u"e_file", u"_eid": u"1000"}])


class TestS1ClassificationFoldersUpdate(DmsTestCase):

    # data-transfer.cfg.1 s__1_classification_folders_update
    options = {
        "condition": "python: modules['imio.transmogrifier.iadocs.utils'].is_in_part(transmogrifier, 's') "
        "and item.get('_bpk') == u'e_mail_folder'",
        "store_key": "e_mail",
    }

    def test___init__(self):
        bp = self.section(
            S1ClassificationFoldersUpdate,
            "s__1_classification_folders_update",
            self.options,
            storage=self.make_storage("a"),
        )
        self.assertFalse(hasattr(bp, "paths"))
        storage = self.make_storage("s", e_mail={})
        bp = self.section(
            S1ClassificationFoldersUpdate, "s__1_classification_folders_update", self.options, storage=storage
        )
        self.assertTrue(bp.f_e_m_c)
        self.assertIs(bp.paths, storage["data"]["e_mail"])
        bp = self.section(
            S1ClassificationFoldersUpdate,
            "s__1_classification_folders_update",
            dict(self.options, fill_empty_mail_category="0"),
            storage=storage,
        )
        self.assertFalse(bp.f_e_m_c)

    def test___iter__(self):
        im2, im3, im4, om1 = (self.mail(mid) for mid in ("courrier2", "courrier3", "courrier4", "reponse1"))
        folder = self.obj("/folders/ordre-public-reglement-general-de-police")
        other = self.obj("/folders/controle-des-armes-et-munitions")
        cat_uid = get_categories(self.portal)[u"-1.7"]["uid"]
        self.imported(im2, im3, im4, om1)
        om1.classification_folders = [folder.UID()]
        om1.classification_categories = [cat_uid]
        im4.classification_folders = [other.UID()]
        paths = {
            u"200": {u"path": self.path(im2)},
            u"201": {u"path": self.path(im3)},
            u"202": {u"path": self.path(om1)},
            u"203": {u"path": self.path(im4)},
        }
        storage = self.make_storage(
            "s",
            e_mail=paths,
            e_folder={
                u"F0001": {u"_category_id": u"C1", u"_title": u"Ordre public"},
                u"F0002": {u"_category_id": u"C9", u"_title": u"Dossier non importé"},
            },
            e_category_full_match={u"C1": {u"_puid": cat_uid}},
            p_irn_to_folder={u"F0001": {"uid": folder.UID()}},
        )

        def link(mail_id, folder_id):
            return {u"_bpk": u"e_mail_folder", u"_eid": mail_id, u"_folder_id": folder_id}

        def upd(mail_id, folder_id, mail, **kw):
            dic = {
                "_eid": mail_id,
                "_folder_id": folder_id,
                "_bpk": u"classification_folders",
                "_path": self.path(mail),
                "_type": mail.portal_type,
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            link(u"200", u"F0001"),
            link(u"201", u"F0002"),  # folder not created in Plone
            link(u"202", u"F9"),  # unknown folder
            link(u"202", u"F0001"),  # already set
            link(u"203", u"F0001"),
            {u"_bpk": u"e_mail", u"_eid": u"204"},
        ]
        bp = self.section(
            S1ClassificationFoldersUpdate, "s__1_classification_folders_update", self.options, items, storage
        )
        result = list(bp)
        self.assertListEqual(
            result,
            [
                upd(u"200", u"F0001", im2, classification_categories=[cat_uid], classification_folders=[folder.UID()]),
                upd(
                    u"201",
                    u"F0002",
                    im3,
                    description=u"Ceci est la description du courrier 3\r\nDOSSIER: Dossier non importé",
                ),
                upd(
                    u"203",
                    u"F0001",
                    im4,
                    classification_categories=[cat_uid],
                    classification_folders=[other.UID(), folder.UID()],
                ),
                {u"_bpk": u"e_mail", u"_eid": u"204"},
            ],
        )
        # Plone 4 behaviour (bug?): the mail folders list is modified in place
        self.assertIs(result[2]["classification_folders"], im4.classification_folders)
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                (
                    "dti",
                    "ERROR",
                    u"e_mail_folder: _eid 201, Cannot find folder_id in plone 'F0002' => put in in " u"description",
                ),
                ("dti", "ERROR", u"e_mail_folder: _eid 202, Cannot find folder_id 'F9' in existing folders"),
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"s__1_classification_folders_update": 5})
        # Plone 4 behaviour (bug?): a mail without classification_categories attribute (example mail of the
        # imio.dms.mail fixture, not created by the pipeline) raises an AttributeError
        storage["data"]["e_mail"][u"205"] = {u"path": self.path(self.mail("courrier5"))}
        bp = self.section(
            S1ClassificationFoldersUpdate,
            "s__1_classification_folders_update",
            self.options,
            [link(u"205", u"F0001")],
            storage,
        )
        self.assertRaises(AttributeError, list, bp)
        # empty mail category not filled
        options = dict(self.options, fill_empty_mail_category="0")
        bp = self.section(
            S1ClassificationFoldersUpdate, "s__1_classification_folders_update", options, items[:1], storage
        )
        self.assertListEqual(list(bp), [upd(u"200", u"F0001", im2, classification_folders=[folder.UID()])])
        # part not run
        bp = self.section(
            S1ClassificationFoldersUpdate,
            "s__1_classification_folders_update",
            self.options,
            items[:1],
            self.make_storage("a"),
        )
        self.assertListEqual(list(bp), [link(u"200", u"F0001")])


class TestS3ClassificationFoldersUpdate(DmsTestCase):

    # data-transfer.cfg.3 s__3_classification_folders_update
    options = {
        "condition": "python: modules['imio.transmogrifier.iadocs.utils'].is_in_part(transmogrifier, 's') "
        "and item.get('_bpk') == u'e_mail_folder'",
        "store_key": "e_mail",
        "mail_id_key": "_mail_id",
        "replace_default_category": "1",
    }

    def test___init__(self):
        bp = self.section(
            S3ClassificationFoldersUpdate,
            "s__3_classification_folders_update",
            self.options,
            storage=self.make_storage("a"),
        )
        self.assertFalse(hasattr(bp, "paths"))
        storage = self.make_storage("s", e_mail={}, p_irn_to_folder={})
        storage["plone"]["def_category"] = u"abcd"
        bp = self.section(
            S3ClassificationFoldersUpdate, "s__3_classification_folders_update", self.options, storage=storage
        )
        self.assertEqual(bp.mail_id_key, u"_mail_id")
        self.assertTrue(bp.rdc)
        self.assertIs(bp.paths, storage["data"]["e_mail"])
        self.assertEqual(bp.def_cat, u"abcd")
        self.assertIs(bp.irn_to_f, storage["data"]["p_irn_to_folder"])
        bp = self.section(
            S3ClassificationFoldersUpdate,
            "s__3_classification_folders_update",
            {"store_key": "e_mail"},
            storage=self.make_storage("s", e_mail={}),
        )
        self.assertEqual(bp.mail_id_key, u"_eid")
        self.assertFalse(bp.rdc)
        self.assertIsNone(bp.def_cat)
        self.assertIsNone(bp.irn_to_f)

    def test___iter__(self):
        im2, im3, om1, im4 = (self.mail(mid) for mid in ("courrier2", "courrier3", "reponse1", "courrier4"))
        folder = self.obj("/folders/ordre-public-reglement-general-de-police")
        cats = get_categories(self.portal)
        def_cat = cats[u"-1"]["uid"]
        self.imported(im2, im3, om1, im4)
        im3.classification_categories = [def_cat]
        paths = {
            u"200": {u"path": self.path(im2)},
            u"201": {u"path": self.path(im3)},
            u"202": {u"path": self.path(om1)},
            u"203": {u"path": self.path(im4)},
        }
        storage = self.make_storage(
            "s",
            e_mail=paths,
            e_folder={u"F0002": {u"_title": u"Dossier non importé"}},
            p_irn_to_folder={u"F0001": {"uid": folder.UID()}},
        )
        storage["plone"]["def_category"] = def_cat

        def upd(eid, folder_id, mail, **kw):
            dic = {
                "_eid": eid,
                "_folder_id": folder_id,
                "_bpk": u"classification_folders",
                "_path": self.path(mail),
                "_type": mail.portal_type,
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            {u"_bpk": u"e_mail_folder", u"_eid": u"1", u"_mail_id": u"200", u"_folder_id": u"F0001"},
            {u"_bpk": u"e_mail_folder", u"_eid": u"2", u"_mail_id": u"201", u"_folder_id": u"F0001"},
            {u"_bpk": u"e_mail_folder", u"_mail_id": u"202", u"_folder_id": u"F0001"},  # no _eid
            {u"_bpk": u"e_mail_folder", u"_eid": u"4", u"_mail_id": u"203", u"_folder_id": u"F0002"},
            {u"_bpk": u"e_mail_folder", u"_eid": u"5", u"_mail_id": u"203", u"_folder_id": u"F9"},
            {u"_bpk": u"e_mail", u"_eid": u"6"},
        ]
        bp = self.section(
            S3ClassificationFoldersUpdate, "s__3_classification_folders_update", self.options, items, storage
        )
        folder_cats = list(folder.classification_categories)
        self.assertListEqual(
            list(bp),
            [
                upd(u"1", u"F0001", im2, classification_categories=folder_cats, classification_folders=[folder.UID()]),
                # default category replaced
                upd(u"2", u"F0001", im3, classification_categories=folder_cats, classification_folders=[folder.UID()]),
                # Plone 4 behaviour (bug?): without _eid, the mail_id_key name is used as _eid
                upd(
                    u"_mail_id",
                    u"F0001",
                    om1,
                    classification_categories=folder_cats,
                    classification_folders=[folder.UID()],
                ),
                upd(
                    u"4",
                    u"F0002",
                    im4,
                    description=u"Ceci est la description du courrier 4\r\nDOSSIER: Dossier non " u"importé",
                ),
                {u"_bpk": u"e_mail", u"_eid": u"6"},
            ],
        )
        self.assertListEqual(
            [rec[2] for rec in self.logs.records if rec[0] == "dti"],
            [
                u"e_mail_folder: _eid 4, Cannot find folder_id in plone 'F0002' => put in in description",
                u"e_mail_folder: _eid 5, Cannot find folder_id in plone 'F9' => put in in description",
                u"e_mail_folder: _eid 5, Cannot find folder_id in external folders 'F9' => we pass it",
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"s__3_classification_folders_update": 5})
        # Plone 4 behaviour (bug?): without _eid, log_error raises a KeyError
        items = [{u"_bpk": u"e_mail_folder", u"_mail_id": u"202", u"_folder_id": u"F9"}]
        bp = self.section(
            S3ClassificationFoldersUpdate, "s__3_classification_folders_update", self.options, items, storage
        )
        self.assertRaises(KeyError, list, bp)
        # part not run
        bp = self.section(
            S3ClassificationFoldersUpdate,
            "s__3_classification_folders_update",
            self.options,
            items[:1],
            self.make_storage("a"),
        )
        self.assertEqual(len(list(bp)), 1)


class TestT1DmsfileCreation(DmsTestCase):

    # data-transfer.cfg.1 t__1_dmsfile_creation
    options = {
        "condition": "python: item.get('_bpk') == u'e_file'",
        "bp_key": "e_dmsfile_i",
        "store_key": "e_mail_i",
        "disk_files_key": "e_files",
        "categories": "\ndmsmainfile annexes_types/incoming_dms_files/incoming-dms-file\n"
        "dmsappendixfile annexes_types/incoming_appendix_files/incoming-appendix-file",
    }

    def test___init__(self):
        bp = self.section(T1DmsfileCreation, "t__1_dmsfile_creation", self.options, storage=self.make_storage("a"))
        self.assertFalse(hasattr(bp, "bp_key"))
        storage = self.make_storage("t", e_mail_i={}, e_files={})
        bp = self.section(T1DmsfileCreation, "t__1_dmsfile_creation", self.options, storage=storage)
        self.assertEqual(bp.bp_key, u"e_dmsfile_i")
        self.assertIs(bp.paths, storage["data"]["e_mail_i"])
        self.assertIs(bp.disk_files, storage["data"]["e_files"])
        self.assertEqual(bp.title_key, u"_desc")
        self.assertDictEqual(
            bp.categories,
            {
                u"dmsmainfile": calculate_category_id(self.obj("/annexes_types/incoming_dms_files/incoming-dms-file")),
                u"dmsappendixfile": calculate_category_id(
                    self.obj("/annexes_types/incoming_appendix_files/incoming-appendix-file")
                ),
            },
        )
        bp = self.section(
            T1DmsfileCreation, "t__1_dmsfile_creation", dict(self.options, title_key="_title"), storage=storage
        )
        self.assertEqual(bp.title_key, u"_title")

    def test___iter__(self):
        im2, im3 = self.mail("courrier2"), self.mail("courrier3")
        for path, content in (
            ("PDF/1000.pdf", b"%PDF-1000"),
            ("DOC/1001.doc", b"DOC-1001"),
            ("PDF/1001.pdf", b"%PDF-1001"),
            ("scans/x.doc", b"DOC-x"),
        ):
            fullpath = os.path.join(self.tmp, path)
            if not os.path.exists(os.path.dirname(fullpath)):
                os.makedirs(os.path.dirname(fullpath))
            with io.open(fullpath, "wb") as fh:
                fh.write(content)
        cdate = datetime(2009, 9, 22, 12, 0)
        e_files = {u"1000": {"f": [(u".pdf", u"PDF")]}, u"1001": {"f": [(u".doc", u"DOC"), (u".pdf", u"PDF")]}}
        paths = {
            u"200": {u"path": self.path(im2)},
            u"201": {u"path": self.path(im3)},
            u"202": {u"path": self.path(im3)},
        }
        storage = self.make_storage("t", e_mail_i=paths, e_files=e_files, e_dmsfile_unfound={})

        def efile(eid, mail_id, **kw):
            dic = {
                u"_bpk": u"e_file",
                u"_eid": eid,
                u"_mail_id": mail_id,
                u"_desc": u"Fichier scanné",
                u"_order": None,
                u"_fs_path": None,
                u"_filename": None,
                u"_ext": None,
                u"creation_date": cdate,
            }
            dic.update(kw)
            return dic

        items = [
            efile(u"1000", u"200", _order=u"1", _filename=u"scan.tif", _ext=u"pdf"),
            efile(u"1001", u"200", _order=u"2"),  # appendix
            efile(u"1003", u"201", _fs_path=u"scans/x.doc", _fs_idx=u"1"),  # split from multiple paths: appendix
            efile(u"1004", u"202"),  # no file
            efile(u"1005", u"202", _fs_path=u"missing/1005.pdf"),
            {u"_bpk": u"e_mail", u"_eid": u"1006"},
        ]
        bp = self.section(T1DmsfileCreation, "t__1_dmsfile_creation", self.options, items, storage)
        result = list(bp)
        main_cat, app_cat = bp.categories[u"dmsmainfile"], bp.categories[u"dmsappendixfile"]

        def dmsfile(eid, mail, typ, cat, title, data):
            return {
                "_eid": eid,
                "_parenth": self.path(mail),
                "_type": typ,
                "_bpk": u"e_dmsfile_i",
                "_id": eid,
                "title": title,
                "content_category": cat,
                "creation_date": cdate,
                "modification_date": cdate,
                "file": {"data": data, "filename": title},
            }

        self.assertListEqual(
            result,
            [
                dmsfile(u"1000", im2, "dmsmainfile", main_cat, u"scan.pdf", b"%PDF-1000"),
                dmsfile(u"1001", im2, "dmsappendixfile", app_cat, u"1001.pdf", b"%PDF-1001"),  # pdf first
                dmsfile(u"1003", im3, "dmsappendixfile", app_cat, u"x.doc", b"DOC-x"),
                {u"_bpk": u"e_mail", u"_eid": u"1006"},
            ],
        )
        self.assertIsInstance(result[0]["file"]["data"], six.binary_type)
        self.assertIsInstance(result[0]["file"]["filename"], six.text_type)
        self.assertEqual(items[2][u"_fs_path"], u"scans/x.doc")
        self.assertEqual(items[1][u"_fs_path"], u"PDF/1001.pdf")  # stored by get_file_content
        self.assertDictEqual(
            storage["data"]["e_dmsfile_unfound"],
            {u"1004": {"err": "no path"}, u"1005": {"err": "missing/1005.pdf not found"}},
        )
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", u"e_file: _eid 1004, Empty file path for mail 202"),
                ("dti", "ERROR", u"e_file: _eid 1005, Cannot open filename 'missing/1005.pdf'"),
            ],
        )
        self.assertDictEqual(
            bp.files,
            {
                u"200": {"lo": 2, "ids": [u"1000", u"1001"]},
                u"201": {"lo": None, "ids": [u"1003"]},
                u"202": {"lo": None, "ids": [u"1004", u"1005"]},
            },
        )
        self.assertDictEqual(dict(storage["course"]), {"t__1_dmsfile_creation": 5})
        # outgoing mails main file, other files
        options = dict(
            self.options,
            bp_key="e_dmsfile_o",
            categories="dmsommainfile annexes_types/outgoing_dms_files/outgoing-scanned-dms-file",
        )
        bp = self.section(
            T1DmsfileCreation,
            "u__1_dmsfile_creation",
            options,
            items[:1],
            self.make_storage("u", e_mail_i=paths, e_files=e_files),
        )
        self.assertEqual(next(iter(bp))["_type"], "dmsommainfile")
        options = dict(self.options, bp_key="e_dmsfile_x")
        bp = self.section(
            T1DmsfileCreation,
            "t__1_dmsfile_creation",
            options,
            items[:1],
            self.make_storage("t", e_mail_i=paths, e_files=e_files),
        )
        self.assertEqual(next(iter(bp))["_type"], "dmsappendixfile")
        # part not run
        bp = self.section(T1DmsfileCreation, "t__1_dmsfile_creation", self.options, items[5:], self.make_storage("a"))
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mail", u"_eid": u"1006"}])


class TestX1ReplyToUpdate(DmsTestCase):

    # data-transfer.cfg.1 x__1_reply_to_update
    options = {
        "condition": "python: modules['imio.transmogrifier.iadocs.utils'].is_in_part(transmogrifier, 'x') "
        "and item.get('_bpk') == u'e_mail_link'",
        "store_key": "e_mail",
    }

    def test___init__(self):
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", self.options, storage=self.make_storage("a"))
        self.assertFalse(hasattr(bp, "catalog"))
        storage = self.make_storage("x", e_mail={})
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", self.options, storage=storage)
        self.assertIs(bp.catalog, getUtility(ICatalog))
        self.assertIs(bp.paths, storage["data"]["e_mail"])
        self.assertIs(bp.batch_store, storage["data"]["reply_to"])
        self.assertEqual(bp.batch_key, u"")
        options = dict(self.options, batch_store="e_mail_link_done", batch_key="_eid")
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", options, storage=storage)
        self.assertIs(bp.batch_store, storage["data"]["e_mail_link_done"])
        self.assertEqual(bp.batch_key, u"_eid")

    def test___iter__(self):
        om1, om2, om3 = self.mail("reponse1"), self.mail("reponse2"), self.mail("reponse3")
        im1, im2, im3 = self.mail("courrier1"), self.mail("courrier2"), self.mail("courrier3")
        iid = self.intids.getId
        # existing links: im1 replies to im3, om2 replies to im1 (back relation), om3 replies to im2
        im1.reply_to = [RelationValue(iid(im3))]
        om2.reply_to = [RelationValue(iid(im1))]
        om3.reply_to = [RelationValue(iid(im2))]
        for obj in (im1, om2, om3):
            notify(ObjectModifiedEvent(obj))
        paths = {
            u"300": {u"path": self.path(om1)},
            u"302": {u"path": self.path(om3)},
            u"200": {u"path": self.path(im1)},
            u"201": {u"path": self.path(im2)},
        }
        storage = self.make_storage("x", e_mail=paths)
        items = [
            {u"_bpk": u"e_mail_link", u"_eid": u"300", u"_target_id": u"200"},
            {u"_bpk": u"e_mail_link", u"_eid": u"302", u"_target_id": u"201"},  # already linked
            {u"_bpk": u"e_mail", u"_eid": u"303"},
        ]
        options = dict(self.options, batch_store="e_mail_link_done", batch_key="_eid")
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", options, items, storage)
        result = list(bp)
        self.assertListEqual(
            [dict((k, v) for k, v in item.items() if k != "reply_to") for item in result],
            [
                {
                    "_eid": u"300",
                    "_target_id": u"200",
                    "_bpk": "reply_to",
                    "_path": self.path(om1),
                    "_type": "dmsoutgoingmail",
                    "_act": "U",
                },
                {u"_bpk": u"e_mail", u"_eid": u"303"},
            ],
        )
        # target, mails linked to the target and mails linking to the target
        self.assertListEqual([rel.to_id for rel in result[0]["reply_to"]], [iid(im1), iid(im3), iid(om2)])
        self.assertDictEqual(storage["data"]["e_mail_link_done"], {u"300": True, u"302": True})
        self.assertDictEqual(dict(storage["course"]), {"x__1_reply_to_update": 2})
        # part not run
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", self.options, items[:1], self.make_storage("a"))
        self.assertListEqual(list(bp), [{u"_bpk": u"e_mail_link", u"_eid": u"300", u"_target_id": u"200"}])


class TestWorkflowHistoryUpdate(DmsTestCase):

    # data-transfer.cfg.1 workflow_history_update, ${config:creator} substituted
    options = {
        "condition": "python: 'csvimport' and item.get('_type') and item.get('_path') "
        "and modules['imio.transmogrifier.iadocs.utils'].is_in_part(transmogrifier, 'dlptu')",
        "owner": "csvimport",
    }

    def test___init__(self):
        bp = self.section(
            WorkflowHistoryUpdate, "workflow_history_update", self.options, storage=self.make_storage("a")
        )
        self.assertEqual(bp.owner_id, "csvimport")
        self.assertFalse(bp.condition({u"_type": "dmsincomingmail", u"_path": u"/x"}))  # part a
        bp = self.section(
            WorkflowHistoryUpdate, "workflow_history_update", self.options, storage=self.make_storage("l")
        )
        self.assertTrue(bp.condition({u"_type": "dmsincomingmail", u"_path": u"/x"}))

    def test___iter__(self):
        im2 = self.mail("courrier2")
        api.content.transition(obj=im2, transition="propose_to_manager")
        wkf = list(im2.workflow_history)[0]
        before = [dict(status) for status in im2.workflow_history[wkf]]
        self.assertGreater(len(before), 1)
        self.assertNotEqual(set(status["actor"] for status in before), {"csvimport"})
        items = [
            {u"_bpk": u"e_mail_i", u"_eid": u"200", u"_type": "dmsincomingmail", u"_path": self.path(im2)},
            {u"_bpk": u"e_mail_i", u"_eid": u"201", u"_type": "dmsincomingmail"},
        ]
        storage = self.make_storage("l")
        bp = self.section(WorkflowHistoryUpdate, "workflow_history_update", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {u"_bpk": u"e_mail_i", u"_eid": u"200", u"_type": "dmsincomingmail", u"_path": self.path(im2)},
                {u"_bpk": u"e_mail_i", u"_eid": u"201", u"_type": "dmsincomingmail"},
            ],
        )
        after = im2.workflow_history[wkf]
        self.assertIsInstance(after, tuple)
        self.assertListEqual([status["actor"] for status in after], ["csvimport"] * len(before))
        self.assertListEqual(
            [status["review_state"] for status in after], [status["review_state"] for status in before]
        )
        self.assertDictEqual(dict(storage["course"]), {"workflow_history_update": 1})


class TestXmlContactStore(DmsTestCase):

    # data-transfer.cfg.2 i__2_xml_contact_store (less columns)
    options = {
        "bp_key": "e_contact",
        "condition": "python: item.get('_bpk') == u'e_contact' and '_xml' in item",
        "source_key": "_xml",
        "xml_contact_keys": "Sender|Addressee",
        "xml_contact_cols": "Organization:OrgId:_parent_id Name _organization Level1 _division Address _street "
        "ZipCode _pc City _city |\n                   Contact:PersId: FirstName firstname LastName lastname "
        "Email _email1",
        "check_key_uniqueness": "0",
        "empty_store": "0",
        "yield_original": "0",
    }
    # data-transfer.cfg.2 m__2_archives_xml_contact_store
    options2 = {
        "bp_key": "e_contact",
        "condition": "python: item.get('_bpk') == u'e_mail_recip_i' and '_xml_a' in item",
        "source_key": "_xml_a",
        "xml_contact_keys": "Expediteur",
        "xml_contact_cols": ":K2ID: Organisme _organization Nom lastname",
        "contact_id_key": "_sender_id",
        "empty_store": "1",
    }
    xml = (
        u'<?xml version="1.0" encoding="utf-8"?><Contacts>'
        u"<Sender><Organization><OrgId>12</OrgId><Name>Commune de Colfontaine</Name><Level1>Travaux</Level1>"
        u"<Address>Rue Haute 1</Address><ZipCode>7340</ZipCode><City>Colfontaine</City></Organization>"
        u"<Contact><PersId>34</PersId><FirstName>Jean</FirstName><LastName>Dupont</LastName><Email> </Email>"
        u"</Contact></Sender>"
        u"<Addressee><Organization><OrgId>13</OrgId><Name></Name></Organization>"
        u"<Contact><PersId>35</PersId><LastName>Martin</LastName></Contact></Addressee></Contacts>"
    )

    def test___init__(self):
        storage = self.make_storage("a")
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, storage=storage)
        self.assertEqual(bp.bp_key, u"e_contact")
        self.assertFalse(hasattr(bp, "source_key"))
        self.assertNotIn("e_contact", storage["data"])
        storage = self.make_storage("i")
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, storage=storage)
        self.assertEqual(bp.source_key, u"_xml")
        self.assertListEqual(bp.xml_contact_keys, [u"Sender", u"Addressee"])
        self.assertDictEqual(
            bp.xml_ct_cols,
            {
                u"Organization:OrgId:_parent_id": [
                    (u"Name", u"_organization"),
                    (u"Level1", u"_division"),
                    (u"Address", u"_street"),
                    (u"ZipCode", u"_pc"),
                    (u"City", u"_city"),
                ],
                u"Contact:PersId:": [(u"FirstName", u"firstname"), (u"LastName", u"lastname"), (u"Email", u"_email1")],
            },
        )
        self.assertFalse(bp.empty_store)
        self.assertEqual(bp.contact_id_key, u"")
        self.assertFalse(bp.cku)
        self.assertFalse(bp.yield_original)
        self.assertDictEqual(storage["data"]["e_contact"], {})
        storage["data"]["e_contact"] = {u"1": {}}
        bp = self.section(
            XmlContactStore,
            "m__2_archives_xml_contact_store",
            self.options2,
            storage=self.make_storage("m", e_contact={u"1": {}}),
        )
        self.assertDictEqual(bp.storage["data"]["e_contact"], {u"1": {}})  # not emptied at init
        self.assertTrue(bp.empty_store)
        self.assertTrue(bp.cku)
        self.assertTrue(bp.yield_original)
        self.assertEqual(bp.contact_id_key, u"_sender_id")
        # Plone 4 behaviour (bug?): a string is raised instead of an exception
        options = dict(self.options2, xml_contact_cols="K2ID Organisme _organization")
        self.assertRaises(
            TypeError,
            self.section,
            XmlContactStore,
            "m__2_archives_xml_contact_store",
            options,
            storage=self.make_storage("m"),
        )
        options = dict(self.options2, xml_contact_cols=":K2ID: Organisme")
        self.assertRaises(
            ValueError,
            self.section,
            XmlContactStore,
            "m__2_archives_xml_contact_store",
            options,
            storage=self.make_storage("m"),
        )

    def test___iter__(self):
        storage = self.make_storage("i")
        items = [{u"_bpk": u"e_contact", u"_eid": u"1", u"_xml": self.xml}, {u"_bpk": u"e_contact", u"_eid": u"2"}]
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, items, storage)
        self.assertListEqual(list(bp), [{u"_bpk": u"e_contact", u"_eid": u"2"}])
        e_contact = storage["data"]["e_contact"]
        self.assertListEqual(sorted(e_contact), [u"12", u"34", u"35"])  # org 13 without value is not stored
        self.assertDictEqual(
            e_contact[u"12"],
            {
                u"_organization": u"Commune de Colfontaine",
                u"_division": u"Travaux",
                u"_street": u"Rue Haute 1",
                u"_pc": u"7340",
                u"_city": u"Colfontaine",
            },
        )
        self.assertIsInstance(e_contact[u"12"][u"_organization"], six.text_type)
        # the organization id is linked on the contact only if the organization is handled before (dict order)
        org_first = list(bp.xml_ct_cols)[0].startswith(u"Organization")
        expected = {u"firstname": u"Jean", u"lastname": u"Dupont", u"_email1": u""}
        if org_first:
            expected[u"_parent_id"] = u"12"
        self.assertDictEqual(e_contact[u"34"], expected)
        self.assertDictEqual(e_contact[u"35"], {u"firstname": u"", u"lastname": u"Martin", u"_email1": u""})
        self.assertDictEqual(dict(storage["course"]), {"i__2_xml_contact_store": 1})
        # m__2_archives_xml_contact_store: contact id on item, store emptied, item yielded for each contact
        xml = (
            u"<Archive><Expediteur><K2ID>5</K2ID><Organisme>SWDE</Organisme><Nom>Robinet</Nom></Expediteur>"
            u"<Expediteur><K2ID>6</K2ID><Nom>Courant</Nom></Expediteur></Archive>"
        )
        storage = self.make_storage("m", e_contact={u"1": {}})
        items = [
            {u"_bpk": u"e_mail_recip_i", u"_eid": u"1", u"_xml_a": xml},
            {u"_bpk": u"e_mail_recip_i", u"_eid": u"2", u"_xml_a": u"<Archive></Archive>"},
        ]
        bp = self.section(XmlContactStore, "m__2_archives_xml_contact_store", self.options2, items, storage)
        result = [(item[u"_eid"], item[u"_sender_id"], dict(storage["data"]["e_contact"])) for item in bp]
        self.assertListEqual(
            result,
            [
                (u"1", u"5", {u"5": {u"_organization": u"SWDE", u"lastname": u"Robinet"}}),
                (u"1", u"6", {u"6": {u"_organization": u"", u"lastname": u"Courant"}}),
                # Plone 4 behaviour (bug?): an item without contact tag is not yielded (yield_original)
            ],
        )
        # key uniqueness check
        xml = (
            u"<Archive><Expediteur><K2ID>5</K2ID><Nom>Robinet</Nom></Expediteur>"
            u"<Expediteur><K2ID>5</K2ID><Nom>Robinet bis</Nom></Expediteur>"
            u"<Expediteur><K2ID>0</K2ID><Nom>Zéro</Nom></Expediteur>"
            u"<Expediteur><K2ID>0</K2ID><Nom>Zéro bis</Nom></Expediteur></Archive>"
        )
        storage = self.make_storage("m")
        options = dict(self.options2, empty_store="0", yield_original="0")
        items = [{u"_bpk": u"e_mail_recip_i", u"_eid": u"3", u"_xml_a": xml}]
        bp = self.section(XmlContactStore, "m__2_archives_xml_contact_store", options, items, storage)
        self.assertListEqual(list(bp), [])
        self.assertDictEqual(
            storage["data"]["e_contact"],
            {
                u"5": {u"_organization": u"", u"lastname": u"Robinet bis"},
                u"0": {u"_organization": u"", u"lastname": u"Zéro"},
                u"1": {u"_organization": u"", u"lastname": u"Zéro bis"},
            },
        )
        self.assertEqual(items[0][u"_sender_id"], u"1")
        self.assertIn(
            ("dti", "ERROR", u"e_mail_recip_i: _eid 3, Key '5' already in 'e_contact' data dict"), self.logs.records
        )
        # part not run
        items = [{u"_bpk": u"e_contact", u"_eid": u"1", u"_xml": self.xml}]
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, items, self.make_storage("a"))
        self.assertEqual(len(list(bp)), 1)


class TestMavalHandling(DmsTestCase):

    # data-transfer.cfg.4 m__4_maval_i and its [config] fields option (extract)
    options = {
        "condition": "python: item.get('_bpk') == u'e_mail_maval_i'",
        "bp_key": "e_maval_i",
        "store_key": "e_mail_i",
        "profile_keys": "e_mails_prd _prd",
    }
    config = {
        "default_i_mail_type": "courrier",
        "default_o_mail_type": "lettre",
        "fields": """ENTRANT NOM description,data_transfer "after:"
         ENTRANT ADRESSE data_transfer "after:NOM"
         ENTRANT LOCALITE data_transfer "after:ADRESSE"
         ENTRANT CODEABARRE internal_reference_no "format:CB{}"
         ENTRANT DATEDOC original_mail_date "date:%Y%m%d"
         ENTRANT TYPEDOC mail_type ""
         ENTRANT SERVDEST treating_groups "USERTRAIT"
         ENTRANT USERTRAIT assigned_user "SERVDEST"
         ENTRANT COPIES recipient_groups ""
         ENTRANT REGCE - ""
         ENTRANT RÉSUMÉ description ""
         TVX REMARQUE data_transfer \"\"""",
    }
    e_palib = {
        u"PROFILINF0": {
            u"NOM": {u"_val": u"Nom"},
            u"ADRESSE": {u"_val": u"Adresse"},
            u"LOCALITE": {u"_val": u"Localité"},
            u"USERTRAIT": {u"_val": u"Agent traitant"},
        },
        u"PARAMUTIL": {u"FDPAP": {u"_val": u"Papier"}},
    }

    def maval(self, parts="m", **data):
        dic = {"e_mail_i": {}, "e_mails_prd": {}, "e_palib": self.e_palib}
        dic.update(data)
        storage = self.make_storage(parts, **dic)
        return self.section(MavalHandling, "m__4_maval_i", self.options, storage=storage, config=self.config)

    def test___init__(self):
        bp = self.section(
            MavalHandling, "m__4_maval_i", self.options, storage=self.make_storage("a"), config=self.config
        )
        self.assertEqual(bp.bp_key, u"e_maval_i")
        self.assertFalse(hasattr(bp, "fields"))
        bp = self.maval()
        self.assertIs(bp.im_paths, bp.storage["data"]["e_mail_i"])
        self.assertIs(bp.batch_store, bp.storage["data"]["e_maval_i"])
        self.assertEqual(len(bp.flds_opt), 12)
        self.assertEqual(bp.flds_opt[0], (u"ENTRANT", u"NOM", u"description,data_transfer", u"after:"))
        self.assertTrue(all(isinstance(val, six.text_type) for tup in bp.flds_opt for val in tup))
        # "after:" transforms contain the full chain of previous fields
        self.assertDictEqual(
            bp.fields,
            {
                u"ENTRANT": {
                    u"NOM": (u"description,data_transfer", u"after:"),
                    u"ADRESSE": (u"data_transfer", u"after:NOM"),
                    u"LOCALITE": (u"data_transfer", u"after:ADRESSE,NOM"),
                    u"CODEABARRE": (u"internal_reference_no", u"format:CB{}"),
                    u"DATEDOC": (u"original_mail_date", u"date:%Y%m%d"),
                    u"TYPEDOC": (u"mail_type", u""),
                    u"SERVDEST": (u"treating_groups", u"USERTRAIT"),
                    u"USERTRAIT": (u"assigned_user", u"SERVDEST"),
                    u"COPIES": (u"recipient_groups", u""),
                    u"REGCE": (u"-", u""),
                    u"RÉSUMÉ": (u"description", u""),
                },
                u"TVX": {u"REMARQUE": (u"data_transfer", u"")},
            },
        )
        self.assertListEqual(bp.profile_keys, [u"e_mails_prd", u"_prd"])
        self.assertIs(bp.e_all_items, bp.storage["data"]["e_mails_prd"])
        self.assertIs(bp.e_palib, bp.storage["data"]["e_palib"])
        self.assertDictEqual(bp.e_papri, {})
        self.assertDictEqual(bp.service_match, {})
        self.assertEqual((bp.e_type, bp.default_mt), ("E", "courrier"))
        self.assertIn(self.uid(PLONEGROUP + "/direction-generale/secretariat"), bp.p_u_s_editor["agent"])
        self.assertListEqual(bp.p_u_s_editor["chef"], [])
        bp = self.section(
            MavalHandling,
            "m__4_maval_i",
            dict(self.options, functions="OM"),
            storage=self.make_storage("m", e_mail_i={}),
            config=self.config,
        )
        self.assertEqual((bp.e_type, bp.default_mt), ("S", "lettre"))
        self.assertIn(self.uid(PLONEGROUP + "/direction-financiere"), bp.p_u_s_editor["chef"])

    def test___iter__(self):  # noqa C901
        im2, im3, im4, im6 = (self.mail(mid) for mid in ("courrier2", "courrier3", "courrier4", "courrier6"))
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        grh_uid = self.uid(PLONEGROUP + "/direction-generale/grh")
        paths = {
            u"200": {u"path": self.path(im2)},
            u"201": {u"path": self.path(im3)},
            u"202": {u"path": u"/incoming-mail/unknown"},
            u"203": {u"path": self.path(im4)},
            u"204": {u"path": self.path(im6)},
        }
        storage = self.make_storage(
            "m",
            e_mail_i=paths,
            e_mails_prd={
                u"200": {u"_prd": u"ENTRANT"},
                u"201": {u"_prd": u"TVX"},
                u"202": {u"_prd": u"ENTRANT"},
                u"203": {u"_prd": u"ENTRANT"},
                u"204": {u"_prd": u"ENTRANT"},
            },
            e_palib=self.e_palib,
            e_service_match={u"S1": {u"uid": secr_uid}, u"S2": {u"uid": grh_uid}},
            e_mailtype_match={u"T1": {u"E": {u"_key": u"facture"}}},
            e_user_match={
                u"U1": {u"_p_userid": u"agent", u"_nom": u"Agent"},
                u"U4": {u"_p_userid": u"chef", u"_nom": u"Chef"},
                u"U9": {u"_p_userid": None, u"_nom": u"DELETED"},
            },
        )

        def mv(eid, fld, val, **kw):
            dic = {u"_bpk": u"e_mail_maval_i", u"_eid": eid, u"_fld": fld, u"_val": val}
            dic.update(kw)
            return dic

        def upd(eid, fld, mail, **kw):
            dic = {
                "_eid": eid,
                "_path": self.path(mail),
                "_type": "dmsincomingmail",
                "_bpk": "i_maval_{}".format(fld),
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            mv(u"200", u"NOM", u"Dupont"),
            mv(u"200", u"UNKNOWN", u"x"),
            mv(u"200", u"REGCE", u"x"),  # ignored field
            mv(u"202", u"NOM", u"x"),  # mail not found
            mv(u"200", u"TYPEDOC", u"T1"),
            mv(u"200", u"TYPEDOC", u"T9"),  # default type
            mv(u"200", u"CODEABARRE", u"123"),
            mv(u"200", u"DATEDOC", u"20230415"),
            mv(u"200", u"DATEDOC", u"2023"),
            mv(u"200", u"COPIES", u" S1 S9 "),
            mv(u"200", u"NOM", u"X", _replace=u"title"),
            mv(u"201", u"REMARQUE", u"Urgent"),
            {u"_bpk": u"e_mail_i", u"_eid": u"205"},
        ]
        bp = self.section(MavalHandling, "m__4_maval_i", self.options, items, storage, config=self.config)
        self.assertListEqual(
            list(bp),
            [
                upd(
                    u"200",
                    u"NOM",
                    im2,
                    description=u"Nom: Dupont\r\nCeci est la description du courrier 2",
                    data_transfer=u"Nom: Dupont",
                ),
                upd(u"200", u"TYPEDOC", im2, mail_type=u"facture"),
                upd(u"200", u"TYPEDOC", im2, mail_type="courrier"),
                upd(u"200", u"CODEABARRE", im2, internal_reference_no=u"CB123"),
                upd(u"200", u"DATEDOC", im2, original_mail_date=date(2023, 4, 15)),
                # Plone 4 behaviour (bug?): continue in the fields loop, the item is yielded with a None value
                upd(u"200", u"DATEDOC", im2, original_mail_date=None),
                upd(u"200", u"COPIES", im2, recipient_groups=[secr_uid]),
                upd(u"200", u"NOM", im2, title=u"X"),
                upd(u"201", u"REMARQUE", im3, data_transfer=u"REMARQUE: Urgent"),  # no label: field name
                {u"_bpk": u"e_mail_i", u"_eid": u"205"},
            ],
        )
        errors = [rec[2] for rec in self.logs.records if rec[0] == "dti"]
        self.assertEqual(errors[0], u"e_mail_maval_i: _eid 200, 'ENTRANT', 'UNKNOWN' not found in fields config")
        self.assertTrue(errors[1].startswith(u"e_mail_maval_i: _eid 200, not a valid date '2023' in key 'k': "))
        self.assertEqual(
            errors[2],
            u"e_mail_maval_i: _eid 200, None value for field 'DATEDOC', p_fld "
            u"'original_mail_date', transform 'date:%Y%m%d'",
        )
        self.assertEqual(len(errors), 3)
        self.assertIn(("dto", "WARNING", "mail 202: path '/incoming-mail/unknown' not found"), self.logs.records)
        self.assertListEqual(
            sorted(storage["data"]["e_maval_i"]),
            [
                u"200|CODEABARRE",
                u"200|COPIES",
                u"200|DATEDOC",
                u"200|NOM",
                u"200|REGCE",
                u"200|TYPEDOC",
                u"200|UNKNOWN",
                u"201|REMARQUE",
                u"202|NOM",
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"m__4_maval_i": 12})

        # treating group and assigned user, in both orders; the next sections update the mail (simulated)
        items = [
            mv(u"200", u"USERTRAIT", u"U1"),  # user stored temporarily in data_transfer
            mv(u"200", u"SERVDEST", u"S2"),  # treating group changed, stored user assigned
            mv(u"203", u"SERVDEST", u"S1"),
            mv(u"203", u"USERTRAIT", u"U4"),  # not an editor: name in data_transfer
            mv(u"203", u"USERTRAIT", u"U9"),  # deleted user
            mv(u"203", u"USERTRAIT", u"U1"),
            mv(u"204", u"USERTRAIT", u"U4"),
            mv(u"204", u"SERVDEST", u"S1"),  # stored user not an editor: name in data_transfer
        ]
        bp = self.section(MavalHandling, "m__4_maval_i", self.options, items, storage, config=self.config)
        gen = iter(bp)
        item2 = next(gen)
        self.assertDictEqual(item2, upd(u"200", u"USERTRAIT", im2, data_transfer=u"Agent traitant: U1"))
        im2.data_transfer = item2["data_transfer"]
        self.assertDictEqual(
            next(gen), upd(u"200", u"SERVDEST", im2, treating_groups=grh_uid, data_transfer=u"", assigned_user=u"agent")
        )
        item2 = next(gen)
        self.assertDictEqual(item2, upd(u"203", u"SERVDEST", im4, treating_groups=secr_uid))
        im4.treating_groups = item2["treating_groups"]
        self.assertDictEqual(next(gen), upd(u"203", u"USERTRAIT", im4, data_transfer=u"Agent traitant: Chef"))
        self.assertDictEqual(next(gen), upd(u"203", u"USERTRAIT", im4))
        self.assertDictEqual(next(gen), upd(u"203", u"USERTRAIT", im4, assigned_user=u"agent"))
        item2 = next(gen)
        im6.data_transfer = item2["data_transfer"]
        # Plone 4 behaviour (bug?): the temporary line is removed, giving an empty text, then the name is added
        # to the mail text (empty text_fld) where the temporary line is still present
        self.assertDictEqual(
            next(gen),
            upd(
                u"204",
                u"SERVDEST",
                im6,
                treating_groups=secr_uid,
                data_transfer=u"Agent traitant: U4\r\nAgent traitant: Chef",
            ),
        )
        self.assertListEqual(list(gen), [])
        # part not run
        bp = self.section(
            MavalHandling, "m__4_maval_i", self.options, items[:1], self.make_storage("a"), config=self.config
        )
        self.assertListEqual(list(bp), [mv(u"200", u"USERTRAIT", u"U1")])

    def test_text_field(self):
        bp = self.maval()
        im2 = self.mail("courrier2")
        # add at the first position
        item2 = {}
        self.assertIsNone(bp.text_field({u"_fld": u"NOM", u"_val": u"Dupont"}, item2, im2, "description", u"after:"))
        self.assertDictEqual(item2, {"description": u"Nom: Dupont\r\nCeci est la description du courrier 2"})
        bp.text_field({u"_fld": u"NOM", u"_val": u"Dupont"}, item2, im2, "data_transfer", u"after:")
        self.assertEqual(item2["data_transfer"], u"Nom: Dupont")
        self.assertIsInstance(item2["data_transfer"], six.text_type)
        # add after the first found previous field, from a given text
        text = u"Nom: Dupont\r\nRemarque: x"
        bp.text_field(
            {u"_fld": u"LOCALITE", u"_val": u"Mons"}, item2, im2, "data_transfer", u"after:ADRESSE,NOM", text_fld=text
        )
        self.assertEqual(item2["data_transfer"], u"Nom: Dupont\r\nLocalité: Mons\r\nRemarque: x")
        bp.text_field(
            {u"_fld": u"LOCALITE", u"_val": u"Mons"}, item2, im2, "data_transfer", u"after:ADRESSE", text_fld=text
        )
        self.assertEqual(item2["data_transfer"], u"Nom: Dupont\r\nRemarque: x\r\nLocalité: Mons")  # not found: end
        # add at the end, label not defined, given value, given label key
        bp.text_field({u"_fld": u"REMARQUE", u"_val": u"y"}, item2, im2, "data_transfer", u"", text_fld=text)
        self.assertEqual(item2["data_transfer"], u"Nom: Dupont\r\nRemarque: x\r\nREMARQUE: y")
        bp.text_field(
            {u"_fld": u"SERVDEST", u"_val": u"S1"},
            item2,
            im2,
            "data_transfer",
            u"",
            lib_key=u"USERTRAIT",
            value=u"Chef",
            text_fld=text,
        )
        self.assertEqual(item2["data_transfer"], u"Nom: Dupont\r\nRemarque: x\r\nAgent traitant: Chef")
        # remove
        text = u"Nom: Dupont\r\nAgent traitant: U1"
        item2 = {}
        ret = bp.text_field(
            {u"_fld": u"SERVDEST", u"_val": u"S1"},
            item2,
            im2,
            "data_transfer",
            u"",
            action="remove",
            lib_key=u"USERTRAIT",
            text_fld=text,
        )
        self.assertEqual(ret, u"U1")
        self.assertDictEqual(item2, {"data_transfer": u"Nom: Dupont"})
        ret = bp.text_field(
            {u"_fld": u"ADRESSE", u"_val": u""}, item2, im2, "data_transfer", u"", action="remove", text_fld=text
        )
        self.assertIsNone(ret)
        self.assertDictEqual(item2, {"data_transfer": text})

    def test_assigned_user(self):
        bp = self.maval()
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        df_uid = self.uid(PLONEGROUP + "/direction-financiere")
        item2 = {}
        self.assertFalse(bp.assigned_user(item2, df_uid, "agent"))
        self.assertFalse(bp.assigned_user(item2, secr_uid, None))
        self.assertFalse(bp.assigned_user(item2, None, "agent"))
        self.assertDictEqual(item2, {})
        self.assertTrue(bp.assigned_user(item2, secr_uid, "agent"))
        self.assertDictEqual(item2, {"assigned_user": "agent"})

    def test_transform_value(self):
        bp = self.maval(
            e_papri={
                u"FORMAT_DOC": {u"_prienc": u"CBO", u"_prizon": u"FD"},
                u"OTHER": {u"_prienc": u"TXT", u"_prizon": u"OT"},
            }
        )

        def mv(val):
            return {u"_bpk": u"e_mail_maval_i", u"_eid": u"200", u"_fld": u"DATEDOC", u"_val": val}

        self.assertEqual(bp.transform_value(mv(u"20230415"), u""), u"20230415")
        self.assertEqual(bp.transform_value(mv(u"20230415"), u"date:%Y%m%d"), date(2023, 4, 15))
        self.assertEqual(bp.transform_value(mv(u"20230415"), u"datetime:%Y%m%d"), datetime(2023, 4, 15))
        item = mv(u"2023")
        self.assertIsNone(bp.transform_value(item, u"date:%Y%m%d"))
        self.assertNotIn(u"_error", item)  # the error is set on a copy
        self.assertIsNone(bp.transform_value(mv(u"18991231"), u"date:%Y%m%d"))  # before 1900
        self.assertIsNone(bp.transform_value(mv(u""), u"date:%Y%m%d"))
        self.assertEqual(bp.transform_value(mv(u"123"), u"format:CB{}"), u"CB123")
        self.assertEqual(bp.transform_value(mv(u"PAP"), u"e_papri:FORMAT_DOC"), u"Papier")
        self.assertEqual(bp.transform_value(mv(u"PAP"), u"e_papri:OTHER"), u"PAP")
        self.assertEqual(bp.transform_value(mv(u"PAP"), u"other:x"), u"PAP")
