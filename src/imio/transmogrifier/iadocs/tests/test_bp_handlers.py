"""Blueprints tests for this package."""

from Acquisition import aq_parent
from collections import OrderedDict
from collective.contact.plonegroup.config import get_registry_organizations
from collective.contact.plonegroup.config import set_registry_organizations
from collective.iconifiedcategory.utils import calculate_category_id
from collective.labels.interfaces import ILabeling
from datetime import date
from datetime import datetime
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
from plone.base.utils import safe_text
from plone.dexterity.fti import DexterityFTIModificationDescription
from plone.dexterity.fti import ftiModified
from z3c.relationfield import RelationValue
from zc.relation.interfaces import ICatalog
from zope.component import getUtility
from zope.event import notify
from zope.intid import IIntIds
from zope.lifecycleevent import ObjectModifiedEvent

import logging
import os
import pickle
import shutil
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
            "1": {"lastname": "Dupont", "firstname": "Jean", "_street": "Rue Haute", "_city": "Colfontaine"}
        }
        item = {"_cid": "1", "_free": "Contact libre\nsuite"}
        # two destinations: name in both, address only in the secondary one
        desc, d_t = [], []
        self.assertTrue(get_contact_info(section, item, "EXPÉDITEUR", "_cid", "_free", desc, d_t))
        self.assertEqual(desc, ["EXPÉDITEUR: Dupont Jean.", "EXPÉDITEUR LIBRE: Contact libre"])
        self.assertEqual(
            d_t,
            [
                "EXPÉDITEUR: Dupont Jean.",
                "ADRESSE EXPÉDITEUR: Rue Haute Colfontaine.",
                "EXPÉDITEUR LIBRE: Contact libre, suite",
            ],
        )
        # one single destination: no duplicated line, full free text kept
        desc = []
        self.assertTrue(get_contact_info(section, item, "EXPÉDITEUR", "_cid", "_free", desc, desc))
        self.assertEqual(
            desc,
            [
                "EXPÉDITEUR: Dupont Jean.",
                "ADRESSE EXPÉDITEUR: Rue Haute Colfontaine.",
                "EXPÉDITEUR LIBRE: Contact libre, suite",
            ],
        )

    def test_get_contact_name(self):
        # person, without parent
        self.assertEqual(
            get_contact_name(
                {
                    "lastname": "Dupont",
                    "firstname": "Jean",
                    "_email1": None,
                    "_email2": "j@d.be",
                    "_function": "Directeur",
                    "enterprise_number": "0123",
                },
                {},
            ),
            (["Dupont Jean", "j@d.be", "Directeur", "NE:0123"], []),
        )
        # same values as parent are not repeated
        org = {"lastname": "Commune", "_email1": "info@c.be", "enterprise_number": "0999"}
        self.assertEqual(get_contact_name(dict(org), org), ([], ["Commune", "info@c.be", "NE:0999"]))
        self.assertEqual(
            get_contact_name({"lastname": "Service", "_email3": "s@c.be", "enterprise_number": "0998"}, org),
            (["Service", "s@c.be", "NE:0998"], ["Commune", "info@c.be", "NE:0999"]),
        )
        # customer 2: organization levels
        parent = {"_organization": "Commune", "_division": "Travaux", "_service": "Voiries", "_level3": ""}
        dic = {"_organization": "", "_division": "", "lastname": "Martin", "firstname": "Paul"}
        self.assertEqual(
            get_contact_name(dic, parent), (["Martin, Paul"], ["Commune, Division: Travaux, Service: Voiries"])
        )
        # second name used when no name
        self.assertEqual(get_contact_name({"_name2": "Service X"}, {"_name2": "Parent"}), (["Service X"], ["Parent"]))
        self.assertEqual(get_contact_name({}, {}), ([], []))

    def test_get_contact_phone(self):
        self.assertListEqual(
            get_contact_phone({"_phone2": "1", "_cell3": "2"}, {"_phone1": "3", "_cell1": "4"}), ["1", "2"]
        )
        self.assertListEqual(get_contact_phone({"_phone1": None}, {"_phone1": "3", "_cell1": "4"}), ["3", "4"])
        self.assertListEqual(get_contact_phone({}, {}), [])

    def test_get_contact_info(self):
        section = self
        self.storage.setdefault("data", {})["e_contact"] = {
            "1": {
                "lastname": "Dupont",
                "firstname": "Jean",
                "_parent_id": "2",
                "_function": "Gérant",
                "_company": "Dupont SPRL",
                "_other": "Tél. après 17h",
            },
            "2": {
                "lastname": "Électricité SA",
                "_street": "Rue Basse",
                "_pc": "7000",
                "_city": "Mons",
                "_phone1": "065/1",
                "_cell1": "0475/1",
            },
            "3": {"lastname": "Dupont SPRL", "_company": "Dupont SPRL"},
        }
        # parent information, company, parent address and phones, complement
        desc, d_t = [], []
        item = {"_cid": "1", "_free": None}
        self.assertTrue(get_contact_info(section, item, "EXPÉDITEUR", "_cid", "_free", desc, d_t))
        self.assertListEqual(desc, ["EXPÉDITEUR PARENT: Électricité SA.", "EXPÉDITEUR: Dupont Jean, Gérant."])
        self.assertListEqual(
            d_t,
            [
                "EXPÉDITEUR PARENT: Électricité SA.",
                "EXPÉDITEUR: Dupont Jean, Gérant.",
                "ORGANISATION: Dupont SPRL.",
                "ADRESSE EXPÉDITEUR PARENT: Rue Basse 7000 Mons.",
                "TÉL EXPÉDITEUR: 065/1, 0475/1.",
                "COMPLÉMENT EXPÉDITEUR: Tél. après 17h",
            ],
        )
        self.assertTrue(all(isinstance(line, str) for line in desc + d_t))
        # empty related label, company equal to the name
        desc, d_t = [], []
        self.assertTrue(
            get_contact_info(section, {"_cid": "3"}, "DESTINATAIRE", "_cid", "", desc, d_t, related_label="")
        )
        self.assertListEqual(desc, ["DESTINATAIRE: Dupont SPRL."])
        self.assertListEqual(d_t, ["DESTINATAIRE: Dupont SPRL."])
        # nothing: no contact id field, only quotes in free text
        desc, d_t = [], []
        self.assertFalse(
            get_contact_info(section, {"_cid": "1", "_free": '""\n'}, "EXPÉDITEUR", "", "_free", desc, d_t)
        )
        self.assertFalse(get_contact_info(section, {"_cid": None}, "EXPÉDITEUR", "_cid", "", desc, d_t))
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
        storage = self.make_storage("a", e_service_match={"10": {"uid": "x"}})
        bp = self.section(AServiceUpdate, "a__service_update", self.options, storage=storage)
        self.assertTrue(bp.condition({"_bpk": "e_service"}))
        self.assertFalse(bp.condition({"_bpk": "e_user"}))
        self.assertIs(bp.all_orgs, storage["data"]["p_orgs_all"])
        self.assertIs(bp.eid_to_orgs, storage["data"]["p_eid_to_orgs"])
        self.assertIs(bp.match, storage["data"]["e_service_match"])

    def test___iter__(self):
        secr_path = PLONEGROUP + "/direction-generale/secretariat"
        secr = self.obj(secr_path)
        grh = self.obj(PLONEGROUP + "/direction-generale/grh")
        grh.internal_number = "5"  # already linked in a previous run
        # part not run: items are yielded unchanged
        bp = self.section(AServiceUpdate, "a__service_update", self.options, [{"_eid": "10"}], self.make_storage("b"))
        self.assertListEqual(list(bp), [{"_eid": "10"}])
        # empty match: all services are passed
        storage = self.make_storage("a", e_service_match={})
        items = [{"_bpk": "e_service", "_eid": "10"}, {"_bpk": "e_user", "_eid": "10"}]
        bp = self.section(AServiceUpdate, "a__service_update", self.options, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_user", "_eid": "10"}])
        # with matching file
        match = {
            "10": {"etitle": "Secrétariat communal", "uid": secr.UID(), "title": "Secrétariat"},
            "11": {"etitle": "Empty", "uid": "", "title": ""},
            "12": {"etitle": "Unknown", "uid": "abcd", "title": ""},
        }
        storage = self.make_storage("a", e_service_match=match)
        self.assertDictEqual(storage["data"]["p_eid_to_orgs"], {"5": {"uid": grh.UID()}})
        items = [
            {"_bpk": "e_service", "_eid": "5", "_title": "GRH"},
            {"_bpk": "e_service", "_eid": "9", "_title": "Not matched"},
            {"_bpk": "e_service", "_eid": "11", "_title": "Empty"},
            {"_bpk": "e_service", "_eid": "12", "_title": "Unknown"},
            {"_bpk": "e_service", "_eid": "10", "_title": "Secrétariat communal"},
            {"_bpk": "e_service", "_eid": "10", "_title": "Duplicated"},
            {"_bpk": "e_user", "_eid": "10"},
        ]
        bp = self.section(AServiceUpdate, "a__service_update", self.options, items, storage)
        gen = iter(bp)
        updated = next(gen)
        self.assertDictEqual(
            updated,
            {
                "_bpk": "e_service",
                "_eid": "10",
                "_title": "Secrétariat communal",
                "_type": "organization",
                "_path": secr_path,
                "internal_number": "10",
            },
        )
        self.assertIsInstance(updated["internal_number"], str)
        secr.internal_number = updated["internal_number"]  # stored by the next sections (schemaupdater)
        self.assertListEqual(
            list(gen),
            [{"_bpk": "e_service", "_eid": "10", "_title": "Duplicated"}, {"_bpk": "e_user", "_eid": "10"}],
        )
        self.assertNotIn("_error", items[0])
        self.assertTrue(items[1]["_error"])
        self.assertTrue(items[2]["_error"])
        self.assertTrue(items[3]["_error"])
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", "e_service: _eid 9, Not in matching file"),
                ("dti", "ERROR", "e_service: _eid 11, Cannot find uid '' matched with service"),
                ("dti", "ERROR", "e_service: _eid 12, Cannot find uid 'abcd' matched with service"),
            ],
        )
        # plonegroup services are reloaded in storage at the end
        self.assertIn(("dto", "INFO", "Part a: some services have been updated."), self.logs.records)
        self.assertDictEqual(storage["data"]["p_eid_to_orgs"], {"5": {"uid": grh.UID()}, "10": {"uid": secr.UID()}})
        self.assertListEqual(storage["data"]["p_orgs_all"][secr.UID()]["eids"], ["10"])
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
        self.assertEqual(bp.related_storage, "e_used_mailtype")
        self.assertIsInstance(bp.related_storage, str)

    def test___iter__(self):
        # e_used_mailtype: _source_id => _etype (E: incoming, S: outgoing) => used mails
        used = {"5": {"E": {"_eid": "100"}, "S": {"_eid": "101"}}, "6": {"E": {"_eid": "102"}}}
        items = [
            {"_bpk": "e_mailtype", "_eid": "5", "_etitle": "Courrier"},
            {"_bpk": "e_mailtype", "_eid": "6", "_etitle": "Facture"},
            {"_bpk": "e_service", "_eid": "7"},
        ]
        bp = self.section(
            BMailtypesByType, "b__1_mailtype_by_type", self.options, items, self.make_storage("b", e_used_mailtype=used)
        )
        # _etype is read when yielded: the same dict is yielded once per type
        yielded = [(item["_eid"], item.get("_etype"), item) for item in bp]
        self.assertListEqual(sorted(tup[:2] for tup in yielded[:2]), [("5", "E"), ("5", "S")])
        self.assertListEqual([tup[:2] for tup in yielded[2:]], [("6", "E"), ("7", None)])
        # Plone 4 behaviour: the same dict object is yielded for each type and keeps the last one
        self.assertIs(yielded[0][2], yielded[1][2])
        self.assertIn(items[0]["_etype"], ("E", "S"))
        self.assertDictEqual(items[1], {"_bpk": "e_mailtype", "_eid": "6", "_etitle": "Facture", "_etype": "E"})
        self.assertDictEqual(items[2], {"_bpk": "e_service", "_eid": "7"})
        self.assertDictEqual(dict(bp.storage["course"]), {"b__1_mailtype_by_type": 2})
        # the matching file is already completed: items are passed unchanged
        items = [{"_bpk": "e_mailtype", "_eid": "5"}]
        storage = self.make_storage("b", e_used_mailtype=used, e_mailtype_match={"5": {}})
        bp = self.section(BMailtypesByType, "b__1_mailtype_by_type", self.options, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_mailtype", "_eid": "5"}])


class TestBMailtypeUpdate(DmsTestCase):

    options = {"condition": "python: item['_bpk'] == u'e_mailtype_match'"}

    def test___init__(self):
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, storage=self.make_storage("a"))
        self.assertDictEqual(bp.to_add, {})
        self.assertFalse(hasattr(bp, "condition"))
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, storage=self.make_storage("b"))
        self.assertTrue(bp.condition({"_bpk": "e_mailtype_match"}))

    def test___iter__(self):
        te_before = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["te"])]
        ts_before = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["ts"])]

        def match(eid, c_type, key, title, active=True):
            return {
                "_bpk": "e_mailtype_match",
                "_eid": eid,
                "_etype": "E",
                "_etitle": title,
                "_enature": "",
                "_esource": "",
                "_c_type": c_type,
                "_key": key,
                "_title": title,
                "_active": active,
            }

        items = [
            match("1", "te", "courrier", "Courrier"),  # already existing
            match("2", "te", "zebre", "Zèbre", False),
            match("3", "te", "avis", "Avis"),
            match("4", "te", "avis", "Avis bis"),  # same key: first one kept
            match("5", "ts", "note", "Note"),
            match("6", "te", "", "Not matched"),
            {"_bpk": "e_mailtype", "_eid": "7"},
        ]
        storage = self.make_storage("b")
        bp = self.section(BMailtypeUpdate, "b__mailtype_update", self.options, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_mailtype", "_eid": "7"}])
        self.assertTrue(items[5]["_error"])
        self.assertIn(("dti", "WARNING", "e_mailtype_match: _eid 6, Empty match: we pass _eid 6"), self.logs.records)
        # new values are added in the registry, sorted by title
        te_after = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["te"])]
        self.assertListEqual(
            te_after,
            te_before
            + [
                {"value": "avis", "dtitle": "Avis", "active": True},
                {"value": "zebre", "dtitle": "Zèbre", "active": False},
            ],
        )
        ts_after = [dict(dic) for dic in api.portal.get_registry_record(MAILTYPES["ts"])]
        self.assertListEqual(ts_after, ts_before + [{"value": "note", "dtitle": "Note", "active": True}])
        # storage is updated
        self.assertDictEqual(storage["data"]["p_mailtype"], get_mailtypes(self.portal))
        self.assertIn("avis", storage["data"]["p_mailtype"]["te"])
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
        "7": {"lastname": "Courant", "firstname": "Jean", "_email1": "jean@electrabel.be"},
        "8": {
            "lastname": "Dupont",
            "firstname": "Jean",
            "_street": "Rue Haute",
            "_pc": "7340",
            "_city": "Colfontaine",
            "_phone1": "065/123456",
            "_email1": "jean@dupont.be",
        },
        "9": {"lastname": "Électricien", "_parent_id": "10", "_function": "Gérant"},
        "10": {"lastname": "Électricité SA", "_street": "Rue Basse", "_cell1": "0475/123456"},
    }

    def test___init__(self):
        storage = self.make_storage("l", e_mail_o={}, e_contact_path={})
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", self.options, storage=storage)
        self.assertEqual(bp.parts, "q")
        self.assertFalse(hasattr(bp, "bp_key"))
        self.assertNotIn("e_mail_recipient", storage["data"])
        storage = self.make_storage("q", e_mail_o={"1": {}}, e_contact_path={"7": {}})
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", self.options, storage=storage)
        self.assertEqual(bp.bp_key, "e_mail_recipient")
        self.assertEqual(bp.contact_label, "DESTINATAIRE")
        self.assertEqual(bp.related_label, "")
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
        self.assertEqual(bp.contact_label, "EXPÉDITEUR")
        self.assertIsInstance(bp.contact_label, str)
        self.assertEqual(bp.related_label, " PARENT")
        self.assertFalse(bp.skip_contact_user)
        self.assertDictEqual(bp.mail_paths, {})

    def test___iter__(self):
        om = self.mail("reponse1")
        om_path = self.path(om)
        storage = self.make_storage(
            "q",
            e_mail_o={"100": {"path": om_path}, "101": {"path": "/outgoing-mail/unknown"}},
            e_contact_path={"7": {"path": "/contacts/jeancourant", "_is_user": False}},
            e_contact=self.e_contact,
        )

        def recip(eid, mail_id, contact_id=None, text=None, **kw):
            dic = {
                "_bpk": "e_mail_recip_o",
                "_eid": eid,
                "_mail_id": mail_id,
                "_contact_id": contact_id,
                "_contact_text": text,
            }
            dic.update(kw)
            return dic

        items = [
            recip("1", "100", "7"),  # contact already created as object: passed
            recip("2", "100"),  # no contact: passed
            recip("3", "100", "8", "Remis en main propre", _action="pour suite", _message="urgent", _response=None),
            recip("4", "101", "8"),  # mail not found
            recip("5", "100", None, "Service X\nRue Y 2"),
            {"_bpk": "e_mail_o", "_eid": "6"},
        ]
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", self.options, items, storage)
        result = list(bp)
        self.assertListEqual(
            result,
            [
                {
                    "_eid": "3",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "e_mail_recipient",
                    "_act": "U",
                    "description": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "DESTINATAIRE LIBRE: Remis en main propre",
                    "data_transfer": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.\r\n"
                    "COMPLÉMENT DESTINATAIRE: pour suite, message=urgent",
                    "modification_date": om.creation_date,
                },
                {
                    "_eid": "5",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "e_mail_recipient",
                    "_act": "U",
                    "description": "DESTINATAIRE LIBRE: Service X",
                    "data_transfer": "DESTINATAIRE LIBRE: Service X, Rue Y 2",
                    "modification_date": om.creation_date,
                },
                {"_bpk": "e_mail_o", "_eid": "6"},
            ],
        )
        self.assertIsInstance(result[0]["description"], str)
        self.assertIsInstance(result[0]["data_transfer"], str)
        # the mail is not modified: next sections will update it
        self.assertEqual(om.description, "")
        self.assertDictEqual(storage["data"]["e_mail_recipient"], {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0})
        self.assertIn(("dto", "WARNING", "mail 101: path '/outgoing-mail/unknown' not found"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"q__1_recipients_as_text": 5})
        # yield_original, contact user skipped, description only (no data_transfer field)
        options = dict(self.options, yield_original="1", skip_real_contact="0")
        storage = self.make_storage(
            "q",
            e_mail_o={"100": {"path": om_path}},
            e_contact_path={
                "7": {"path": "/contacts/jeancourant", "_is_user": True},
                "8": {"path": "/contacts/jeancourant", "_is_user": False},
            },
            e_contact=self.e_contact,
            desc_only=True,
        )
        storage["desc_only"] = True
        items = [recip("1", "100", "7"), recip("2", "100"), recip("3", "100", "8")]
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                recip("2", "100"),
                recip("3", "100", "8"),
                {
                    "_eid": "3",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "e_mail_recipient",
                    "_act": "U",
                    "description": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.",
                    # Plone 4 behaviour: data_transfer is also set with desc_only (the field doesn't exist)
                    "data_transfer": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.",
                    "modification_date": om.creation_date,
                },
            ],
        )
        # data-transfer.cfg.2 m__2_sender_as_text: contact with parent, im existing description kept
        im = self.mail("courrier1")
        im.data_transfer = "AGENT: Agent Fred."
        storage = self.make_storage("m", e_mail_i={"200": {"path": self.path(im)}}, e_contact=self.e_contact)
        items = [
            {"_bpk": "e_mail_recip_i", "_eid": "1", "_mail_id": "200", "_sender_id": "9"},
            {"_bpk": "e_mail_recip_i", "_eid": "2", "_mail_id": "200", "_sender_id": "0"},  # empty contact
        ]
        storage["data"]["e_contact"]["0"] = {"lastname": ""}
        self.addCleanup(storage["data"]["e_contact"].pop, "0")
        bp = self.section(ContactAsTextUpdate, "m__2_sender_as_text", self.options2, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_eid": "1",
                    "_path": self.path(im),
                    "_type": "dmsincomingmail",
                    "_bpk": "e_mail_i_sender",
                    "_act": "U",
                    "description": "Ceci est la description du courrier 1\r\nEXPÉDITEUR: Électricité SA.\r\n"
                    "EXPÉDITEUR: Électricien, Gérant.",
                    "data_transfer": "AGENT: Agent Fred.\r\nEXPÉDITEUR: Électricité SA.\r\n"
                    "EXPÉDITEUR: Électricien, Gérant.\r\nADRESSE EXPÉDITEUR: Rue Basse.\r\n"
                    "TÉL EXPÉDITEUR: 0475/123456.",
                    "modification_date": im.creation_date,
                },
            ],
        )
        # original item mode: the item itself is completed and always yielded
        options = dict(self.options, original_item="1")
        storage = self.make_storage(
            "q",
            e_contact_path={"7": {"path": "/contacts/jeancourant", "_is_user": False}},
            e_contact=self.e_contact,
        )
        items = [
            recip("1", "100", "7"),  # skipped real contact
            recip("2", "100"),  # no contact
            recip("3", "100", "8", description="Desc", data_transfer="DT"),
            recip("4", "100", text='""'),  # only quotes: no contact info
        ]
        bp = self.section(ContactAsTextUpdate, "q__1_recipients_as_text", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                recip("1", "100", "7"),
                recip("2", "100"),
                recip(
                    "3",
                    "100",
                    "8",
                    description="Desc\r\nDESTINATAIRE: Dupont Jean, jean@dupont.be.",
                    data_transfer="DT\r\nDESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.",
                ),
                recip("4", "100", text='""'),
            ],
        )
        self.assertNotIn("_eid", storage["data"]["e_mail_recipient"])  # no batch store with original item
        # part not run
        bp = self.section(
            ContactAsTextUpdate, "q__1_recipients_as_text", self.options, items[:1], self.make_storage("p")
        )
        self.assertListEqual(list(bp), [recip("1", "100", "7")])


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
        self.assertEqual(bp.bp_key, "e_mail_sender")  # set before the part check
        self.assertFalse(hasattr(bp, "fieldname"))
        bp = self.section(ContactSet, "l__contact_set", self.options, storage=self.make_storage("l"))
        self.assertEqual(bp.fieldname, "sender")
        self.assertEqual(bp.mail_id_key, "_mail_id")
        self.assertEqual(bp.contact_id_key, "_sender_id")
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
            "7": {"path": "/contacts/jeancourant", "_is_user": False},
            "8": {"path": "/contacts/electrabel", "_is_user": True},
            "9": {"path": "/contacts/unknown", "_is_user": False},
        }
        # original item (l__contact_set)
        storage = self.make_storage("l", e_contact_path=e_c)
        items = [
            {"_bpk": "e_mail_i", "_eid": "1", "_sender_id": "7"},
            {"_bpk": "e_mail_i", "_eid": "2", "_sender_id": "6"},  # not created contact: default contact
            {"_bpk": "e_mail_i", "_eid": "3", "_sender_id": "9"},  # contact path not found: default contact
            {"_bpk": "e_mail_o", "_eid": "4", "_sender_id": "7"},
        ]
        bp = self.section(ContactSet, "l__contact_set", self.options, items, storage)
        result = list(bp)
        self.assertListEqual([item["_eid"] for item in result], ["1", "2", "3", "4"])
        self.assertListEqual([rel.to_id for rel in result[0]["sender"]], [jc_iid])
        self.assertListEqual([rel.to_id for rel in result[1]["sender"]], [def_iid])
        self.assertListEqual([rel.to_id for rel in result[2]["sender"]], [def_iid])
        self.assertDictEqual(result[3], {"_bpk": "e_mail_o", "_eid": "4", "_sender_id": "7"})
        self.assertDictEqual(storage["data"]["e_mail_sender"], {})
        self.assertDictEqual(dict(storage["course"]), {"l__contact_set": 3})
        # existing value: contact added and default contact removed. Not a list field.
        items = [
            {"_bpk": "e_mail_i", "_eid": "1", "_sender_id": "7", "sender": [RelationValue(def_iid)]},
            {"_bpk": "e_mail_i", "_eid": "2", "_sender_id": "7", "sender": [RelationValue(jc_iid)]},
        ]
        bp = self.section(ContactSet, "l__contact_set", self.options, items, self.make_storage("l", e_contact_path=e_c))
        result = list(bp)
        self.assertListEqual([rel.to_id for rel in result[0]["sender"]], [jc_iid])
        self.assertListEqual([rel.to_id for rel in result[1]["sender"]], [jc_iid])
        options = dict(self.options, is_list="0", skip_contact_user="1")
        items = [
            {"_bpk": "e_mail_i", "_eid": "1", "_sender_id": "7"},
            {"_bpk": "e_mail_i", "_eid": "2", "_sender_id": "8"},  # user contact skipped: default contact
        ]
        bp = self.section(ContactSet, "l__contact_set", options, items, self.make_storage("l", e_contact_path=e_c))
        result = list(bp)
        self.assertIsInstance(result[0]["sender"], RelationValue)
        self.assertEqual(result[0]["sender"].to_id, jc_iid)
        self.assertEqual(result[1]["sender"].to_id, def_iid)
        # no default contact: item yielded unchanged when the contact is not found
        storage = self.make_storage("l", e_contact_path=e_c)
        storage["plone"]["def_contact"] = None
        items = [{"_bpk": "e_mail_i", "_eid": "1", "_sender_id": "6"}]
        bp = self.section(ContactSet, "l__contact_set", self.options, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_mail_i", "_eid": "1", "_sender_id": "6"}])
        # mail modified (q__1_recipients): reponse1 has a recipient
        om = self.mail("reponse1")
        om_path = self.path(om)
        orig_recipients = [rel.to_id for rel in om.recipients]
        storage = self.make_storage(
            "q", e_contact_path=e_c, e_mail_o={"100": {"path": om_path}, "101": {"path": "/outgoing-mail/no"}}
        )
        items = [
            {"_bpk": "e_mail_recip_o", "_eid": "1", "_mail_id": "100", "_contact_id": "7"},
            {"_bpk": "e_mail_recip_o", "_eid": "2", "_mail_id": "101", "_contact_id": "7"},
            {"_bpk": "e_mail_recip_o", "_eid": "3", "_mail_id": "100", "_contact_id": "8"},
        ]
        bp = self.section(ContactSet, "q__1_recipients", self.options2, items, storage)
        # relations read when yielded
        result = [(item, [rel.to_id for rel in item.get("recipients", [])]) for item in bp]
        self.assertListEqual(
            [{k: v for k, v in item.items() if k != "recipients"} for item, rels in result],
            [
                {
                    "_eid": "1",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "e_mail_recipient",
                    "_act": "U",
                    "modification_date": om.creation_date,
                },
                {"_bpk": "e_mail_recip_o", "_eid": "1", "_mail_id": "100", "_contact_id": "7"},
                # mail not found: original item not yielded
                {
                    "_eid": "3",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "e_mail_recipient",
                    "_act": "U",
                    "modification_date": om.creation_date,
                },
                {"_bpk": "e_mail_recip_o", "_eid": "3", "_mail_id": "100", "_contact_id": "8"},
            ],
        )
        self.assertListEqual(result[0][1], orig_recipients + [jc_iid])
        # user contact skipped: default contact used but removed because there is another contact
        self.assertListEqual(result[2][1], orig_recipients + [jc_iid])
        # Plone 4 behaviour (bug?): the mail field list is modified in place (the yielded list for eid 1 is the
        # mail list) and the default contact appended to it is not removed (only the yielded copy is filtered)
        self.assertIs(result[0][0]["recipients"], om.recipients)
        self.assertListEqual([rel.to_id for rel in om.recipients], orig_recipients + [jc_iid, def_iid])
        self.assertDictEqual(storage["data"]["e_mail_recipient"], {"1": 0, "2": 0, "3": 0})
        self.assertIn(("dto", "WARNING", "mail 101: path '/outgoing-mail/no' not found"), self.logs.records)
        self.assertListEqual(orig_recipients, [el_iid])  # fixture: reponse1 recipient is electrabel
        # no contact found and no default contact: original item yielded following yield option
        storage = self.make_storage("q", e_contact_path=e_c, e_mail_o={"100": {"path": om_path}})
        storage["plone"]["def_contact"] = None
        items = [{"_bpk": "e_mail_recip_o", "_eid": "1", "_mail_id": "100", "_contact_id": "6"}]
        bp = self.section(ContactSet, "q__1_recipients", self.options2, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_mail_recip_o", "_eid": "1", "_mail_id": "100", "_contact_id": "6"}])
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
        self.assertEqual(bp.fieldname, "recipients")
        self.assertTrue(bp.is_list)
        self.assertEqual(bp.def_ctct_iid, self.iid("/contacts/notencoded"))
        bp = self.section(
            DefaultContactSet, "p__default_contact_set", dict(self.options, is_list="0"), storage=self.make_storage("p")
        )
        self.assertFalse(bp.is_list)

    def test___iter__(self):
        def_iid = self.iid("/contacts/notencoded")
        items = [{"_bpk": "e_mail_o", "_eid": "1"}, {"_bpk": "e_mail_i", "_eid": "2"}]
        storage = self.make_storage("p")
        bp = self.section(DefaultContactSet, "p__default_contact_set", self.options, items, storage)
        result = list(bp)
        self.assertListEqual([rel.to_id for rel in result[0]["recipients"]], [def_iid])
        self.assertDictEqual(result[1], {"_bpk": "e_mail_i", "_eid": "2"})
        self.assertDictEqual(dict(storage["course"]), {"p__default_contact_set": 1})
        items = [{"_bpk": "e_mail_o", "_eid": "1"}]
        options = dict(self.options, is_list="0")
        bp = self.section(DefaultContactSet, "p__default_contact_set", options, items, self.make_storage("p"))
        self.assertEqual(next(iter(bp))["recipients"].to_id, def_iid)
        # part not run
        items = [{"_bpk": "e_mail_o", "_eid": "1"}]
        bp = self.section(DefaultContactSet, "p__default_contact_set", self.options, items, self.make_storage("l"))
        self.assertListEqual(list(bp), [{"_bpk": "e_mail_o", "_eid": "1"}])


class DTestCase(DmsTestCase):
    """Data of part d (personnel)."""

    def d_storage(self):
        e_user_match = {
            "u1": {"_nom": "Agent", "_prenom": "Fred", "_p_userid": "agent", "_fullname": "Fred Agent"},
            "u2": {"_nom": "Scanner", "_prenom": "Jean", "_p_userid": "scanner", "_fullname": "Jean Scanner"},
            "u3": {"_nom": "Dupuis", "_prenom": "Éric", "_p_userid": "", "_fullname": ""},
        }
        e_contacts_sender = {"20": {"_user_id": "u1"}, "21": {"_user_id": "u2"}, "22": {"_user_id": "u3"}}
        dg_uid = self.uid(PLONEGROUP + "/direction-generale")
        return self.make_storage(
            "d", e_user_match=e_user_match, e_contacts_sender=e_contacts_sender, p_eid_to_orgs={"S1": {"uid": dg_uid}}
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

    def new_person_dic(self, path, eid, euid, title, bpk="person_sender", **kwargs):
        cdate = datetime(2009, 9, 22, 12, 0)
        dic = {
            "_type": "person",
            "_path": path,
            "internal_number": euid,
            "use_parent_address": False,
            "_bpk": bpk,
            "_eid": eid,
            "_deactivate": True,
            "creation_date": cdate,
            "_act": "N",
            "modification_date": cdate,
            "title": title,
            "_post_actions": {"store_internal_person_info": ""},
        }
        dic.update(kwargs)
        return dic

    def new_hp_dic(self, path, eid):
        cdate = datetime(2009, 9, 22, 12, 0)
        return {
            "_type": "held_position",
            "_path": path,
            "use_parent_address": True,
            "internal_number": "",
            "_bpk": "hp_sender",
            "_eid": eid,
            "_deactivate": True,
            "creation_date": cdate,
            "_act": "N",
            "modification_date": cdate,
        }


class TestHandlersFunctions(DTestCase):
    """Module functions of handlers.py needing an ia.docs site."""

    def test_person_dic(self):
        storage = self.d_storage()
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", {}, storage=storage)
        lecteur = self.obj("/contacts/personnel-folder/lecteur")
        lecteur.internal_number = "x1"
        lecteur.firstname = None
        lecteur.userid = None
        # existing person: fields completed if empty
        res = person_dic(
            bp,
            "personnel",
            "u9",
            {"_eid": "1"},
            "lecteur",
            "lecteur",
            "Existing",
            firstname="Jef",
            lastname="Lecteur",
        )
        self.assertListEqual(
            res,
            [
                {
                    "_type": "person",
                    "_path": "/contacts/personnel-folder/lecteur",
                    "internal_number": "x1,u9",
                    "_bpk": "personnel",
                    "_eid": "1",
                    "title": "Existing",
                    "_act": "U",
                    "firstname": "Jef",
                    "userid": "lecteur",
                }
            ],
        )
        self.assertIsInstance(res[0]["internal_number"], str)
        self.assertTrue(bp.change)
        self.assertEqual(storage["data"]["p_euid_to_pers"]["u9"], lecteur.UID())
        # already handled external user
        self.assertListEqual(person_dic(bp, "personnel", "u9", {"_eid": "2"}, None, "lecteur", "Existing"), [])
        # external user id already stored on the person
        lecteur.internal_number = "u8"
        res = person_dic(bp, "personnel", "u8", {"_eid": "3"}, None, "lecteur", "Existing")
        self.assertEqual(res[0]["internal_number"], "u8")
        self.assertNotIn("userid", res[0])
        # new person, without userid nor firstname and without deactivation
        res = person_dic(
            bp, "personnel", "u7", {"_eid": "4"}, None, "dupont", "Unmatched", lastname="Dupont", transitions=()
        )
        self.assertListEqual(
            res,
            [
                self.new_person_dic(
                    "/contacts/personnel-folder/dupont",
                    "4",
                    "u7",
                    "Unmatched",
                    bpk="personnel",
                    _deactivate=False,
                    lastname="Dupont",
                )
            ],
        )
        self.assertNotIn("u7", storage["data"]["p_euid_to_pers"])  # stored by post_actions after creation

    def test_hp_dic(self):
        storage = self.d_storage()
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", {}, storage=storage)
        agent = self.obj("/contacts/personnel-folder/agent")
        dg = self.obj(PLONEGROUP + "/direction-generale")
        secr = self.obj(PLONEGROUP + "/direction-generale/secretariat")
        storage["data"]["p_euid_to_pers"]["u1"] = agent.UID()
        storage["data"]["p_eid_to_orgs"]["S2"] = {"uid": secr.UID()}
        # existing held position
        self.assertListEqual(hp_dic(bp, "hp_sender", "u1", {"_eid": "1", "_service": "S2"}), [])
        self.assertFalse(bp.change)
        # new held position
        res = hp_dic(bp, "hp_sender", "u1", {"_eid": "2", "_service": "S1"}, transitions=())
        self.assertEqual(res[0].pop("position").to_id, self.intids.getId(dg))
        path = f"/contacts/personnel-folder/agent/{dg.UID()}"
        self.assertListEqual(res, [dict(self.new_hp_dic(path, "2"), _deactivate=False)])
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
            {"_bpk": "e_mail_sender", "_eid": "100", "_sender_id": "20", "_service": "S1"},  # existing person
            {"_bpk": "e_mail_sender", "_eid": "101", "_sender_id": "20", "_service": "S1"},  # already done
            {"_bpk": "e_mail_sender", "_eid": "102", "_sender_id": "21", "_service": "S1"},  # matched user
            {"_bpk": "e_mail_sender", "_eid": "103", "_sender_id": "22", "_service": "S1"},  # not matched
            {"_bpk": "e_mail_sender", "_eid": "104", "_sender_id": None, "_service": "S1"},  # no sender
            {"_bpk": "e_mail_o", "_eid": "105"},
        ]
        bp = self.section(DOMSenderCreation, "d__om_sender_creation", self.options, items, storage)
        gen = iter(bp)
        # plone user with a person
        self.assertDictEqual(
            next(gen),
            {
                "_type": "person",
                "_path": "/contacts/personnel-folder/agent",
                "internal_number": "u1",
                "_bpk": "person_sender",
                "_eid": "100",
                "title": "Existing",
                "_act": "U",
            },
        )
        self.assertEqual(storage["data"]["p_euid_to_pers"]["u1"], agent.UID())
        hpdic = next(gen)
        self.assertEqual(hpdic.pop("position").to_id, dg_iid)
        self.assertDictEqual(hpdic, self.new_hp_dic("/contacts/personnel-folder/agent/" + dg.UID(), "100"))
        # plone user without person
        pdic = next(gen)
        path = "/contacts/personnel-folder/scanner"
        self.assertDictEqual(
            pdic,
            self.new_person_dic(path, "102", "u2", "Matched", userid="scanner", firstname="Jean", lastname="Scanner"),
        )
        self.new_person(pdic, storage)
        hpdic = next(gen)
        self.assertEqual(hpdic.pop("position").to_id, dg_iid)
        self.assertDictEqual(hpdic, self.new_hp_dic(path + "/" + dg.UID(), "102"))
        # external user not matched with a plone user
        pdic = next(gen)
        path = "/contacts/personnel-folder/eric-dupuis"
        self.assertDictEqual(
            pdic, self.new_person_dic(path, "103", "u3", "Unmatched", firstname="Éric", lastname="Dupuis")
        )
        self.new_person(pdic, storage)
        self.assertEqual(next(gen)["_path"], path + "/" + dg.UID())
        # no sender: generic person
        pdic = next(gen)
        path = "/contacts/personnel-folder/reprise-donnees"
        self.assertDictEqual(
            pdic, self.new_person_dic(path, "104", "None", "None", firstname="", lastname="Reprise données")
        )
        self.new_person(pdic, storage)
        self.assertEqual(next(gen)["_path"], path + "/" + dg.UID())
        self.assertListEqual(list(gen), [{"_bpk": "e_mail_o", "_eid": "105"}])
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
            {"_bpk": "e_contacts_sender", "_eid": "20", "_user_id": "u1"},  # existing person
            {"_bpk": "e_contacts_sender", "_eid": "21", "_user_id": "u2"},  # matched user
            {"_bpk": "e_contacts_sender", "_eid": "22", "_user_id": "u3"},  # not matched: nothing
            {"_bpk": "e_contacts_sender", "_eid": "23", "_user_id": "u1"},  # already done
            {"_bpk": "e_mail_sender", "_eid": "24"},
        ]
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_type": "person",
                    "_path": "/contacts/personnel-folder/agent",
                    "internal_number": "u1",
                    "_bpk": "personnel",
                    "_eid": "20",
                    "title": "Existing",
                    "_act": "U",
                },
                self.new_person_dic(
                    "/contacts/personnel-folder/scanner",
                    "21",
                    "u2",
                    "Matched",
                    bpk="personnel",
                    userid="scanner",
                    firstname="Jean",
                    lastname="Scanner",
                ),
                {"_bpk": "e_mail_sender", "_eid": "24"},
            ],
        )
        self.assertEqual(storage["data"]["p_euid_to_pers"]["u1"], agent.UID())
        self.assertIn(
            ("dto", "INFO", "Part d: some internal persons or/and held positions have been added."), self.logs.records
        )
        self.assertDictEqual(dict(storage["course"]), {"d__personnel_creation": 4})
        # last name first
        storage = self.d_storage()
        storage["plone"]["firstname_first"] = False
        bp = self.section(DPersonnelCreation, "d__personnel_creation", self.options, items[1:2], storage)
        res = list(bp)
        self.assertEqual((res[0]["firstname"], res[0]["lastname"]), ("Scanner", "Jean"))
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
        open(os.path.join(self.tmp, "1_category_match.csv"), "w").close()
        sections = {"e__category_match_read": {"filename": "1_category_match.csv"}}
        bp = self.section(ECategoryUpdate, "e__category_update", self.options, storage=storage, sections=sections)
        self.assertTrue(bp.b_cond)
        # creation only when there is no category in Plone
        bp = self.section(ECategoryUpdate, "e__category_create", self.options2, storage=storage)
        self.assertFalse(bp.b_cond)
        storage = self.make_storage("e", e_category_parent={"A1": {"_parent_id": "-1"}}, e_category_code={"x": 1})
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
        cats["-1.753"]["obj"].title = "-1.753"  # title not defined
        options = dict(self.options, b_condition="python: True")

        def match(eid, ecode, etitle, pcode, ptitle, **kw):
            dic = {
                "_bpk": "e_category_match",
                "_eid": eid,
                "_ecode": ecode,
                "_etitle": etitle,
                "_eactive": True,
                "_pcode": pcode,
                "_puid": "",
                "_ptitle": ptitle,
                "_pactive": False,
            }
            dic.update(kw)
            return dic

        items = [
            match("1", "1.753", "Armes / munitions", "-1.753", "-1.753"),
            match("2", "1.7", "Police", "-1.7", "Tâches de police."),
            match("3", "9", "Unknown", "-9", ""),
            {"_bpk": "e_category", "_eid": "4"},
        ]
        bp = self.section(ECategoryUpdate, "e__category_update", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                match(
                    "1",
                    "1.753",
                    "Armes / munitions",
                    "-1.753",
                    "Armes - munitions",
                    _pactive=True,
                    _act="U",
                    _type="ClassificationCategory",
                    title="Armes - munitions",
                ),
                match(
                    "2",
                    "1.7",
                    "Police",
                    "-1.7",
                    "Tâches de police.",
                    _type="ClassificationCategory",
                    title="Tâches de police.",
                ),
                {"_bpk": "e_category", "_eid": "4"},
            ],
        )
        node = get_categories(self.portal)["-1.753"]
        self.assertEqual((node["title"], node["enabled"]), ("Armes - munitions", True))
        self.assertEqual((cats["-1.753"]["title"], cats["-1.753"]["enabled"]), ("Armes - munitions", True))
        self.assertTrue(items[2]["_error"])
        self.assertIn(
            ("dti", "ERROR", "e_category_match: _eid 3, The _pcode '-9' is not in the loaded categories. We pass it"),
            self.logs.records,
        )
        self.assertDictEqual(dict(storage["course"]), {"e__category_update": 3})
        # b_condition False: items passed unchanged
        bp = self.section(ECategoryUpdate, "e__category_update", self.options, items[1:2], self.make_storage("e"))
        self.assertListEqual(list(bp), [items[1]])

        # create categories (decimal codes), parents are created if necessary
        def cat(eid, ecode, etitle, eactive=True, **kw):
            dic = {"_bpk": "e_category", "_eid": eid, "_ecode": ecode, "_etitle": etitle, "_eactive": eactive}
            dic.update(kw)
            return dic

        # e_category is stored by _ecode and _eid (e__category_store)
        del self.logs.records[:]
        storage = self.make_storage("e", e_category={"-1.759": {"51": {"_etitle": "Foires", "_eactive": True}}})
        cats = storage["data"]["p_category"]
        options = dict(self.options2, b_condition="python: True")
        items = [
            cat("50", "-1.759.1", "Marchés / foires"),
            cat("51", "-1.759", "Foires"),  # already created as parent
            cat("52", "-1.753", "Armes", False),  # existing code
        ]
        bp = self.section(ECategoryUpdate, "e__category_create", options, items, storage)
        result = list(bp)
        tree = get_categories(self.portal)
        self.assertListEqual(
            result,
            [
                cat(
                    "50",
                    "-1.759.1",
                    "Marchés / foires",
                    _pcode="-1.759.1",
                    _ptitle="Marchés - foires",
                    _puid=tree["-1.759.1"]["uid"],
                    _pactive=True,
                    _act="N",
                    _type="ClassificationCategory",
                    title="Marchés - foires",
                ),
                cat("51", "-1.759", "Foires", _error=True, _type="ClassificationCategory", title=""),
                cat(
                    "52",
                    "-1.753",
                    "Armes",
                    False,
                    _pcode="-1.753-a",
                    _ptitle="Armes",
                    _puid=tree["-1.753-a"]["uid"],
                    _pactive=False,
                    _act="N",
                    _error=True,
                    _type="ClassificationCategory",
                    title="Armes",
                ),
            ],
        )
        # Plone 4 behaviour (bug?): the parent title is not found in e_category (stored by code and eid): code used
        self.assertEqual((tree["-1.759"]["title"], tree["-1.759"]["enabled"]), ("-1.759", True))
        self.assertEqual(cats["-1.759"]["case"], "parent")
        self.assertEqual(aq_parent(tree["-1.759"]["obj"]).identifier, "-1.75")
        self.assertEqual(aq_parent(tree["-1.759.1"]["obj"]).identifier, "-1.759")
        self.assertEqual(aq_parent(tree["-1.753-a"]["obj"]).identifier, "-1.75")
        self.assertEqual((tree["-1.759.1"]["title"], tree["-1.759.1"]["enabled"]), ("Marchés - foires", True))
        self.assertIsInstance(tree["-1.759.1"]["title"], str)
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                (
                    "dti",
                    "ERROR",
                    "e_category: _eid 51, Same code '-1.759' with title 'Foires' previously created "
                    "with title '-1.759'",
                ),
                (
                    "dti",
                    "ERROR",
                    "e_category: _eid 52, Same code '-1.753' with title 'Armes' previously created "
                    "with title 'Armes - munitions'",
                ),  # title updated above
            ],
        )
        # prefix: the prefix part is not a parent, title kept with slash
        storage = self.make_storage("e", e_category={})
        options = dict(self.options2, b_condition="python: True", prefix="-1", title_replace_slash="0")
        bp = self.section(ECategoryUpdate, "e__category_create", options, [cat("53", "-1.8", "A / B")], storage)
        self.assertEqual(next(iter(bp))["title"], "A / B")
        node = get_categories(self.portal)["-1.8"]["obj"]
        self.assertEqual(aq_parent(node).getPhysicalPath(), self.portal.tree.getPhysicalPath())
        # not decimal codes: parent relation
        e_category_parent = {"A1": {"_parent_id": "-1"}, "A2": {"_parent_id": "ZZ"}}
        storage = self.make_storage("e", e_category={}, e_category_parent=e_category_parent)
        options = dict(
            self.options2, b_condition="python: True", decimal_import="0", parent_relation="e_category_parent"
        )
        items = [cat("54", "A1", "A un"), cat("55", "A2", "A deux"), cat("56", "A3", "A trois")]
        bp = self.section(ECategoryUpdate, "e__category_create", options, items, storage)
        self.assertListEqual([item["_pcode"] for item in bp], ["A1", "A2", "A3"])
        tree = get_categories(self.portal)
        self.assertEqual(aq_parent(tree["A1"]["obj"]).identifier, "-1")
        self.assertEqual(aq_parent(tree["A2"]["obj"]).getPhysicalPath(), self.portal.tree.getPhysicalPath())
        self.assertEqual(aq_parent(tree["A3"]["obj"]).getPhysicalPath(), self.portal.tree.getPhysicalPath())
        self.assertTrue(items[1]["_error"])
        self.assertIn(
            ("dti", "ERROR", "e_category: _eid 55, The parent 'ZZ' is not in the created categories."),
            self.logs.records,
        )
        # category code eid: e_category and parent relation stored by eid
        storage = self.make_storage(
            "e",
            e_category={"61": {"_ecode": "-1.7"}},
            e_category_code={"-1.7": {}},
            e_category_parent={"60": {"_parent_id": "61"}},
        )
        options = dict(
            self.options2,
            b_condition="python: True",
            decimal_import="0",
            parent_relation="e_category_parent",
            category_code_eid="e_category_code",
        )
        bp = self.section(ECategoryUpdate, "e__category_create", options, [cat("60", "B1", "B un")], storage)
        list(bp)
        self.assertEqual(aq_parent(get_categories(self.portal)["B1"]["obj"]).identifier, "-1.7")
        # part not run
        bp = self.section(
            ECategoryUpdate, "e__category_create", options, [cat("62", "B2", "B")], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [cat("62", "B2", "B")])


class TestF1Level3Handler(DmsTestCase):

    # data-transfer.cfg.1 f__1_level3_handler
    options = {"condition": "python: item['_level'] == 3"}

    def test___init__(self):
        bp = self.section(F1Level3Handler, "f__1_level3_handler", self.options, storage=self.make_storage("a"))
        self.assertEqual(bp.parts, "f")
        self.assertTrue(bp.condition({"_level": 3}))  # always parsed
        self.assertFalse(bp.condition({"_level": 2}))

    def test___iter__(self):
        storage = self.make_storage(
            "f",
            e_f_parent_relation={"3": {"_parent_id": "2"}, "2": {"_parent_id": "1"}},
            e_folder={"3": {"title": "Sous-dossier"}},
        )
        items = [
            {"_bpk": "e_folder", "_eid": "4", "_parent_id": "3", "title": "Pièces", "_level": 3},
            {"_bpk": "e_folder", "_eid": "3", "_parent_id": "2", "title": "Sous-dossier", "_level": 2},
        ]
        bp = self.section(F1Level3Handler, "f__1_level3_handler", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_folder",
                    "_eid": "4",
                    "_parent_id": "2",
                    "title": "Sous-dossier | Pièces",
                    "_level": 3,
                    "_type": "ClassificationSubfolder",
                    "_error": True,
                },
                {"_bpk": "e_folder", "_eid": "3", "_parent_id": "2", "title": "Sous-dossier", "_level": 2},
            ],
        )
        self.assertIn(("dti", "ERROR", "e_folder: _eid 4, Modifying level3 folder"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"f__1_level3_handler": 1})
        # part not run
        item = {"_bpk": "e_folder", "_eid": "4", "_parent_id": "3", "title": "Pièces", "_level": 3}
        bp = self.section(F1Level3Handler, "f__1_level3_handler", self.options, [item], self.make_storage("a"))
        self.assertListEqual(
            list(bp), [{"_bpk": "e_folder", "_eid": "4", "_parent_id": "3", "title": "Pièces", "_level": 3}]
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
        self.assertEqual(bp.bp_key, "e_folder_subfolder")
        self.assertEqual(bp.prefix, "_s_")
        self.assertIsInstance(bp.prefix, str)

    def test___iter__(self):
        storage = self.make_storage("f")
        items = [
            {
                "_bpk": "e_folder",
                "_eid": "1",
                "title": "Dossier",
                "_s__eid": "1-1",
                "_s_title": "Farde",
                "_s__parent_id": "1",
            },
            {"_bpk": "e_folder", "_eid": "2", "title": "Dossier 2", "_s__eid": None},
        ]
        bp = self.section(F2FolderSubfolderSplit, "f__2_folder_subfolder", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_folder",
                    "_eid": "1",
                    "title": "Dossier",
                    "_s__eid": "1-1",
                    "_s_title": "Farde",
                    "_s__parent_id": "1",
                },
                # prefixed keys are kept, bp_key is not used
                {
                    "_bpk": "e_folder",
                    "_eid": "1-1",
                    "title": "Farde",
                    "_parent_id": "1",
                    "_s__eid": "1-1",
                    "_s_title": "Farde",
                    "_s__parent_id": "1",
                },
                {"_bpk": "e_folder", "_eid": "2", "title": "Dossier 2", "_s__eid": None},
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
        self.assertEqual(bp.fld_name, "classification_informations")
        self.assertEqual(bp.sep, "/")
        self.assertEqual(bp.pattern, r"^-[.\d]+$")
        options = {"bp_key": "p_category", "txt_field_name": "data_transfer"}
        bp = self.section(F3TextCategoriesToId, "l__3_category_id_insert", options, storage=storage)
        self.assertEqual(bp.cat_fld_name, "_classification_categories_text")
        self.assertEqual(bp.sep, "")
        self.assertEqual(bp.pattern, "")

    def test___iter__(self):
        storage = self.make_storage("f")
        cats = storage["data"]["p_category"]
        storage["plone"]["def_category"] = cats["-1"]["uid"]

        def folder(eid, cat_id, **kw):
            dic = {"_bpk": "e_folder", "_eid": eid, "_category_id": cat_id}
            dic.update(kw)
            return dic

        items = [
            folder("1", "-1.7/ -1.75"),
            folder("2", "-1.7/-9.9", classification_informations="Info"),
            folder("3", "-1.7/abc"),  # not matching pattern: whole text
            folder("4", "-1.753"),
            folder("5", None),
        ]
        bp = self.section(F3TextCategoriesToId, "f__3_category_id_insert", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                folder("1", "-1.7/ -1.75", classification_categories=[cats["-1.7"]["uid"], cats["-1.75"]["uid"]]),
                folder(
                    "2",
                    "-1.7/-9.9",
                    classification_categories=[cats["-1"]["uid"]],
                    _error=True,
                    classification_informations="Info\r\nCLASSEMENT: -1.7/-9.9",
                ),
                folder(
                    "3",
                    "-1.7/abc",
                    classification_categories=[cats["-1"]["uid"]],
                    _error=True,
                    classification_informations="CLASSEMENT: -1.7/abc",
                ),
                folder("4", "-1.753", classification_categories=[cats["-1.753"]["uid"]]),
                folder("5", None),
            ],
        )
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", "e_folder: _eid 2, Category part '-9.9' not found ('-1.7/-9.9')"),
                ("dti", "ERROR", "e_folder: _eid 3, Category part '-1.7/abc' not found ('-1.7/abc')"),
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"f__3_category_id_insert": 4})
        # without separator
        options = dict(self.options)
        del options["separator"]
        bp = self.section(
            F3TextCategoriesToId, "f__3_category_id_insert", options, [folder("1", "-1.7/-1.75")], storage
        )
        self.assertListEqual(next(iter(bp))["classification_categories"], [cats["-1"]["uid"]])
        # part not run
        bp = self.section(
            F3TextCategoriesToId,
            "f__3_category_id_insert",
            self.options,
            [folder("1", "-1.7")],
            self.make_storage("a"),
        )
        self.assertListEqual(list(bp), [folder("1", "-1.7")])


class TestHContactTypeUpdate(DmsTestCase):

    # data-transfer.cfg.1 h__contact_type_update
    options = {"condition": "python: item['_bpk'] == u'e_contact_type_match'", "clean_unused": "1"}

    def h_storage(self):
        storage = self.make_storage("h")
        directory = storage["plone"]["directory"]
        for typ in ("types", "levels"):  # as the Initialization section
            key = f"p_dir_org_{typ}"
            storage["data"][key] = OrderedDict(
                [
                    (safe_text(t["token"]), {"name": safe_text(t["name"])})
                    for t in getattr(directory, "organization_%s" % typ)
                ]
            )
            storage["data"][f"{key}_len"] = len(storage["data"][key])
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
            ["non-defini", "sa", "commune", "cpas", "intercommunale", "zp", "zs"],
        )

        def match(eid, pid, ptitle):
            return {
                "_bpk": "e_contact_type_match",
                "_eid": eid,
                "_etitle": ptitle,
                "_pid": pid,
                "_ptitle": ptitle,
            }

        items = [
            match("1", "commune", "Commune"),
            match("2", "asbl", "ASBL"),
            match("3", "", ""),
            match("4", "sa", "Société anonyme"),
            {"_bpk": "e_contact", "_eid": "5"},
        ]
        storage = self.h_storage()
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", self.options, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_contact", "_eid": "5"}])
        self.assertTrue(items[2]["_error"])
        self.assertIn(("dti", "ERROR", "e_contact_type_match: _eid 3, Empty match: we pass _eid 3"), self.logs.records)
        # unused types are removed, except non-defini
        self.assertListEqual(
            [dict(dic) for dic in directory.organization_types],
            [
                {"token": "non-defini", "name": "Non défini"},
                {"token": "sa", "name": "Société anonyme"},
                {"token": "commune", "name": "Commune"},
                {"token": "asbl", "name": "ASBL"},
            ],
        )
        self.assertListEqual(list(storage["data"]["p_dir_org_types"]), ["non-defini", "sa", "commune", "asbl"])
        self.assertDictEqual(dict(storage["course"]), {"h__contact_type_update": 4})
        # without cleaning
        directory.organization_types = types_before
        bp = self.section(HContactTypeUpdate, "h__contact_type_update", {}, items[1:2], self.h_storage())
        list(bp)
        self.assertListEqual(
            [dict(dic) for dic in directory.organization_types], types_before + [{"token": "asbl", "name": "ASBL"}]
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
                "_pc": "",
                "_city": "",
                "_number": "",
                "_box": "",
                "_street": "",
                "_imp_addr": "",
                "_imp_pc": "",
                "_imp_city": "",
                "_imp_country": "",
                "_phone": "",
                "_cell": "",
                "_fax": "",
                "_website": "",
                "_email": "",
            }
            dic.update(kw)
            return dic

        def contact(eid, **kw):
            dic = {
                "_bpk": "e_contact",
                "_eid": eid,
                "lastname": "Dupont",
                "firstname": "Jean",
                "_ptitle": None,
                "_name2": None,
            }
            dic.update(kw)
            return dic

        e_address = {
            "A1": address(
                _pc="7340",
                _city="Colfontaine",
                _number="12",
                _box="b",
                _street="Rue Haute",
                _imp_addr="Rue Haute 12b",
                _imp_country="France",
                _phone="065/1",
                _cell="0475/1",
                _fax="065/2",
                _website="www.dupont.be",
                _email="j@dupont.be",
            ),
            "A2": address(_pc="7000", _city="Mons", _box="3", _imp_addr="Grand-Place\n 1 ", _imp_country="Belgique"),
            "A3": address(_imp_addr="Rue X 1", _imp_pc="1000", _imp_city="Bruxelles"),
        }
        items = [
            contact("1", _addr_id="A1"),
            contact("2", _addr_id="A2"),
            contact("3", _addr_id="A3", _phone2="02/1", _cell3="0476/1", _email1="x@y.be", _web="www.y.be"),
            contact(
                "4",
                _street="Rue Basse\n2",
                _street_nb="5",
                _street_nb_box="a",
                _pc="7340",
                _city="Colfontaine",
                _country="belgique",
                _fax="065/3",
            ),
            contact("5", _street_nb_box="a", _country="France"),
            contact("6", _parent_id="1", lastname="", _ptitle="Directeur"),
            contact("7", lastname="", _name2="Service travaux"),
            {"_bpk": "e_service", "_eid": "8"},
        ]
        storage = self.make_storage("i", e_address=e_address)
        options = {"condition": "python: item['_bpk'] == u'e_contact'"}
        bp = self.section(I1ContactUpdate, "i__1_contact_update", options, items, storage)
        empty = {"phone": None, "cell_phone": None, "email": None, "use_parent_address": False}
        self.assertListEqual(
            list(bp),
            [
                contact(
                    "1",
                    _addr_id="A1",
                    zip_code="7340",
                    city="Colfontaine",
                    number="12 bte b",
                    street="Rue Haute",
                    country="France",
                    phone="065/1",
                    cell_phone="0475/1",
                    fax="065/2",
                    website="www.dupont.be",
                    email="j@dupont.be",
                    use_parent_address=False,
                ),
                contact("2", _addr_id="A2", zip_code="7000", city="Mons", number="3", street="Grand-Place, 1", **empty),
                contact(
                    "3",
                    _addr_id="A3",
                    _phone2="02/1",
                    _cell3="0476/1",
                    _email1="x@y.be",
                    _web="www.y.be",
                    street="Rue X 1",
                    zip_code="1000",
                    city="Bruxelles",
                    phone="02/1",
                    cell_phone="0476/1",
                    email="x@y.be",
                    website="www.y.be",
                    use_parent_address=False,
                ),
                contact(
                    "4",
                    _street="Rue Basse\n2",
                    _street_nb="5",
                    _street_nb_box="a",
                    _pc="7340",
                    _city="Colfontaine",
                    _country="belgique",
                    _fax="065/3",
                    street="Rue Basse, 2",
                    number="5 a",
                    zip_code="7340",
                    city="Colfontaine",
                    fax="065/3",
                    **empty,
                ),
                # Plone 4 behaviour (bug?): box without number gives a leading space
                contact("5", _street_nb_box="a", _country="France", number=" a", country="France", **empty),
                # no address: parent address, empty lastname replaced by title
                dict(
                    contact("6", _parent_id="1", lastname="Directeur", _ptitle=None),
                    **dict(empty, use_parent_address=True),
                ),
                contact("7", lastname="Service travaux", _name2=None, **empty),
                {"_bpk": "e_service", "_eid": "8"},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"i__1_contact_update": 7})
        # part not run
        bp = self.section(I1ContactUpdate, "i__1_contact_update", options, [contact("1")], self.make_storage("a"))
        self.assertListEqual(list(bp), [contact("1")])


class TestI2ContactUpdate(DmsTestCase):

    # data-transfer.cfg.2 i__2_contact_update
    options = {"paths_key": "e_contact_path", "plonegroup_org_id": "-3"}

    def i_storage(self, **data):
        secr_uid = self.uid(PLONEGROUP + "/direction-generale/secretariat")
        dic = {
            "e_contact": {},
            "e_contact_path": {},
            "e_contact_i": {},
            "e_user_match": {"Agent_Fred": {"_p_userid": "agent"}, "Lecteur_Jef": {"_p_userid": "unknown"}},
            "e_service_match": {"S5": {"uid": secr_uid}},
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
            "S5": {"_organization": "Secrétariat", "_type": "organization"},
            "12": {"_organization": "Commune", "_type": "organization"},
            "40": {"lastname": "Durand", "_pc": "7000", "_city": "Mons", "_street": "Rue Neuve"},
        }
        storage = self.i_storage(e_contact=e_contact)
        cdate = storage["creation_date"]

        def org(eid, title, division="", **kw):
            dic = {
                "_bpk": "e_contact",
                "_eid": eid,
                "_organization": title,
                "_division": division,
                "_service": "",
                "_pc": "7340",
                "_city": "Colfontaine",
                "_street": "Rue Haute",
                "_email1": "info@commune.be",
                "_phone1": "065/1",
                "_phone2": "065/2",
            }
            dic.update(kw)
            return dic

        def pers(eid, parent_id, lastname, firstname, **kw):
            dic = {
                "_bpk": "e_contact",
                "_eid": eid,
                "_parent_id": parent_id,
                "lastname": lastname,
                "firstname": firstname,
                "_cell1": "0475/1",
                "_phone1": "065/3",
                "_phone2": None,
                "_email1": "p@x.be",
            }
            dic.update(kw)
            return dic

        new = {"use_parent_address": False, "creation_date": cdate, "modification_date": cdate}
        items = [
            org("S5", "Secrétariat"),  # internal organization
            org("12", "Commune"),
            org("13", "Commune", "Travaux", _service="Voiries"),
            org("14", ""),  # just an address: passed
            pers("30", "12", "Dupont", "Jean"),  # held position in an external organization
            pers("31", "S5", "Agent", "Fred"),  # existing internal held position
            pers("32", "S5", "Lecteur", "Jef"),  # new internal person
            pers("33", "40", "Durand", "Marie"),  # person with a person parent
            pers("34", None, "Martin", "Paul"),
            {"_bpk": "e_service", "_eid": "S6"},
        ]
        options = dict(self.options, condition="python: item['_bpk'] == u'e_contact'")
        bp = self.section(I2ContactUpdate, "i__2_contact_update", options, items, storage)
        gen = iter(bp)
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk="e_contact",
                _eid="12",
                _division="",
                _service="",
                _id="12",
                _type="organization",
                _parenth="/contacts",
                internal_number="12",
                zip_code="7340",
                city="Colfontaine",
                street="Rue Haute",
                title="Commune",
                email="info@commune.be",
                phone="065/1",
                fax="065/2",
            ),
        )
        self.assertEqual(
            storage["data"]["e_contact_path"]["S5"], {"path": PLONEGROUP + "/direction-generale/secretariat"}
        )
        self.assertIn(("dti", "INFO", "Passed internal organization: S5"), self.logs.records)
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk="e_contact",
                _eid="13",
                _organization="Commune",
                _division="Travaux",
                _service="Voiries",
                _id="13",
                _type="organization",
                _parenth="/contacts",
                internal_number="13",
                zip_code="7340",
                city="Colfontaine",
                street="Rue Haute",
                title="Commune - Travaux - Voiries",
                email="info@commune.be",
                phone="065/1",
                fax="065/2",
            ),
        )
        # the organization "12" has been created by the next sections (an existing organization is used here)
        storage["data"]["e_contact_path"]["12"] = {"path": "/contacts/electrabel"}
        pdic = next(gen)
        self.assertDictEqual(
            pdic,
            {
                "_bpk": "e_contact",
                "_eid": "30a",
                "_type": "person",
                "lastname": "Dupont",
                "firstname": "Jean",
                "_parenth": "/contacts",
            },
        )
        pdic["_path"] = "/contacts/30a"  # set by the next sections (path_insert)
        hp = next(gen)
        self.assertEqual(hp.pop("position").to_id, self.iid("/contacts/electrabel"))
        self.assertDictEqual(
            hp,
            dict(
                new,
                _bpk="e_contact",
                _eid="30",
                _parent_id="12",
                lastname="Dupont",
                firstname="Jean",
                _type="held_position",
                _parenth="/contacts/30a",
                use_parent_address=True,
                _is_user=False,
                cell_phone="0475/1",
                phone="065/3",
                fax=None,
                email="p@x.be",
                _id="30",
                internal_number="30",
            ),
        )
        # existing internal person and held position: nothing yielded
        pdic = next(gen)
        self.assertDictEqual(
            pdic,
            {
                "_bpk": "e_contact",
                "_eid": "32a",
                "_type": "person",
                "lastname": "Lecteur",
                "firstname": "Jef",
                "_parenth": "/contacts/personnel-folder",
            },
        )
        self.assertDictEqual(
            storage["data"]["e_contact_path"]["31"],
            {"path": "/contacts/personnel-folder/agent/agent-secretariat", "_is_user": True},
        )
        self.assertIn(("dti", "INFO", "Passed internal person: 31"), self.logs.records)
        self.assertIn(("dti", "INFO", "Passed internal hp: 31"), self.logs.records)
        pdic["_path"] = "/contacts/personnel-folder/32a"
        hp = next(gen)
        self.assertEqual(hp.pop("position").to_id, self.iid(PLONEGROUP + "/direction-generale/secretariat"))
        self.assertEqual(
            (hp["_type"], hp["_parenth"], hp["_is_user"]), ("held_position", "/contacts/personnel-folder/32a", True)
        )
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk="e_contact",
                _eid="33",
                _parent_id="40",
                lastname="Durand",
                firstname="Marie",
                _type="person",
                _parenth="/contacts",
                _is_user=False,
                zip_code="7000",
                city="Mons",
                street="Rue Neuve",
                cell_phone="0475/1",
                phone="065/3",
                fax=None,
                email="p@x.be",
                _id="33",
                internal_number="33",
            ),
        )
        self.assertDictEqual(
            next(gen),
            dict(
                new,
                _bpk="e_contact",
                _eid="34",
                _parent_id=None,
                lastname="Martin",
                firstname="Paul",
                _type="person",
                _parenth="/contacts",
                _is_user=False,
                cell_phone="0475/1",
                phone="065/3",
                fax=None,
                email="p@x.be",
                _id="34",
                internal_number="34",
            ),
        )
        self.assertListEqual(list(gen), [{"_bpk": "e_service", "_eid": "S6"}])
        self.assertListEqual(
            sorted(storage["data"]["e_contact_i"]),
            sorted(["S5", "12", "13", "14", "30", "31", "32", "33", "34"]),
        )
        self.assertDictEqual(dict(storage["course"]), {"i__2_contact_update": 9})
        # part not run
        bp = self.section(
            I2ContactUpdate, "i__2_contact_update", options, [org("S5", "Secrétariat")], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [org("S5", "Secrétariat")])


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
                "_bpk": "e_contact_c",
                "_eid": eid,
                "lastname": "Dupont",
                "firstname": "Jean",
                "_function": "Directeur",
                "person_title": "Monsieur",
                "phone": "065/1",
                "_phone2": None,
                "fax": "",
                "email": "jean@dupont.be",
                "cell_phone": None,
                "_p_enabled": "1",
                "_o_eid": o_eid,
                "_o_title": o_title,
                "street": "Rue Haute",
                "number": "5",
                "_box": "b",
                "zip_code": "7340",
                "city": "Colfontaine",
                "_o_phone": "065/0",
                "_o_fax": None,
                "_o_email": "info@commune.be",
                "enterprise_number": "0123",
                "_o_enabled": "1",
                "country": "Belgique",
            }
            dic.update(kw)
            return dic

        eids = {
            "12": {"path": "/contacts/electrabel"},
            "34": {"path": "/contacts/jeancourant/agent-electrabel"},
            "36": {"path": "/contacts/sergerobinet"},
        }
        storage = self.make_storage("j", e_contact_path=eids, e_contact_j={})
        items = [
            contact("34", "12", "Commune"),
            contact("35", "99", "Unknown"),  # organization not found
            contact("33", "12", "Commune", country="France"),  # held position not found
            contact("36"),  # person
            contact("37"),  # person not found
            {"_bpk": "e_contact", "_eid": "38"},
        ]
        options = dict(self.options, condition="python: item['_bpk'] == u'e_contact_c'")
        bp = self.section(J2ContactUpdate, "j__2_contact_update", options, items, storage)
        base = {
            "_bpk": "e_contact_c",
            "street": "Rue Haute",
            "number": "5 b",
            "city": "Colfontaine",
            "zip_code": "7340",
            "country": "",
        }
        org = dict(
            base,
            _eid="12",
            _type="organization",
            title="Commune",
            enterprise_number="0123",
            phone="065/0",
            email="info@commune.be",
            _path="/contacts/electrabel",
        )
        self.assertListEqual(
            list(bp),
            [
                org,
                # Plone 4 behaviour (bug?): updated held position and person have no _path
                dict(
                    base,
                    _eid="34",
                    _type="held_position",
                    label="Directeur",
                    phone="065/1",
                    fax="",
                    email="jean@dupont.be",
                ),
                dict(org, country="France"),
                dict(base, _eid="36", _type="person", phone="065/1", fax="", email="jean@dupont.be"),
                {"_bpk": "e_contact", "_eid": "38"},
            ],
        )
        # Plone 4 behaviour (bug?): the path is stored as a tuple on the original item
        self.assertEqual(items[0]["_path"], ("/contacts/jeancourant/agent-electrabel",))
        self.assertEqual(items[3]["_path"], ("/contacts/sergerobinet",))
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", "e_contact_c: _eid 35, Organization not found: 99"),
                ("dti", "ERROR", "e_contact_c: _eid 33, HP not found: 33"),
                ("dti", "ERROR", "e_contact_c: _eid 37, Person not found: 37"),
            ],
        )
        self.assertListEqual(sorted(storage["data"]["e_contact_j"]), ["33", "34", "35", "36", "37"])
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
            container=self.obj(PLONEGROUP), type="organization", id="reprise-de-donnees", title="Reprise de données"
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
        self.assertEqual(bp.grs_title, "Reprise de données")
        self.assertIsInstance(bp.grs_title, str)
        self.assertEqual(bp.grs_uid, grs.UID())  # no part check
        self.assertListEqual(bp.gr_tg_exc, [])
        options = dict(self.options, global_recipient_tg_exceptions="333 Personnel 336 DirecteurFinancier")
        bp = self.section(L1RecipientGroupsSet, "p__1_recipient_groups_set", options, storage=self.make_storage("p"))
        self.assertListEqual(bp.gr_tg_exc, ["333", "336"])
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
            {"_bpk": "e_mail_i", "_eid": "1", "_service_id": "10"},
            {"_bpk": "e_mail_i", "_eid": "2", "_service_id": "11", "recipient_groups": [secr_uid]},
            {"_bpk": "e_mail_i", "_eid": "3", "_service_id": "333"},
            {"_bpk": "e_mail_i", "_eid": "4", "_service_id": "12", "recipient_groups": [grs_uid]},
            {"_bpk": "e_mail_o", "_eid": "5", "_service_id": "10"},
        ]
        storage = self.make_storage("l")
        bp = self.section(L1RecipientGroupsSet, "l__1_recipient_groups_set", options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {"_bpk": "e_mail_i", "_eid": "1", "_service_id": "10", "recipient_groups": [grs_uid]},
                {"_bpk": "e_mail_i", "_eid": "2", "_service_id": "11", "recipient_groups": [secr_uid, grs_uid]},
                {"_bpk": "e_mail_i", "_eid": "3", "_service_id": "333"},
                {"_bpk": "e_mail_i", "_eid": "4", "_service_id": "12", "recipient_groups": [grs_uid]},
                {"_bpk": "e_mail_o", "_eid": "5", "_service_id": "10"},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"l__1_recipient_groups_set": 4})


class TestL1SenderAsTextSet(DmsTestCase):

    # data-transfer.cfg.1 l__1_sender_as_text_set
    options = {"condition": "python: item.get('_bpk') == u'e_mail_i'"}

    def test___init__(self):
        storage = self.make_storage("a", e_contact_path={"7": {}})
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, storage=storage)
        self.assertEqual(bp.eid_key, "_sender_id")
        self.assertIs(bp.eids, storage["data"]["e_contact_path"])  # no part check
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, storage=self.make_storage("l"))
        self.assertIsNone(bp.eids)

    def test___iter__(self):
        e_contact = TestContactAsTextUpdate.e_contact
        storage = self.make_storage("l", e_contact_path={"7": {"path": "/contacts/jeancourant"}}, e_contact=e_contact)
        items = [
            {"_bpk": "e_mail_i", "_eid": "1", "_sender_id": "8", "_sender": "Lettre de M. Dupont"},
            {"_bpk": "e_mail_i", "_eid": "2", "_sender_id": "7", "_sender": None, "description": "Desc"},
            {
                "_bpk": "e_mail_i",
                "_eid": "3",
                "_sender_id": None,
                "_sender": "Anonyme\nRue X",
                "data_transfer": "DT",
            },
            {"_bpk": "e_mail_o", "_eid": "4", "_sender_id": "8", "_sender": None},
        ]
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_mail_i",
                    "_eid": "1",
                    "_sender_id": "8",
                    "_sender": "Lettre de M. Dupont",
                    "description": "EXPÉDITEUR: Dupont Jean, jean@dupont.be.\r\nEXPÉDITEUR LIBRE: Lettre de M. Dupont",
                    "data_transfer": "EXPÉDITEUR: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE EXPÉDITEUR: Rue Haute 7340 Colfontaine.\r\nTÉL EXPÉDITEUR: 065/123456.",
                },
                # contact created as object: no text
                {"_bpk": "e_mail_i", "_eid": "2", "_sender_id": "7", "_sender": None, "description": "Desc"},
                {
                    "_bpk": "e_mail_i",
                    "_eid": "3",
                    "_sender_id": None,
                    "_sender": "Anonyme\nRue X",
                    "description": "EXPÉDITEUR LIBRE: Anonyme",
                    "data_transfer": "DT\r\nEXPÉDITEUR LIBRE: Anonyme, Rue X",
                },
                {"_bpk": "e_mail_o", "_eid": "4", "_sender_id": "8", "_sender": None},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"l__1_sender_as_text_set": 3})
        # no data_transfer field: everything in description
        storage["desc_only"] = True
        items = [
            {
                "_bpk": "e_mail_i",
                "_eid": "3",
                "_sender_id": None,
                "_sender": "Anonyme\nRue X",
                "data_transfer": "DT",
            }
        ]
        bp = self.section(L1SenderAsTextSet, "l__1_sender_as_text_set", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_mail_i",
                    "_eid": "3",
                    "_sender_id": None,
                    "_sender": "Anonyme\nRue X",
                    "data_transfer": "DT",
                    "description": "EXPÉDITEUR LIBRE: Anonyme, Rue X",
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
            "u1": {"_nom": "Agent", "_prenom": "Fred", "_p_userid": "agent", "_user_id": "u1"},
            "u4": {"_nom": "Inconnu", "_prenom": "", "_p_userid": "", "_user_id": "u4"},
            "u5": {"_nom": "Chef", "_prenom": "Michel", "_p_userid": "chef", "_user_id": "u5"},
        }
        dic = {
            "e_mail_i": {},
            "e_mail_o": {},
            "e_contacts_sender": {"20": {"_user_id": "u1"}, "21": {"_user_id": "u4"}},
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
        im2.assigned_user = "encodeur"
        im4.data_transfer = "DESTINATAIRE: Agent Fred"
        paths = {
            "200": {"path": self.path(im2)},
            "300": {"path": self.path(im1)},
            "201": {"path": self.path(im3)},
            "202": {"path": "/incoming-mail/unknown"},
            "203": {"path": self.path(im4)},
        }
        storage = self.m_storage(e_mail_i=paths)

        def recip(eid, mail_id, contact_id, principal=False, **kw):
            dic = {
                "_bpk": "e_mail_recip_i",
                "_eid": eid,
                "_mail_id": mail_id,
                "_contact_id": contact_id,
                "_principal": principal,
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
            recip("1", "200", "20", True, _action="Pour suite", _message="Urgent", _response=None),
            recip("2", "300", "20"),  # user not editor in the treating group
            recip("3", "200", "99"),  # contact not a user
            recip("4", "202", "20"),  # mail not found
            recip("5", "201", "21"),  # user not matched
            recip("6", "200", "20"),  # mail already assigned
            recip("7", "203", "20"),  # nothing to do
            {"_bpk": "e_mail_i", "_eid": "8"},
        ]
        bp = self.section(M1AssignedUserHandling, "m__1_assigned_user", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                upd(
                    "1",
                    im2,
                    data_transfer="DESTINATAIRE PRINCIPAL: Agent Fred, Pour suite, message=Urgent",
                    assigned_user="agent",
                ),
                upd("2", im1, data_transfer="DESTINATAIRE: Agent Fred"),
                upd("5", im3, data_transfer="DESTINATAIRE: Inconnu"),
                upd("6", im2, data_transfer="DESTINATAIRE: Agent Fred"),
                {"_bpk": "e_mail_i", "_eid": "8"},
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
            list(bp), [upd("2", im1, description="Ceci est la description du courrier 1\r\nDESTINATAIRE: Agent Fred")]
        )
        # original item (p__2_assigned_user): om sender
        df_uid = self.uid(PLONEGROUP + "/direction-financiere")
        info_uid = self.uid(PLONEGROUP + "/direction-generale/informatique")  # not a selected service
        items = [
            {"_bpk": "e_mail_o", "_eid": "1", "_sender_id": "u5", "treating_groups": df_uid},
            {"_bpk": "e_mail_o", "_eid": "2", "_sender_id": "u5", "treating_groups": info_uid},
        ]
        bp = self.section(M1AssignedUserHandling, "p__2_assigned_user", self.options2, items, self.m_storage("p"))
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_mail_o",
                    "_eid": "1",
                    "_sender_id": "u5",
                    "treating_groups": df_uid,
                    "assigned_user": "chef",
                },
                {"_bpk": "e_mail_o", "_eid": "2", "_sender_id": "u5", "treating_groups": info_uid},
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
            "20": {"_user_id": "u1", "lastname": "Agent", "firstname": "Fred"},
            "21": {"_user_id": None, "lastname": "Dupont", "firstname": "Jean"},
        }
        storage = self.make_storage(
            "p",
            e_contact=e_contact,
            p_eid_to_orgs={"S1": {"uid": secr_uid}},
            p_euid_to_pers={"u1": agent_uid, "None": chef_uid},
        )
        items = [
            {"_bpk": "e_mail_o", "_eid": "1", "_sender_id": "20", "_sender": None, "_service_id": "S1"},
            {
                "_bpk": "e_mail_o",
                "_eid": "2",
                "_sender_id": "21",
                "_sender": "Libre",
                "_service_id": "S1",
                "description": "Desc",
            },
            {"_bpk": "e_mail_o", "_eid": "3", "_sender_id": None, "_sender": None, "_service_id": "S1"},
            {"_bpk": "e_mail_i", "_eid": "4", "_sender_id": "20"},
        ]
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, items, storage)
        agent_hp = self.uid("/contacts/personnel-folder/agent/agent-secretariat")
        chef_hp = self.uid("/contacts/personnel-folder/chef/responsable-secretariat")
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_mail_o",
                    "_eid": "1",
                    "_sender_id": "20",
                    "_sender": None,
                    "_service_id": "S1",
                    "sender": agent_hp,
                },
                # not a user: generic person and contact in text
                {
                    "_bpk": "e_mail_o",
                    "_eid": "2",
                    "_sender_id": "21",
                    "_sender": "Libre",
                    "_service_id": "S1",
                    "sender": chef_hp,
                    "description": "Desc\r\nEXPÉDITEUR: Dupont Jean.\r\nEXPÉDITEUR LIBRE: Libre",
                    "data_transfer": "EXPÉDITEUR: Dupont Jean.",
                },
                {
                    "_bpk": "e_mail_o",
                    "_eid": "3",
                    "_sender_id": None,
                    "_sender": None,
                    "_service_id": "S1",
                    "sender": chef_hp,
                },
                {"_bpk": "e_mail_i", "_eid": "4", "_sender_id": "20"},
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"p__1_sender_set": 3})
        # part not run
        bp = self.section(POMSenderSet, "p__1_sender_set", self.options, items[:1], self.make_storage("a"))
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "e_mail_o",
                    "_eid": "1",
                    "_sender_id": "20",
                    "_sender": None,
                    "_service_id": "S1",
                    "sender": agent_hp,
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
        self.assertEqual(bp.bp_key, "e_folder_p")
        self.assertIsInstance(bp.bp_key, str)

    def test___iter__(self):
        cdate = datetime(2009, 9, 22, 12, 0)
        storage = self.make_storage("lpfi", e_parent={"F1": {"path": "/folders/f1"}, "O1": {"path": "/contacts/o1"}})
        items = [
            {"_type": "dmsincomingmail", "creation_date": cdate},
            {"_type": "dmsincoming_email", "creation_date": cdate},
            {"_type": "dmsoutgoingmail", "creation_date": cdate},
            {"_type": "ClassificationFolder"},
            {"_type": "ClassificationSubfolder", "_parent_id": "F1"},
            {"_bpk": "e_folder", "_eid": "2", "_type": "ClassificationSubfolder", "_parent_id": "F9"},
            {"_type": "person"},
            {"_type": "organization", "_parent_id": "O1"},
            {"_bpk": "e_contact", "_eid": "3", "_type": "organization", "_parent_id": "O9"},
            {"_type": "organization", "_parent_id": None},
            {"_type": "held_position"},
            {"_type": "dmsincomingmail", "_path": "/incoming-mail/x"},
            {"_type": "dmsincomingmail", "_parenth": "/incoming-mail/y"},
            {"_bpk": "e_mail_i"},
        ]
        bp = self.section(ParentPathInsert, "lpfi__parent_insert", {"bp_key": "e_parent"}, items, storage)
        result = list(bp)
        self.assertListEqual(
            result,
            [
                {"_type": "dmsincomingmail", "creation_date": cdate, "_parenth": "/incoming-mail/200938"},
                {"_type": "dmsincoming_email", "creation_date": cdate, "_parenth": "/incoming-mail/200938"},
                {"_type": "dmsoutgoingmail", "creation_date": cdate, "_parenth": "/outgoing-mail/200938"},
                {"_type": "ClassificationFolder", "_parenth": "/folders"},
                {"_type": "ClassificationSubfolder", "_parent_id": "F1", "_parenth": "/folders/f1"},
                {
                    "_bpk": "e_folder",
                    "_eid": "2",
                    "_type": "ClassificationSubfolder",
                    "_parent_id": "F9",
                    "_error": True,
                },
                {"_type": "person", "_parenth": "/contacts"},
                {"_type": "organization", "_parent_id": "O1", "_parenth": "/contacts/o1"},
                {
                    "_bpk": "e_contact",
                    "_eid": "3",
                    "_type": "organization",
                    "_parent_id": "O9",
                    "_parenth": "/contacts",
                    "_error": True,
                },
                {"_type": "organization", "_parent_id": None, "_parenth": "/contacts"},
                {"_type": "held_position"},
                {"_type": "dmsincomingmail", "_path": "/incoming-mail/x"},
                {"_type": "dmsincomingmail", "_parenth": "/incoming-mail/y"},
                {"_bpk": "e_mail_i"},
            ],
        )
        self.assertIsInstance(result[0]["_parenth"], str)
        # period folders are created
        self.assertIn("200938", self.portal["incoming-mail"])
        self.assertIn("200938", self.portal["outgoing-mail"])
        self.assertEqual(api.content.get_state(self.portal["incoming-mail"]["200938"]), "internally_published")
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", "e_folder: _eid 2, Parent id not found 'F9'"),
                ("dti", "ERROR", "e_contact: _eid 3, Parent id not found 'O9'"),
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"lpfi__parent_insert": 11})
        # part not run
        bp = self.section(ParentPathInsert, "l__1_parent_insert", {}, [{"_type": "person"}], self.make_storage("a"))
        self.assertListEqual(list(bp), [{"_type": "person"}])


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
            container=self.obj("/contacts/personnel-folder"), type="person", id="dupont", lastname="Dupont"
        )
        post = {"store_internal_person_info": ""}
        items = [
            {
                "_bpk": "person_sender",
                "_eid": "1",
                "_path": "/contacts/personnel-folder/dupont",
                "internal_number": "u7",
                "_post_actions": post,
            },
            {
                "_bpk": "personnel",
                "_eid": "2",
                "_path": "/contacts/personnel-folder/agent",
                "internal_number": "u1",
                "_post_actions": post,
            },
            {"_bpk": "e_mail_i", "_eid": "3"},
        ]
        bp = self.section(PostActions, "post_actions", {}, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_bpk": "person_sender",
                    "_eid": "1",
                    "_path": "/contacts/personnel-folder/dupont",
                    "internal_number": "u7",
                    "_post_actions": {"store_internal_person_info": ""},
                },
                {
                    "_bpk": "personnel",
                    "_eid": "2",
                    "_path": "/contacts/personnel-folder/agent",
                    "internal_number": "u1",
                    "_post_actions": {"store_internal_person_info": ""},
                },
                {"_bpk": "e_mail_i", "_eid": "3"},
            ],
        )
        self.assertEqual(storage["data"]["p_euid_to_pers"]["u7"], dupont.UID())
        self.assertEqual(storage["data"]["p_euid_to_pers"]["u1"], agent.UID())
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
        storage["data"]["e_mail_i"] = {"100": {"path": "/incoming-mail/x/100"}}
        bp = self.section(PostActions, "post_actions", {}, [{"_bpk": "e_mail_i", "_eid": "100"}], storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_mail_i", "_eid": "100"}])
        self.assertListEqual(commits, [True])
        self.assertIn(("dto", "INFO", "Commit in 'e_mail_i' at 4"), self.logs.records)
        self.assertIn(("dto", "INFO", f"Dumping '{dumped}'"), self.logs.records)
        with open(dumped, "rb") as fh:
            self.assertDictEqual(pickle.load(fh), {"100": {"path": "/incoming-mail/x/100"}})
        self.assertFalse(os.path.exists(os.path.join(self.tmp, "2_no.pkl")))
        # not a multiple of commit_nb
        storage["count"]["commit_count"][""]["c"] = 5
        bp = self.section(PostActions, "post_actions", {}, [{"_bpk": "e_mail_i", "_eid": "100"}], storage)
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
        open(os.path.join(self.tmp, "1_contact_type_match.csv"), "w").close()
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
            e_mail_o={"100": {"path": om_path}, "101": {"path": "/outgoing-mail/no"}},
            e_contact=TestContactAsTextUpdate.e_contact,
        )
        items = [
            {
                "_bpk": "e_mail_recip_o",
                "_eid": "1",
                "_mail_id": "100",
                "_contact_id": "8",
                "_comment": "Copie",
                "_action": "Pour info",
            },
            {"_bpk": "e_mail_recip_o", "_eid": "2", "_mail_id": "100", "_contact_id": None, "_comment": None},
            {"_bpk": "e_mail_recip_o", "_eid": "3", "_mail_id": "101", "_contact_id": "8", "_comment": None},
            {"_bpk": "e_mail_o", "_eid": "4"},
        ]
        bp = self.section(Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {
                    "_eid": "1",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "o_recipients",
                    "_act": "U",
                    "description": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\nDESTINATAIRE LIBRE: Copie",
                    # Plone 4 behaviour (bug?): separator added before the complement
                    "data_transfer": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.\r\n"
                    "COMPLÉMENT DESTINATAIRE: , Pour info",
                },
                {"_bpk": "e_mail_o", "_eid": "4"},
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
                    "_eid": "1",
                    "_path": om_path,
                    "_type": "dmsoutgoingmail",
                    "_bpk": "o_recipients",
                    "_act": "U",
                    "description": "DESTINATAIRE: Dupont Jean, jean@dupont.be.\r\n"
                    "ADRESSE DESTINATAIRE: Rue Haute 7340 Colfontaine.\r\nTÉL DESTINATAIRE: 065/123456.\r\n"
                    "DESTINATAIRE LIBRE: Copie",
                }
            ],
        )
        # part not run
        bp = self.section(
            Q1RecipientsAsTextUpdate, "q__1_recipients_as_text", self.options, items[3:], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [{"_bpk": "e_mail_o", "_eid": "4"}])


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
            "200": {"path": self.path(im2)},
            "201": {"path": self.path(im3)},
            "202": {"path": "/incoming-mail/no"},
            "203": {"path": self.path(om1)},
        }
        e_s_m = {"S1": {"uid": secr_uid}, "S2": {"uid": grh_uid}, "S4": {"uid": comm_uid}}
        storage = self.make_storage("r", e_mail=paths, e_service_match=e_s_m, recipient_groups={})

        def rec(eid, mail_id, service_id):
            return {"_bpk": "e_mail_rec_groups", "_eid": eid, "_mail_id": mail_id, "_service_id": service_id}

        def upd(eid, mail_id, service_id, mail, **kw):
            dic = {
                "_eid": eid,
                "_mail_id": mail_id,
                "_service_id": service_id,
                "_bpk": "recipient_groups",
                "_path": self.path(mail),
                "_type": mail.portal_type,
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            rec("1", "200", "S1"),  # treating group: stored
            rec("2", "200", "S2"),
            rec("3", "202", "S2"),  # mail not found
            rec("4", "203", "S2"),
            rec("5", "201", "S1"),  # already a recipient group
            rec("6", "201", "S4"),
            {"_bpk": "e_mail_i", "_eid": "7"},
        ]
        bp = self.section(R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                upd("2", "200", "S2", im2, recipient_groups=[grh_uid], _suid=grh_uid),
                upd("4", "203", "S2", om1, recipient_groups=[grh_uid], _suid=grh_uid),
                upd("5", "201", "S1", im3, _suid=secr_uid),  # useful when correcting
                upd("6", "201", "S4", im3, recipient_groups=[secr_uid, comm_uid], _suid=comm_uid),
                {"_bpk": "e_mail_i", "_eid": "7"},
            ],
        )
        self.assertDictEqual(storage["data"]["recipient_groups"], {"200": {"S1": {"_eid": "1"}}})
        self.assertListEqual(im3.recipient_groups, [secr_uid])  # not modified
        self.assertIn(("dto", "WARNING", "mail 202: path '/incoming-mail/no' not found"), self.logs.records)
        self.assertDictEqual(dict(storage["course"]), {"r__1_recipient_groups_update": 6})
        # already treated in a previous batch
        storage["data"]["recipient_groups"]["201"] = {"S1": {"_eid": "5"}}
        bp = self.section(R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, items[4:5], storage)
        self.assertListEqual(list(bp), [])
        # part not run
        bp = self.section(
            R1RecipientGroupsUpdate, "r__1_recipient_groups_update", self.options, items[:1], self.make_storage("a")
        )
        self.assertListEqual(list(bp), [rec("1", "200", "S1")])


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
        self.assertTrue(bp.condition({"_bpk": "recipient_groups", "_suid": "x", "_type": "dmsincomingmail"}))

    def test___iter__(self):
        im2 = self.mail("courrier2")
        grh_uid = self.uid(PLONEGROUP + "/direction-generale/grh")
        ILabeling(im2).storage["lu"] = PersistentList(["agent", "chef"])
        items = [
            {
                "_bpk": "recipient_groups",
                "_eid": "1",
                "_path": self.path(im2),
                "_type": "dmsincomingmail",
                "_suid": grh_uid,
            },
            {
                "_bpk": "recipient_groups",
                "_eid": "2",
                "_path": "/incoming-mail/no",
                "_type": "dmsincomingmail",
                "_suid": grh_uid,
            },
            {
                "_bpk": "recipient_groups",
                "_eid": "3",
                "_path": self.path(im2),
                "_type": "dmsoutgoingmail",
                "_suid": grh_uid,
            },
        ]
        storage = self.make_storage("r")
        bp = self.section(ReadLabelForRecipientGroup, "r__read_label_for_recipient_group", self.options, items, storage)
        self.assertListEqual([item["_eid"] for item in bp], ["1", "3"])
        # readers of the recipient group are added as having read the mail
        self.assertListEqual(list(ILabeling(im2).storage["lu"]), ["agent", "chef", "lecteur"])
        self.assertTrue(items[1]["_error"])
        self.assertIn(
            ("dti", "ERROR", "recipient_groups: _eid 2, Cannot find mail with path '/incoming-mail/no'"),
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
        self.assertEqual(bp.filename, os.path.join(self.tmp, "2_rsync_files.txt"))
        self.assertFalse(bp.doit)
        self.assertFalse(hasattr(bp, "bp_key"))
        open(os.path.join(self.tmp, "2_files.pkl"), "wb").close()
        storage = self.make_storage("1", e_files={})
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, storage=storage, sections=self.sections)
        self.assertTrue(bp.doit)
        self.assertEqual(bp.bp_key, "e_files")
        self.assertIs(bp.files, storage["data"]["e_files"])
        self.assertEqual(bp.unused_filename, "")
        self.assertEqual(bp.used, set())
        options = dict(self.options, filename="/tmp/rsync.txt", unused_filename="3_unused.txt")
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", options, storage=storage, sections=self.sections)
        self.assertEqual(bp.filename, "/tmp/rsync.txt")
        self.assertEqual(bp.unused_filename, os.path.join(self.tmp, "3_unused.txt"))

    def test___iter__(self):
        open(os.path.join(self.tmp, "2_files.pkl"), "wb").close()
        e_files = {
            "1000": {"f": [(".pdf", "PDF_0-999")]},
            "1001": {"f": [(".doc", "DOC"), (".pdf", "PDF")]},
            "1002": {"f": [(".pdf", "PDF"), (".doc", "DOC")]},
            "1005": {"f": [(".pdf", "PDF")]},
        }
        storage = self.make_storage("1", e_files=e_files)
        items = [
            {"_bpk": "e_file", "_eid": "1000"},
            {"_bpk": "e_file", "_eid": "1003", "_fs_path": "scans/1005.pdf"},
            {"_bpk": "e_file", "_eid": "1001"},
            {"_bpk": "e_file", "_eid": "1004"},  # not on disk
        ]
        options = dict(self.options, unused_filename="2_unused_files.txt")
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", options, items, storage, sections=self.sections)
        self.assertListEqual(list(bp), [])  # items are consumed
        with open(os.path.join(self.tmp, "2_rsync_files.txt"), encoding="utf8") as fh:
            self.assertEqual(fh.read(), "PDF_0-999/1000.pdf\nscans/1005.pdf\nDOC/1001.doc\nPDF/1001.pdf\n")
        with open(os.path.join(self.tmp, "2_unused_files.txt"), encoding="utf8") as fh:
            self.assertEqual(fh.read(), "DOC/1002.doc\nPDF/1002.pdf\n")
        self.assertSetEqual(bp.used, {"1000", "1003", "1005", "1001", "1004"})
        self.assertIsNone(bp.fh)
        self.assertIn(
            ("dto", "INFO", "Writing '{}'".format(os.path.join(self.tmp, "2_rsync_files.txt"))), self.logs.records
        )
        self.assertDictEqual(dict(storage["course"]), {"1__rsync_file_write": 4})
        # file cannot be created
        options = dict(self.options, filename=os.path.join(self.tmp, "unknown", "rsync.txt"))
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", options, items, storage, sections=self.sections)
        self.assertRaises(Exception, list, bp)
        # b_condition False or part not run: items yielded
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, items[:1], storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_file", "_eid": "1000"}])
        bp = self.section(RsyncFileWrite, "1__rsync_file_write", self.options, items[:1], self.make_storage("a"))
        self.assertListEqual(list(bp), [{"_bpk": "e_file", "_eid": "1000"}])


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
        cat_uid = get_categories(self.portal)["-1.7"]["uid"]
        self.imported(im2, im3, im4, om1)
        om1.classification_folders = [folder.UID()]
        om1.classification_categories = [cat_uid]
        im4.classification_folders = [other.UID()]
        paths = {
            "200": {"path": self.path(im2)},
            "201": {"path": self.path(im3)},
            "202": {"path": self.path(om1)},
            "203": {"path": self.path(im4)},
        }
        storage = self.make_storage(
            "s",
            e_mail=paths,
            e_folder={
                "F0001": {"_category_id": "C1", "_title": "Ordre public"},
                "F0002": {"_category_id": "C9", "_title": "Dossier non importé"},
            },
            e_category_full_match={"C1": {"_puid": cat_uid}},
            p_irn_to_folder={"F0001": {"uid": folder.UID()}},
        )

        def link(mail_id, folder_id):
            return {"_bpk": "e_mail_folder", "_eid": mail_id, "_folder_id": folder_id}

        def upd(mail_id, folder_id, mail, **kw):
            dic = {
                "_eid": mail_id,
                "_folder_id": folder_id,
                "_bpk": "classification_folders",
                "_path": self.path(mail),
                "_type": mail.portal_type,
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            link("200", "F0001"),
            link("201", "F0002"),  # folder not created in Plone
            link("202", "F9"),  # unknown folder
            link("202", "F0001"),  # already set
            link("203", "F0001"),
            {"_bpk": "e_mail", "_eid": "204"},
        ]
        bp = self.section(
            S1ClassificationFoldersUpdate, "s__1_classification_folders_update", self.options, items, storage
        )
        result = list(bp)
        self.assertListEqual(
            result,
            [
                upd("200", "F0001", im2, classification_categories=[cat_uid], classification_folders=[folder.UID()]),
                upd(
                    "201",
                    "F0002",
                    im3,
                    description="Ceci est la description du courrier 3\r\nDOSSIER: Dossier non importé",
                ),
                upd(
                    "203",
                    "F0001",
                    im4,
                    classification_categories=[cat_uid],
                    classification_folders=[other.UID(), folder.UID()],
                ),
                {"_bpk": "e_mail", "_eid": "204"},
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
                    "e_mail_folder: _eid 201, Cannot find folder_id in plone 'F0002' => put in in " "description",
                ),
                ("dti", "ERROR", "e_mail_folder: _eid 202, Cannot find folder_id 'F9' in existing folders"),
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"s__1_classification_folders_update": 5})
        # an example mail of the imio.dms.mail fixture, not created by the pipeline (on Plone 4 it had no
        # classification_categories attribute and raised an AttributeError)
        im5 = self.mail("courrier5")
        storage["data"]["e_mail"]["205"] = {"path": self.path(im5)}
        bp = self.section(
            S1ClassificationFoldersUpdate,
            "s__1_classification_folders_update",
            self.options,
            [link("205", "F0001")],
            storage,
        )
        self.assertListEqual(
            list(bp),
            [
                upd(
                    "205",
                    "F0001",
                    im5,
                    classification_categories=[cat_uid],
                    classification_folders=[folder.UID()],
                )
            ],
        )
        # empty mail category not filled
        options = dict(self.options, fill_empty_mail_category="0")
        bp = self.section(
            S1ClassificationFoldersUpdate, "s__1_classification_folders_update", options, items[:1], storage
        )
        self.assertListEqual(list(bp), [upd("200", "F0001", im2, classification_folders=[folder.UID()])])
        # part not run
        bp = self.section(
            S1ClassificationFoldersUpdate,
            "s__1_classification_folders_update",
            self.options,
            items[:1],
            self.make_storage("a"),
        )
        self.assertListEqual(list(bp), [link("200", "F0001")])


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
        storage["plone"]["def_category"] = "abcd"
        bp = self.section(
            S3ClassificationFoldersUpdate, "s__3_classification_folders_update", self.options, storage=storage
        )
        self.assertEqual(bp.mail_id_key, "_mail_id")
        self.assertTrue(bp.rdc)
        self.assertIs(bp.paths, storage["data"]["e_mail"])
        self.assertEqual(bp.def_cat, "abcd")
        self.assertIs(bp.irn_to_f, storage["data"]["p_irn_to_folder"])
        bp = self.section(
            S3ClassificationFoldersUpdate,
            "s__3_classification_folders_update",
            {"store_key": "e_mail"},
            storage=self.make_storage("s", e_mail={}),
        )
        self.assertEqual(bp.mail_id_key, "_eid")
        self.assertFalse(bp.rdc)
        self.assertIsNone(bp.def_cat)
        self.assertIsNone(bp.irn_to_f)

    def test___iter__(self):
        im2, im3, om1, im4 = (self.mail(mid) for mid in ("courrier2", "courrier3", "reponse1", "courrier4"))
        folder = self.obj("/folders/ordre-public-reglement-general-de-police")
        cats = get_categories(self.portal)
        def_cat = cats["-1"]["uid"]
        self.imported(im2, im3, om1, im4)
        im3.classification_categories = [def_cat]
        paths = {
            "200": {"path": self.path(im2)},
            "201": {"path": self.path(im3)},
            "202": {"path": self.path(om1)},
            "203": {"path": self.path(im4)},
        }
        storage = self.make_storage(
            "s",
            e_mail=paths,
            e_folder={"F0002": {"_title": "Dossier non importé"}},
            p_irn_to_folder={"F0001": {"uid": folder.UID()}},
        )
        storage["plone"]["def_category"] = def_cat

        def upd(eid, folder_id, mail, **kw):
            dic = {
                "_eid": eid,
                "_folder_id": folder_id,
                "_bpk": "classification_folders",
                "_path": self.path(mail),
                "_type": mail.portal_type,
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            {"_bpk": "e_mail_folder", "_eid": "1", "_mail_id": "200", "_folder_id": "F0001"},
            {"_bpk": "e_mail_folder", "_eid": "2", "_mail_id": "201", "_folder_id": "F0001"},
            {"_bpk": "e_mail_folder", "_mail_id": "202", "_folder_id": "F0001"},  # no _eid
            {"_bpk": "e_mail_folder", "_eid": "4", "_mail_id": "203", "_folder_id": "F0002"},
            {"_bpk": "e_mail_folder", "_eid": "5", "_mail_id": "203", "_folder_id": "F9"},
            {"_bpk": "e_mail", "_eid": "6"},
        ]
        bp = self.section(
            S3ClassificationFoldersUpdate, "s__3_classification_folders_update", self.options, items, storage
        )
        folder_cats = list(folder.classification_categories)
        self.assertListEqual(
            list(bp),
            [
                upd("1", "F0001", im2, classification_categories=folder_cats, classification_folders=[folder.UID()]),
                # default category replaced
                upd("2", "F0001", im3, classification_categories=folder_cats, classification_folders=[folder.UID()]),
                # Plone 4 behaviour (bug?): without _eid, the mail_id_key name is used as _eid
                upd(
                    "_mail_id",
                    "F0001",
                    om1,
                    classification_categories=folder_cats,
                    classification_folders=[folder.UID()],
                ),
                upd(
                    "4",
                    "F0002",
                    im4,
                    description="Ceci est la description du courrier 4\r\nDOSSIER: Dossier non " "importé",
                ),
                {"_bpk": "e_mail", "_eid": "6"},
            ],
        )
        self.assertListEqual(
            [rec[2] for rec in self.logs.records if rec[0] == "dti"],
            [
                "e_mail_folder: _eid 4, Cannot find folder_id in plone 'F0002' => put in in description",
                "e_mail_folder: _eid 5, Cannot find folder_id in plone 'F9' => put in in description",
                "e_mail_folder: _eid 5, Cannot find folder_id in external folders 'F9' => we pass it",
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"s__3_classification_folders_update": 5})
        # Plone 4 behaviour (bug?): without _eid, log_error raises a KeyError
        items = [{"_bpk": "e_mail_folder", "_mail_id": "202", "_folder_id": "F9"}]
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
        self.assertEqual(bp.bp_key, "e_dmsfile_i")
        self.assertIs(bp.paths, storage["data"]["e_mail_i"])
        self.assertIs(bp.disk_files, storage["data"]["e_files"])
        self.assertEqual(bp.title_key, "_desc")
        self.assertDictEqual(
            bp.categories,
            {
                "dmsmainfile": calculate_category_id(self.obj("/annexes_types/incoming_dms_files/incoming-dms-file")),
                "dmsappendixfile": calculate_category_id(
                    self.obj("/annexes_types/incoming_appendix_files/incoming-appendix-file")
                ),
            },
        )
        bp = self.section(
            T1DmsfileCreation, "t__1_dmsfile_creation", dict(self.options, title_key="_title"), storage=storage
        )
        self.assertEqual(bp.title_key, "_title")

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
            with open(fullpath, "wb") as fh:
                fh.write(content)
        cdate = datetime(2009, 9, 22, 12, 0)
        e_files = {"1000": {"f": [(".pdf", "PDF")]}, "1001": {"f": [(".doc", "DOC"), (".pdf", "PDF")]}}
        paths = {
            "200": {"path": self.path(im2)},
            "201": {"path": self.path(im3)},
            "202": {"path": self.path(im3)},
        }
        storage = self.make_storage("t", e_mail_i=paths, e_files=e_files, e_dmsfile_unfound={})

        def efile(eid, mail_id, **kw):
            dic = {
                "_bpk": "e_file",
                "_eid": eid,
                "_mail_id": mail_id,
                "_desc": "Fichier scanné",
                "_order": None,
                "_fs_path": None,
                "_filename": None,
                "_ext": None,
                "creation_date": cdate,
            }
            dic.update(kw)
            return dic

        items = [
            efile("1000", "200", _order="1", _filename="scan.tif", _ext="pdf"),
            efile("1001", "200", _order="2"),  # appendix
            efile("1003", "201", _fs_path="scans/x.doc", _fs_idx="1"),  # split from multiple paths: appendix
            efile("1004", "202"),  # no file
            efile("1005", "202", _fs_path="missing/1005.pdf"),
            {"_bpk": "e_mail", "_eid": "1006"},
        ]
        bp = self.section(T1DmsfileCreation, "t__1_dmsfile_creation", self.options, items, storage)
        result = list(bp)
        main_cat, app_cat = bp.categories["dmsmainfile"], bp.categories["dmsappendixfile"]

        def dmsfile(eid, mail, typ, cat, title, data):
            return {
                "_eid": eid,
                "_parenth": self.path(mail),
                "_type": typ,
                "_bpk": "e_dmsfile_i",
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
                dmsfile("1000", im2, "dmsmainfile", main_cat, "scan.pdf", b"%PDF-1000"),
                dmsfile("1001", im2, "dmsappendixfile", app_cat, "1001.pdf", b"%PDF-1001"),  # pdf first
                dmsfile("1003", im3, "dmsappendixfile", app_cat, "x.doc", b"DOC-x"),
                {"_bpk": "e_mail", "_eid": "1006"},
            ],
        )
        self.assertIsInstance(result[0]["file"]["data"], bytes)
        self.assertIsInstance(result[0]["file"]["filename"], str)
        self.assertEqual(items[2]["_fs_path"], "scans/x.doc")
        self.assertEqual(items[1]["_fs_path"], "PDF/1001.pdf")  # stored by get_file_content
        self.assertDictEqual(
            storage["data"]["e_dmsfile_unfound"],
            {"1004": {"err": "no path"}, "1005": {"err": "missing/1005.pdf not found"}},
        )
        self.assertListEqual(
            [rec for rec in self.logs.records if rec[0] == "dti"],
            [
                ("dti", "ERROR", "e_file: _eid 1004, Empty file path for mail 202"),
                ("dti", "ERROR", "e_file: _eid 1005, Cannot open filename 'missing/1005.pdf'"),
            ],
        )
        self.assertDictEqual(
            bp.files,
            {
                "200": {"lo": 2, "ids": ["1000", "1001"]},
                "201": {"lo": None, "ids": ["1003"]},
                "202": {"lo": None, "ids": ["1004", "1005"]},
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
        self.assertListEqual(list(bp), [{"_bpk": "e_mail", "_eid": "1006"}])


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
        self.assertEqual(bp.batch_key, "")
        options = dict(self.options, batch_store="e_mail_link_done", batch_key="_eid")
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", options, storage=storage)
        self.assertIs(bp.batch_store, storage["data"]["e_mail_link_done"])
        self.assertEqual(bp.batch_key, "_eid")

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
            "300": {"path": self.path(om1)},
            "302": {"path": self.path(om3)},
            "200": {"path": self.path(im1)},
            "201": {"path": self.path(im2)},
        }
        storage = self.make_storage("x", e_mail=paths)
        items = [
            {"_bpk": "e_mail_link", "_eid": "300", "_target_id": "200"},
            {"_bpk": "e_mail_link", "_eid": "302", "_target_id": "201"},  # already linked
            {"_bpk": "e_mail", "_eid": "303"},
        ]
        options = dict(self.options, batch_store="e_mail_link_done", batch_key="_eid")
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", options, items, storage)
        result = list(bp)
        self.assertListEqual(
            [{k: v for k, v in item.items() if k != "reply_to"} for item in result],
            [
                {
                    "_eid": "300",
                    "_target_id": "200",
                    "_bpk": "reply_to",
                    "_path": self.path(om1),
                    "_type": "dmsoutgoingmail",
                    "_act": "U",
                },
                {"_bpk": "e_mail", "_eid": "303"},
            ],
        )
        # target, mails linked to the target and mails linking to the target
        self.assertListEqual([rel.to_id for rel in result[0]["reply_to"]], [iid(im1), iid(im3), iid(om2)])
        self.assertDictEqual(storage["data"]["e_mail_link_done"], {"300": True, "302": True})
        self.assertDictEqual(dict(storage["course"]), {"x__1_reply_to_update": 2})
        # part not run
        bp = self.section(X1ReplyToUpdate, "x__1_reply_to_update", self.options, items[:1], self.make_storage("a"))
        self.assertListEqual(list(bp), [{"_bpk": "e_mail_link", "_eid": "300", "_target_id": "200"}])


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
        self.assertFalse(bp.condition({"_type": "dmsincomingmail", "_path": "/x"}))  # part a
        bp = self.section(
            WorkflowHistoryUpdate, "workflow_history_update", self.options, storage=self.make_storage("l")
        )
        self.assertTrue(bp.condition({"_type": "dmsincomingmail", "_path": "/x"}))

    def test___iter__(self):
        im2 = self.mail("courrier2")
        api.content.transition(obj=im2, transition="propose_to_manager")
        wkf = list(im2.workflow_history)[0]
        before = [dict(status) for status in im2.workflow_history[wkf]]
        self.assertGreater(len(before), 1)
        self.assertNotEqual({status["actor"] for status in before}, {"csvimport"})
        items = [
            {"_bpk": "e_mail_i", "_eid": "200", "_type": "dmsincomingmail", "_path": self.path(im2)},
            {"_bpk": "e_mail_i", "_eid": "201", "_type": "dmsincomingmail"},
        ]
        storage = self.make_storage("l")
        bp = self.section(WorkflowHistoryUpdate, "workflow_history_update", self.options, items, storage)
        self.assertListEqual(
            list(bp),
            [
                {"_bpk": "e_mail_i", "_eid": "200", "_type": "dmsincomingmail", "_path": self.path(im2)},
                {"_bpk": "e_mail_i", "_eid": "201", "_type": "dmsincomingmail"},
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
        '<?xml version="1.0" encoding="utf-8"?><Contacts>'
        "<Sender><Organization><OrgId>12</OrgId><Name>Commune de Colfontaine</Name><Level1>Travaux</Level1>"
        "<Address>Rue Haute 1</Address><ZipCode>7340</ZipCode><City>Colfontaine</City></Organization>"
        "<Contact><PersId>34</PersId><FirstName>Jean</FirstName><LastName>Dupont</LastName><Email> </Email>"
        "</Contact></Sender>"
        "<Addressee><Organization><OrgId>13</OrgId><Name></Name></Organization>"
        "<Contact><PersId>35</PersId><LastName>Martin</LastName></Contact></Addressee></Contacts>"
    )

    def test___init__(self):
        storage = self.make_storage("a")
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, storage=storage)
        self.assertEqual(bp.bp_key, "e_contact")
        self.assertFalse(hasattr(bp, "source_key"))
        self.assertNotIn("e_contact", storage["data"])
        storage = self.make_storage("i")
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, storage=storage)
        self.assertEqual(bp.source_key, "_xml")
        self.assertListEqual(bp.xml_contact_keys, ["Sender", "Addressee"])
        self.assertDictEqual(
            bp.xml_ct_cols,
            {
                "Organization:OrgId:_parent_id": [
                    ("Name", "_organization"),
                    ("Level1", "_division"),
                    ("Address", "_street"),
                    ("ZipCode", "_pc"),
                    ("City", "_city"),
                ],
                "Contact:PersId:": [("FirstName", "firstname"), ("LastName", "lastname"), ("Email", "_email1")],
            },
        )
        self.assertFalse(bp.empty_store)
        self.assertEqual(bp.contact_id_key, "")
        self.assertFalse(bp.cku)
        self.assertFalse(bp.yield_original)
        self.assertDictEqual(storage["data"]["e_contact"], {})
        storage["data"]["e_contact"] = {"1": {}}
        bp = self.section(
            XmlContactStore,
            "m__2_archives_xml_contact_store",
            self.options2,
            storage=self.make_storage("m", e_contact={"1": {}}),
        )
        self.assertDictEqual(bp.storage["data"]["e_contact"], {"1": {}})  # not emptied at init
        self.assertTrue(bp.empty_store)
        self.assertTrue(bp.cku)
        self.assertTrue(bp.yield_original)
        self.assertEqual(bp.contact_id_key, "_sender_id")
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
        items = [{"_bpk": "e_contact", "_eid": "1", "_xml": self.xml}, {"_bpk": "e_contact", "_eid": "2"}]
        bp = self.section(XmlContactStore, "i__2_xml_contact_store", self.options, items, storage)
        self.assertListEqual(list(bp), [{"_bpk": "e_contact", "_eid": "2"}])
        e_contact = storage["data"]["e_contact"]
        self.assertListEqual(sorted(e_contact), ["12", "34", "35"])  # org 13 without value is not stored
        self.assertDictEqual(
            e_contact["12"],
            {
                "_organization": "Commune de Colfontaine",
                "_division": "Travaux",
                "_street": "Rue Haute 1",
                "_pc": "7340",
                "_city": "Colfontaine",
            },
        )
        self.assertIsInstance(e_contact["12"]["_organization"], str)
        # the organization id is linked on the contact only if the organization is handled before (dict order)
        org_first = list(bp.xml_ct_cols)[0].startswith("Organization")
        expected = {"firstname": "Jean", "lastname": "Dupont", "_email1": ""}
        if org_first:
            expected["_parent_id"] = "12"
        self.assertDictEqual(e_contact["34"], expected)
        self.assertDictEqual(e_contact["35"], {"firstname": "", "lastname": "Martin", "_email1": ""})
        self.assertDictEqual(dict(storage["course"]), {"i__2_xml_contact_store": 1})
        # m__2_archives_xml_contact_store: contact id on item, store emptied, item yielded for each contact
        xml = (
            "<Archive><Expediteur><K2ID>5</K2ID><Organisme>SWDE</Organisme><Nom>Robinet</Nom></Expediteur>"
            "<Expediteur><K2ID>6</K2ID><Nom>Courant</Nom></Expediteur></Archive>"
        )
        storage = self.make_storage("m", e_contact={"1": {}})
        items = [
            {"_bpk": "e_mail_recip_i", "_eid": "1", "_xml_a": xml},
            {"_bpk": "e_mail_recip_i", "_eid": "2", "_xml_a": "<Archive></Archive>"},
        ]
        bp = self.section(XmlContactStore, "m__2_archives_xml_contact_store", self.options2, items, storage)
        result = [(item["_eid"], item["_sender_id"], dict(storage["data"]["e_contact"])) for item in bp]
        self.assertListEqual(
            result,
            [
                ("1", "5", {"5": {"_organization": "SWDE", "lastname": "Robinet"}}),
                ("1", "6", {"6": {"_organization": "", "lastname": "Courant"}}),
                # Plone 4 behaviour (bug?): an item without contact tag is not yielded (yield_original)
            ],
        )
        # key uniqueness check
        xml = (
            "<Archive><Expediteur><K2ID>5</K2ID><Nom>Robinet</Nom></Expediteur>"
            "<Expediteur><K2ID>5</K2ID><Nom>Robinet bis</Nom></Expediteur>"
            "<Expediteur><K2ID>0</K2ID><Nom>Zéro</Nom></Expediteur>"
            "<Expediteur><K2ID>0</K2ID><Nom>Zéro bis</Nom></Expediteur></Archive>"
        )
        storage = self.make_storage("m")
        options = dict(self.options2, empty_store="0", yield_original="0")
        items = [{"_bpk": "e_mail_recip_i", "_eid": "3", "_xml_a": xml}]
        bp = self.section(XmlContactStore, "m__2_archives_xml_contact_store", options, items, storage)
        self.assertListEqual(list(bp), [])
        self.assertDictEqual(
            storage["data"]["e_contact"],
            {
                "5": {"_organization": "", "lastname": "Robinet bis"},
                "0": {"_organization": "", "lastname": "Zéro"},
                "1": {"_organization": "", "lastname": "Zéro bis"},
            },
        )
        self.assertEqual(items[0]["_sender_id"], "1")
        self.assertIn(
            ("dti", "ERROR", "e_mail_recip_i: _eid 3, Key '5' already in 'e_contact' data dict"), self.logs.records
        )
        # part not run
        items = [{"_bpk": "e_contact", "_eid": "1", "_xml": self.xml}]
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
        "PROFILINF0": {
            "NOM": {"_val": "Nom"},
            "ADRESSE": {"_val": "Adresse"},
            "LOCALITE": {"_val": "Localité"},
            "USERTRAIT": {"_val": "Agent traitant"},
        },
        "PARAMUTIL": {"FDPAP": {"_val": "Papier"}},
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
        self.assertEqual(bp.bp_key, "e_maval_i")
        self.assertFalse(hasattr(bp, "fields"))
        bp = self.maval()
        self.assertIs(bp.im_paths, bp.storage["data"]["e_mail_i"])
        self.assertIs(bp.batch_store, bp.storage["data"]["e_maval_i"])
        self.assertEqual(len(bp.flds_opt), 12)
        self.assertEqual(bp.flds_opt[0], ("ENTRANT", "NOM", "description,data_transfer", "after:"))
        self.assertTrue(all(isinstance(val, str) for tup in bp.flds_opt for val in tup))
        # "after:" transforms contain the full chain of previous fields
        self.assertDictEqual(
            bp.fields,
            {
                "ENTRANT": {
                    "NOM": ("description,data_transfer", "after:"),
                    "ADRESSE": ("data_transfer", "after:NOM"),
                    "LOCALITE": ("data_transfer", "after:ADRESSE,NOM"),
                    "CODEABARRE": ("internal_reference_no", "format:CB{}"),
                    "DATEDOC": ("original_mail_date", "date:%Y%m%d"),
                    "TYPEDOC": ("mail_type", ""),
                    "SERVDEST": ("treating_groups", "USERTRAIT"),
                    "USERTRAIT": ("assigned_user", "SERVDEST"),
                    "COPIES": ("recipient_groups", ""),
                    "REGCE": ("-", ""),
                    "RÉSUMÉ": ("description", ""),
                },
                "TVX": {"REMARQUE": ("data_transfer", "")},
            },
        )
        self.assertListEqual(bp.profile_keys, ["e_mails_prd", "_prd"])
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
            "200": {"path": self.path(im2)},
            "201": {"path": self.path(im3)},
            "202": {"path": "/incoming-mail/unknown"},
            "203": {"path": self.path(im4)},
            "204": {"path": self.path(im6)},
        }
        storage = self.make_storage(
            "m",
            e_mail_i=paths,
            e_mails_prd={
                "200": {"_prd": "ENTRANT"},
                "201": {"_prd": "TVX"},
                "202": {"_prd": "ENTRANT"},
                "203": {"_prd": "ENTRANT"},
                "204": {"_prd": "ENTRANT"},
            },
            e_palib=self.e_palib,
            e_service_match={"S1": {"uid": secr_uid}, "S2": {"uid": grh_uid}},
            e_mailtype_match={"T1": {"E": {"_key": "facture"}}},
            e_user_match={
                "U1": {"_p_userid": "agent", "_nom": "Agent"},
                "U4": {"_p_userid": "chef", "_nom": "Chef"},
                "U9": {"_p_userid": None, "_nom": "DELETED"},
            },
        )

        def mv(eid, fld, val, **kw):
            dic = {"_bpk": "e_mail_maval_i", "_eid": eid, "_fld": fld, "_val": val}
            dic.update(kw)
            return dic

        def upd(eid, fld, mail, **kw):
            dic = {
                "_eid": eid,
                "_path": self.path(mail),
                "_type": "dmsincomingmail",
                "_bpk": f"i_maval_{fld}",
                "_act": "U",
                "modification_date": mail.creation_date,
            }
            dic.update(kw)
            return dic

        items = [
            mv("200", "NOM", "Dupont"),
            mv("200", "UNKNOWN", "x"),
            mv("200", "REGCE", "x"),  # ignored field
            mv("202", "NOM", "x"),  # mail not found
            mv("200", "TYPEDOC", "T1"),
            mv("200", "TYPEDOC", "T9"),  # default type
            mv("200", "CODEABARRE", "123"),
            mv("200", "DATEDOC", "20230415"),
            mv("200", "DATEDOC", "2023"),
            mv("200", "COPIES", " S1 S9 "),
            mv("200", "NOM", "X", _replace="title"),
            mv("201", "REMARQUE", "Urgent"),
            {"_bpk": "e_mail_i", "_eid": "205"},
        ]
        bp = self.section(MavalHandling, "m__4_maval_i", self.options, items, storage, config=self.config)
        self.assertListEqual(
            list(bp),
            [
                upd(
                    "200",
                    "NOM",
                    im2,
                    description="Nom: Dupont\r\nCeci est la description du courrier 2",
                    data_transfer="Nom: Dupont",
                ),
                upd("200", "TYPEDOC", im2, mail_type="facture"),
                upd("200", "TYPEDOC", im2, mail_type="courrier"),
                upd("200", "CODEABARRE", im2, internal_reference_no="CB123"),
                upd("200", "DATEDOC", im2, original_mail_date=date(2023, 4, 15)),
                # Plone 4 behaviour (bug?): continue in the fields loop, the item is yielded with a None value
                upd("200", "DATEDOC", im2, original_mail_date=None),
                upd("200", "COPIES", im2, recipient_groups=[secr_uid]),
                upd("200", "NOM", im2, title="X"),
                upd("201", "REMARQUE", im3, data_transfer="REMARQUE: Urgent"),  # no label: field name
                {"_bpk": "e_mail_i", "_eid": "205"},
            ],
        )
        errors = [rec[2] for rec in self.logs.records if rec[0] == "dti"]
        self.assertEqual(errors[0], "e_mail_maval_i: _eid 200, 'ENTRANT', 'UNKNOWN' not found in fields config")
        self.assertTrue(errors[1].startswith("e_mail_maval_i: _eid 200, not a valid date '2023' in key 'k': "))
        self.assertEqual(
            errors[2],
            "e_mail_maval_i: _eid 200, None value for field 'DATEDOC', p_fld "
            "'original_mail_date', transform 'date:%Y%m%d'",
        )
        self.assertEqual(len(errors), 3)
        self.assertIn(("dto", "WARNING", "mail 202: path '/incoming-mail/unknown' not found"), self.logs.records)
        self.assertListEqual(
            sorted(storage["data"]["e_maval_i"]),
            [
                "200|CODEABARRE",
                "200|COPIES",
                "200|DATEDOC",
                "200|NOM",
                "200|REGCE",
                "200|TYPEDOC",
                "200|UNKNOWN",
                "201|REMARQUE",
                "202|NOM",
            ],
        )
        self.assertDictEqual(dict(storage["course"]), {"m__4_maval_i": 12})

        # treating group and assigned user, in both orders; the next sections update the mail (simulated)
        items = [
            mv("200", "USERTRAIT", "U1"),  # user stored temporarily in data_transfer
            mv("200", "SERVDEST", "S2"),  # treating group changed, stored user assigned
            mv("203", "SERVDEST", "S1"),
            mv("203", "USERTRAIT", "U4"),  # not an editor: name in data_transfer
            mv("203", "USERTRAIT", "U9"),  # deleted user
            mv("203", "USERTRAIT", "U1"),
            mv("204", "USERTRAIT", "U4"),
            mv("204", "SERVDEST", "S1"),  # stored user not an editor: name in data_transfer
        ]
        bp = self.section(MavalHandling, "m__4_maval_i", self.options, items, storage, config=self.config)
        gen = iter(bp)
        item2 = next(gen)
        self.assertDictEqual(item2, upd("200", "USERTRAIT", im2, data_transfer="Agent traitant: U1"))
        im2.data_transfer = item2["data_transfer"]
        self.assertDictEqual(
            next(gen), upd("200", "SERVDEST", im2, treating_groups=grh_uid, data_transfer="", assigned_user="agent")
        )
        item2 = next(gen)
        self.assertDictEqual(item2, upd("203", "SERVDEST", im4, treating_groups=secr_uid))
        im4.treating_groups = item2["treating_groups"]
        self.assertDictEqual(next(gen), upd("203", "USERTRAIT", im4, data_transfer="Agent traitant: Chef"))
        self.assertDictEqual(next(gen), upd("203", "USERTRAIT", im4))
        self.assertDictEqual(next(gen), upd("203", "USERTRAIT", im4, assigned_user="agent"))
        item2 = next(gen)
        im6.data_transfer = item2["data_transfer"]
        # Plone 4 behaviour (bug?): the temporary line is removed, giving an empty text, then the name is added
        # to the mail text (empty text_fld) where the temporary line is still present
        self.assertDictEqual(
            next(gen),
            upd(
                "204",
                "SERVDEST",
                im6,
                treating_groups=secr_uid,
                data_transfer="Agent traitant: U4\r\nAgent traitant: Chef",
            ),
        )
        self.assertListEqual(list(gen), [])
        # part not run
        bp = self.section(
            MavalHandling, "m__4_maval_i", self.options, items[:1], self.make_storage("a"), config=self.config
        )
        self.assertListEqual(list(bp), [mv("200", "USERTRAIT", "U1")])

    def test_text_field(self):
        bp = self.maval()
        im2 = self.mail("courrier2")
        # add at the first position
        item2 = {}
        self.assertIsNone(bp.text_field({"_fld": "NOM", "_val": "Dupont"}, item2, im2, "description", "after:"))
        self.assertDictEqual(item2, {"description": "Nom: Dupont\r\nCeci est la description du courrier 2"})
        bp.text_field({"_fld": "NOM", "_val": "Dupont"}, item2, im2, "data_transfer", "after:")
        self.assertEqual(item2["data_transfer"], "Nom: Dupont")
        self.assertIsInstance(item2["data_transfer"], str)
        # add after the first found previous field, from a given text
        text = "Nom: Dupont\r\nRemarque: x"
        bp.text_field(
            {"_fld": "LOCALITE", "_val": "Mons"}, item2, im2, "data_transfer", "after:ADRESSE,NOM", text_fld=text
        )
        self.assertEqual(item2["data_transfer"], "Nom: Dupont\r\nLocalité: Mons\r\nRemarque: x")
        bp.text_field({"_fld": "LOCALITE", "_val": "Mons"}, item2, im2, "data_transfer", "after:ADRESSE", text_fld=text)
        self.assertEqual(item2["data_transfer"], "Nom: Dupont\r\nRemarque: x\r\nLocalité: Mons")  # not found: end
        # add at the end, label not defined, given value, given label key
        bp.text_field({"_fld": "REMARQUE", "_val": "y"}, item2, im2, "data_transfer", "", text_fld=text)
        self.assertEqual(item2["data_transfer"], "Nom: Dupont\r\nRemarque: x\r\nREMARQUE: y")
        bp.text_field(
            {"_fld": "SERVDEST", "_val": "S1"},
            item2,
            im2,
            "data_transfer",
            "",
            lib_key="USERTRAIT",
            value="Chef",
            text_fld=text,
        )
        self.assertEqual(item2["data_transfer"], "Nom: Dupont\r\nRemarque: x\r\nAgent traitant: Chef")
        # remove
        text = "Nom: Dupont\r\nAgent traitant: U1"
        item2 = {}
        ret = bp.text_field(
            {"_fld": "SERVDEST", "_val": "S1"},
            item2,
            im2,
            "data_transfer",
            "",
            action="remove",
            lib_key="USERTRAIT",
            text_fld=text,
        )
        self.assertEqual(ret, "U1")
        self.assertDictEqual(item2, {"data_transfer": "Nom: Dupont"})
        ret = bp.text_field(
            {"_fld": "ADRESSE", "_val": ""}, item2, im2, "data_transfer", "", action="remove", text_fld=text
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
                "FORMAT_DOC": {"_prienc": "CBO", "_prizon": "FD"},
                "OTHER": {"_prienc": "TXT", "_prizon": "OT"},
            }
        )

        def mv(val):
            return {"_bpk": "e_mail_maval_i", "_eid": "200", "_fld": "DATEDOC", "_val": val}

        self.assertEqual(bp.transform_value(mv("20230415"), ""), "20230415")
        self.assertEqual(bp.transform_value(mv("20230415"), "date:%Y%m%d"), date(2023, 4, 15))
        self.assertEqual(bp.transform_value(mv("20230415"), "datetime:%Y%m%d"), datetime(2023, 4, 15))
        item = mv("2023")
        self.assertIsNone(bp.transform_value(item, "date:%Y%m%d"))
        self.assertNotIn("_error", item)  # the error is set on a copy
        self.assertIsNone(bp.transform_value(mv("18991231"), "date:%Y%m%d"))  # before 1900
        self.assertIsNone(bp.transform_value(mv(""), "date:%Y%m%d"))
        self.assertEqual(bp.transform_value(mv("123"), "format:CB{}"), "CB123")
        self.assertEqual(bp.transform_value(mv("PAP"), "e_papri:FORMAT_DOC"), "Papier")
        self.assertEqual(bp.transform_value(mv("PAP"), "e_papri:OTHER"), "PAP")
        self.assertEqual(bp.transform_value(mv("PAP"), "other:x"), "PAP")
