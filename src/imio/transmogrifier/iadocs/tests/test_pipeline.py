# -*- coding: utf-8 -*-
"""End-to-end runs of the I8s base pipeline data-transfer.cfg.1, done as execute_pipeline.py does them.

Each test copies the tiny source csv files of tests/data/pipeline1 to a temporary imports dir and runs the parts
in the order a user does: templates (0_*.csv) written, completed by hand (1_*.csv), applied, then the mails.
"""
from collective.classification.tree.utils import iterate_over_tree
from collective.contact.plonegroup.config import get_registry_organizations
from collective.documentviewer.settings import GlobalSettings
from collective.transmogrifier.transmogrifier import _load_config
from collective.transmogrifier.transmogrifier import configuration_registry
from collective.transmogrifier.transmogrifier import Transmogrifier
from datetime import date
from datetime import datetime
from DateTime import DateTime
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING
from imio.transmogrifier.iadocs.utils import get_related_parts
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID

import csv
import io
import json
import logging
import os
import pickle
import shutil
import six
import tempfile
import unittest


PKG_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "pipeline1")
CONFIG_ID = "imio.transmogrifier.iadocs.tests.pipeline1"
# The base pipeline must be adapted for each customer ("CHANGE l__1_type_insert FOR EACH CUSTOMER"): its value is
# wrapped in FAIL(...). This is the adaptation used here, passed as a Transmogrifier override.
L_TYPE = {
    "l__1_type_insert": {
        "value": "python:(item['_source_id'] == u'295' or item['_nature_id'] == u'180') and 'dmsincoming_email' "
        "or 'dmsincomingmail'"
    }
}
LONG_TITLE = u" ".join([u"Demande d'information n°{}".format(i) for i in range(1, 15)])
DEFAULT_CREATION_DATE = DateTime("2009/09/22 12:00")  # [config] default_creation_date


def read_csv(path):
    if six.PY2:
        with open(path, "rb") as fh:
            return [[cell.decode("utf8") for cell in row] for row in csv.reader(fh)]
    with io.open(path, encoding="utf8", newline="") as fh:
        return list(csv.reader(fh))


def write_csv(path, rows):
    if six.PY2:
        with open(path, "wb") as fh:
            csv.writer(fh).writerows([[cell.encode("utf8") for cell in row] for row in rows])
    else:
        with io.open(path, "w", encoding="utf8", newline="") as fh:
            csv.writer(fh).writerows(rows)


def read_text(path):
    with io.open(path, encoding="utf8") as fh:
        return fh.read()


def read_pickle(path):
    """Loads a pickle dumped by the pipeline itself in the temporary imports dir (trusted)."""
    with open(path, "rb") as fh:
        return pickle.load(fh)


def auto_parts(func_part, parts=u""):
    """Parts computed as execute_pipeline.auto_parts does (the script only runs under 'instance run')."""
    config = _load_config(CONFIG_ID)
    needed = u""
    for section in config["transmogrifier"]["pipeline"].splitlines():
        if (
            section
            and config[section]["blueprint"] == "imio.transmogrifier.iadocs.need_other"
            and func_part in (get_related_parts(section) or u"")
        ):
            needed = config[section].get("parts", u"")
            break
    return u"".join(sorted(set(needed + parts + func_part)))


class TestDataTransfer1(unittest.TestCase):
    """One test method per part of data-transfer.cfg.1, the needed parts being run before as a user does."""

    layer = IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        try:
            configuration_registry.getConfiguration(CONFIG_ID)
        except KeyError:
            configuration_registry.registerConfiguration(
                CONFIG_ID, u"", u"", os.path.join(PKG_DIR, "data-transfer.cfg.1")
            )
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        self.wd = os.path.join(tmp, "imports")
        shutil.copytree(DATA_DIR, self.wd)
        # under 'instance run', imio.pyutils setup_logger gives the dt loggers the INFO level
        for lgr in (e_logger, o_logger):
            self.addCleanup(lgr.setLevel, lgr.level)
            lgr.setLevel(logging.INFO)
        self.pf = self.portal["contacts"]["personnel-folder"]
        self.dg = self.portal.unrestrictedTraverse("contacts/plonegroup-organization/direction-generale")
        self.secr = self.dg["secretariat"]

    @property
    def reprise(self):
        """Service created by the initialization section ([config] generic_service)."""
        return self.portal.unrestrictedTraverse("contacts/plonegroup-organization/reprise-de-donnees")

    def run_pipeline(self, func_part, parts=u"", commit=False, **overrides):
        """Runs the pipeline as 'FUNC_PART=func_part execute_pipeline.py -p parts -c0|1' does (without the final
        transaction commit). The working paths of the initialization section are set to the temporary imports dir.

        :return: the storage
        """
        options = {"commit": commit, "parts": auto_parts(func_part, parts), "batch_nb": 0, "commit_nb": 0}
        self.portal.REQUEST.set("_transmo_options_", json.dumps(options))
        overrides.setdefault(
            "initialization",
            {"basepath": self.wd, "subpath": "", "csvpath": "", "filespath": os.path.join(self.wd, "files")},
        )
        handlers = [(lgr, list(lgr.handlers)) for lgr in (e_logger, o_logger)]
        transmogrifier = Transmogrifier(self.portal)
        try:
            transmogrifier(CONFIG_ID, **overrides)
        finally:  # the process of a real run ends: the log files opened by the initialization section are closed
            for lgr, hdls in handlers:
                for hdl in lgr.handlers[:]:
                    if hdl not in hdls:
                        lgr.removeHandler(hdl)
                        hdl.close()
        return transmogrifier.storage

    def path(self, filename):
        return os.path.join(self.wd, filename)

    def log(self, errors=False):
        return read_text(self.path(errors and "plone_dt_input_errors.log" or "plone_dt.log"))

    def category(self, code):
        return [cat for cat in iterate_over_tree(self.portal.tree) if cat.identifier == code][0]

    def get(self, path):
        return self.portal.unrestrictedTraverse(path)

    def last_wf_status(self, obj):
        status = list(obj.workflow_history.values())[0][-1]
        return status["action"], status["review_state"], status["actor"], status["time"]

    def _complete_services(self):
        """Completes 0_service_match.csv as a user does, with the full titles of 0_imio_service.csv."""
        orgs = dict((row[1], row[0]) for row in read_csv(self.path("0_imio_service.csv"))[1:])
        match = {u"1": u"Direction générale", u"2": u"Direction générale - Secrétariat", u"3x": u"Reprise de données"}
        rows = read_csv(self.path("0_service_match.csv"))
        for row in rows[1:]:
            title = match.get(row[0], u"")
            row.extend([orgs.get(title, u""), title])
        write_csv(self.path("1_service_match.csv"), rows)

    def _complete_mailtypes(self):
        match = {
            (u"10", u"E"): [u"facture", u"Facture", u"1"],
            (u"11", u"S"): [u"courrier-ordinaire", u"Courrier ordinaire", u"1"],
            (u"20", u"E"): [u"post", u"Courrier postal", u"1"],
            (u"20", u"S"): [u"post", u"Courrier postal", u"1"],
            (u"295", u"E"): [u"email", u"Email", u"1"],
        }
        rows = read_csv(self.path("0_mailtype_match.csv"))
        for row in rows[1:]:
            row.extend(match[(row[0], row[1])])
        write_csv(self.path("1_mailtype_match.csv"), rows)

    def _complete_users(self):
        match = {u"u1": [u"agent", u"Fred Agent"], u"u3": [u"scanner", u"Scanner"]}  # u2 has no plone user
        rows = read_csv(self.path("0_user_match.csv"))
        for row in rows[1:]:
            row.extend(match.get(row[0], [u"", u""]))
        write_csv(self.path("1_user_match.csv"), rows)

    def _prepare_mappings(self):
        """Parts a, b, c and e as a user runs them: template written, completed by hand, applied (e with -c1)."""
        self.run_pipeline(u"a")
        self._complete_services()
        self.run_pipeline(u"a")
        self.run_pipeline(u"b")
        self._complete_mailtypes()
        self.run_pipeline(u"c")
        self._complete_users()
        self.run_pipeline(u"e")
        shutil.copy(self.path("0_category_match.csv"), self.path("1_category_match.csv"))
        self.run_pipeline(u"e", commit=True)
        # contacts imported by part i (not covered here) are passed to the next parts in this pickle
        with open(self.path("2_e_contact.pkl"), "wb") as fh:
            pickle.dump({u"301": {u"path": u"/contacts/electrabel", u"_is_user": False}}, fh, 2)

    def _import_mails(self, incoming=True, outgoing=True):
        """Mappings, then parts l and/or d + p run with -c1 so that their pickles are dumped for the next parts."""
        self._prepare_mappings()
        if incoming:
            self.run_pipeline(u"l", commit=True, **L_TYPE)
        if outgoing:
            self.run_pipeline(u"d")
            self.run_pipeline(u"p", commit=True)

    def test_part_a(self):
        self.assertNotIn("reprise-de-donnees", self.portal.contacts["plonegroup-organization"])
        # first run: the matching template and the plone services list are written
        storage = self.run_pipeline(u"a")
        self.assertEqual(storage["parts"], u"a")
        self.assertIn(self.reprise.UID(), get_registry_organizations())  # generic service created and selected
        self.assertListEqual(
            read_csv(self.path("0_service_match.csv")),
            [
                [u"eid", u"parent_eid", u"etitle", u"uid", u"full_title"],
                [u"1", u"", u"Direction générale"],  # NULL parent becomes None, written empty
                [u"2", u"1", u"Secrétariat"],
                [u"3x", u"", u"Service inconnu"],
                [u"4", u"1", u"Service supprimé"],
            ],
        )
        rows = read_csv(self.path("0_imio_service.csv"))
        self.assertEqual(rows[0], [u"uid", u"full_title", u"title"])
        self.assertIn([self.secr.UID(), u"Direction générale - Secrétariat", u"Secrétariat"], rows)
        self.assertIn([self.reprise.UID(), u"Reprise de données", u"Reprise de données"], rows)  # created by init
        titles = [row[1] for row in rows[1:]]
        self.assertEqual(len(titles), 24)
        self.assertListEqual(titles, sorted(titles))
        self.assertEqual(storage["course"]["a__service_match_write"], 4)
        self.assertEqual(storage["course"]["a__plone_service_write"], 1)
        self.assertIn(u"Writing '{}'".format(self.path("0_service_match.csv")), self.log())
        self.assertIsNone(self.secr.internal_number)
        # the completed matching file is only applied when part a is run alone
        self._complete_services()
        os.remove(self.path("0_service_match.csv"))
        storage = self.run_pipeline(u"a", parts=u"b")
        self.assertEqual(storage["parts"], u"ab")
        self.assertNotIn("a__1_service_read", storage["course"])
        self.assertIsNone(self.secr.internal_number)
        # second run of part a alone: the external ids are stored on the plonegroup organizations
        storage = self.run_pipeline(u"a")
        self.assertEqual(storage["course"]["a__service_update"], 4)
        self.assertFalse(os.path.exists(self.path("0_service_match.csv")))  # template not written again
        # dry run (commit 0): the ZODB is changed anyway, only the final commit of execute_pipeline is skipped
        self.assertEqual(self.dg.internal_number, u"1")
        self.assertEqual(self.secr.internal_number, u"2")
        self.assertEqual(self.reprise.internal_number, u"3x")
        self.assertEqual([b.UID for b in api.content.find(internal_number=u"2")], [self.secr.UID()])
        self.assertIn(u"Part a: some services have been updated.", self.log())
        self.assertIn(u"e_service: _eid 4, Cannot find uid '' matched with service", self.log(errors=True))
        # a new run doesn't add the same external id twice
        self.run_pipeline(u"a")
        self.assertEqual(self.secr.internal_number, u"2")
        self.assertNotIn(u"Part a: some services have been updated.", self.log())

    def test_part_b(self):
        mt_key = "imio.dms.mail.browser.settings.IImioDmsMailConfig.{}"
        mail_types = api.portal.get_registry_record(mt_key.format("mail_types"))
        # first run: the matching template with the used natures and sources
        storage = self.run_pipeline(u"b")
        self.assertEqual(storage["parts"], u"b")
        rows = read_csv(self.path("0_mailtype_match.csv"))
        self.assertListEqual(
            rows[0], [u"eid", u"etype", u"etitle", u"nature", u"source", u"type", u"key", u"title", u"active"]
        )
        self.assertListEqual(
            sorted(rows[1:]),
            [
                [u"10", u"E", u"Facture", u"True", u"False", u"te"],
                [u"11", u"S", u"Courrier ordinaire", u"True", u"False", u"ts"],
                [u"20", u"E", u"Poste", u"False", u"True", u"fee"],
                [u"20", u"S", u"Poste", u"False", u"True", u"fes"],
                [u"295", u"E", u"Courriel", u"False", u"True", u"fee"],
            ],
        )  # "Nature inutilisée" is not used by a mail
        rows = read_csv(self.path("0_imio_mailtype.csv"))
        self.assertListEqual(rows[0], [u"type", u"key", u"title", u"active"])
        self.assertIn([u"te", u"retour-recommande", u"Retour recommandé", u"True"], rows)
        self.assertIn([u"ts", u"courrier", u"Courrier", u"True"], rows)
        self.assertEqual(len(rows), 15)
        self.assertEqual(storage["course"]["b__1_mail_read"], 6)
        self.assertEqual(storage["course"]["b__1_etype_correct"], 1)  # the internal mail is considered as incoming
        self.assertEqual(storage["course"]["b__1_mailtype_check"], 4)
        self.assertEqual(storage["course"]["b__mailtype_match_write"], 5)
        # second run with the completed matching file: the missing types are added in the configuration
        self._complete_mailtypes()
        os.remove(self.path("0_mailtype_match.csv"))
        storage = self.run_pipeline(u"b")
        self.assertEqual(storage["course"]["b__mailtype_update"], 5)
        self.assertFalse(os.path.exists(self.path("0_mailtype_match.csv")))
        omail_types = api.portal.get_registry_record(mt_key.format("omail_types"))
        self.assertListEqual(
            [dict(dic) for dic in omail_types],
            [
                {u"value": u"courrier", u"dtitle": u"Courrier", u"active": True},
                {u"value": u"courrier-ordinaire", u"dtitle": u"Courrier ordinaire", u"active": True},
            ],
        )
        self.assertListEqual(api.portal.get_registry_record(mt_key.format("mail_types")), mail_types)
        self.assertIn(u"Part b: updating 'ts' mail types with new value", self.log())

    def test_part_c(self):
        storage = self.run_pipeline(u"c")
        self.assertEqual(storage["parts"], u"c")
        self.assertListEqual(
            read_csv(self.path("0_user_match.csv")),
            [
                [u"eid", u"nom", u"prenom", u"p_uid", u"p_fullname"],
                [u"u1", u"Agent", u"Fred"],
                [u"u2", u"Doe", u"John"],
                [u"u3", u"Scanner", u""],
            ],
        )
        rows = read_csv(self.path("0_imio_user.csv"))
        self.assertListEqual(rows[0], [u"uid", u"fullname"])
        self.assertIn([u"agent", u"Fred Agent"], rows)
        self.assertIn([u"csvimport", u"csvimport"], rows)  # [config] creator, created by the initialization
        self.assertListEqual([row[1] for row in rows[1:]], sorted([row[1] for row in rows[1:]]))
        self.assertEqual(api.user.get(userid="csvimport").getProperty("email"), "csv@imio.be")
        # second run: the matching file is only loaded (used by parts d and m)
        self._complete_users()
        os.remove(self.path("0_user_match.csv"))
        storage = self.run_pipeline(u"c")
        self.assertFalse(os.path.exists(self.path("0_user_match.csv")))
        self.assertDictEqual(
            storage["data"]["e_user_match"][u"u1"],
            {u"_nom": u"Agent", u"_prenom": u"Fred", u"_p_userid": u"agent", u"_fullname": u"Fred Agent"},
        )
        self.assertEqual(storage["data"]["e_user_match"][u"u2"][u"_p_userid"], u"")

    def test_part_d(self):
        self.run_pipeline(u"a")
        self._complete_services()
        self.run_pipeline(u"a")
        self.run_pipeline(u"c")
        self._complete_users()
        storage = self.run_pipeline(u"d")
        self.assertEqual(storage["parts"], u"cd")
        self.assertEqual(storage["course"]["d__om_sender_creation"], 3)
        self.assertEqual(storage["course"]["d__personnel_creation"], 3)
        self.assertEqual(storage["course"]["post_actions"], 3)
        self.assertIn(u"Part d: some internal persons or/and held positions have been added.", self.log())
        # OM 2001 sender: existing person of the matched plone user, new held position in the mail service
        agent = self.pf["agent"]
        self.assertEqual(agent.internal_number, u"u1")
        self.assertEqual(api.content.get_state(agent), "active")
        hp = agent[self.dg.UID()]
        self.assertEqual(hp.portal_type, "held_position")
        self.assertEqual(hp.position.to_object, self.dg)
        self.assertEqual(api.content.get_state(hp), "deactivated")
        self.assertEqual(hp.creation_date, DEFAULT_CREATION_DATE)
        self.assertEqual(self.last_wf_status(hp), ("deactivate", "deactivated", "csvimport", DEFAULT_CREATION_DATE))
        self.assertEqual(hp.creators[0], u"csvimport")
        # OM 2002 sender: external user without plone user
        person = self.pf["john-doe"]
        self.assertEqual((person.firstname, person.lastname, person.userid), (u"John", u"Doe", None))
        self.assertEqual(person.internal_number, u"u2")
        self.assertEqual(api.content.get_state(person), "deactivated")
        self.assertEqual(person[self.secr.UID()].position.to_object, self.secr)
        # OM 2003 without sender
        person = self.pf["reprise-donnees"]
        self.assertEqual((person.firstname, person.lastname), (u"", u"Reprise données"))
        self.assertEqual(person.internal_number, u"None")
        self.assertEqual(person[self.reprise.UID()].position.to_object, self.reprise)
        # contact linked to a plone user without person: personnel created, without held position
        person = self.pf["scanner"]
        self.assertEqual((person.lastname, person.userid, person.internal_number), (u"Scanner", u"scanner", u"u3"))
        self.assertEqual(api.content.get_state(person), "deactivated")
        self.assertEqual(person.objectIds(), [])
        self.assertEqual(person.getOwner().getId(), "csvimport")

    def test_part_e(self):
        storage = self.run_pipeline(u"e")
        self.assertEqual(storage["parts"], u"e")
        armes = self.category(u"-1.753")
        self.assertListEqual(
            read_csv(self.path("0_category_match.csv")),
            [
                [u"eid", u"ecode", u"etitle", u"eactive", u"pcode", u"puid", u"ptitle", u"pactive"],
                [u"500", u"-1.753", u"Contrôle des armes", u"True", u"-1.753", armes.UID(), armes.title, u"True"],
                [u"501", u"1.84", u"Nouvelle catégorie/test", u"True", u"", u"", u"", u""],  # "1.84." stripped
            ],
        )  # 502 is not used by a mail or a folder
        rows = read_csv(self.path("0_imio_category.csv"))
        self.assertListEqual(rows[0], [u"pcode", u"puid", u"ptitle", u"pactive"])
        self.assertIn([u"-1.753", armes.UID(), u"Contrôle des armes et munitions.", u"True"], rows)
        self.assertEqual(len(rows), 13)
        self.assertEqual(storage["course"]["e__1_category_reject"], 8)  # 6 mails + 2 folders
        self.assertEqual(storage["course"]["e__category_used_only"], 3)
        # second run (-c1) with the template used as matching file: missing categories created, full match dumped
        shutil.copy(self.path("0_category_match.csv"), self.path("1_category_match.csv"))
        storage = self.run_pipeline(u"e", commit=True)
        self.assertEqual(storage["course"]["e__category_update"], 2)
        new = self.category(u"1.84")
        self.assertEqual((new.title, new.enabled), (u"Nouvelle catégorie-test", True))
        parent = new.aq_parent
        self.assertEqual((parent.identifier, parent.title, parent.enabled), (u"1.8", u"1.8", False))
        self.assertEqual((parent.aq_parent.identifier, parent.aq_parent.enabled), (u"1", False))
        self.assertEqual(armes.title, u"Contrôle des armes et munitions.")  # matched category unchanged
        self.assertListEqual(
            read_csv(self.path("2_category_match.csv"))[1:],
            [
                [u"500", u"-1.753", u"Contrôle des armes", u"True", u"-1.753", armes.UID(), armes.title, u"True"],
                [u"501", u"1.84", u"Nouvelle catégorie/test", u"True", u"1.84", new.UID(), new.title, u"True"],
            ],
        )
        self.assertIn(u"e_category_match:501,ClassificationCategory,N,Nouvelle catégorie-test", self.log())
        self.assertTrue(os.path.exists(self.path("plone_dt_commit.log")))
        # next runs use the full matching file only
        count = len(list(iterate_over_tree(self.portal.tree)))
        storage = self.run_pipeline(u"e")
        self.assertEqual(storage["course"]["e__category_full_match_store"], 2)
        self.assertNotIn("e__category_update", storage["course"])
        self.assertEqual(len(list(iterate_over_tree(self.portal.tree))), count)
        self.assertEqual(storage["data"]["e_category_full_match"][u"501"][u"_puid"], new.UID())

    def test_part_f(self):
        self._prepare_mappings()
        storage = self.run_pipeline(u"f", commit=True)
        self.assertEqual(storage["parts"], u"aef")
        self.assertListEqual(
            read_csv(self.path("0_folder_match.csv")),
            [
                [u"eid", u"etitle", u"earchive", u"eparent", u"eservice", u"puid", u"ptitle", u"pparent", u"origin"],
                [u"700", u"Dossier armes", u"False", u"", u"Direction générale - Secrétariat", u"", u"", u"", u""],
                [u"701", u"Sous-dossier armes", u"True", u"700", u"", u"", u"", u"", u""],
            ],
        )
        rows = read_csv(self.path("0_imio_folder.csv"))
        self.assertListEqual(rows[0], [u"uid", u"title", u"parent_uid", u"reference_nb", u"eid", u"treating_group"])
        self.assertEqual(len(rows), 23)
        folder1 = self.portal.folders["ordre-public-reglement-general-de-police"]
        self.assertIn(
            [folder1.UID(), folder1.title, u"", u"F0001", u"F0001", u"Direction générale - Secrétariat"], rows
        )
        folder = self.portal.folders["700"]
        self.assertEqual((folder.portal_type, folder.title), ("ClassificationFolder", u"Dossier armes"))
        self.assertEqual(folder.internal_reference_no, u"700")
        self.assertEqual(folder.treating_groups, self.secr.UID())
        self.assertEqual(folder.classification_categories, [self.category(u"-1.753").UID()])
        self.assertEqual(folder.classification_informations, u"LOCALISATION: Armoire 1.")
        self.assertFalse(folder.archived)
        self.assertEqual(api.content.get_state(folder), "active")
        self.assertEqual(folder.creation_date, DEFAULT_CREATION_DATE)
        subfolder = folder["701"]
        self.assertEqual((subfolder.portal_type, subfolder.title), ("ClassificationSubfolder", u"Sous-dossier armes"))
        self.assertEqual(subfolder.treating_groups, self.reprise.UID())  # [config] default_tg service
        self.assertIsNone(subfolder.classification_categories)
        self.assertEqual(subfolder.classification_informations, u"REMARQUE: Remarque.")
        self.assertTrue(subfolder.archived)
        self.assertEqual(api.content.get_state(subfolder), "deactivated")
        self.assertEqual(self.last_wf_status(subfolder)[:3], ("deactivate", "deactivated", "csvimport"))
        # with -c1: created folders list and pickle
        rows = read_csv(self.path("3_folder_created.csv"))
        self.assertEqual(len(rows), 3)
        self.assertListEqual(
            rows[1][:3] + rows[1][5:7], [u"700", u"Dossier armes", folder.UID(), u"/folders/700", u"ClassificationFolder"]
        )
        self.assertEqual(rows[2][2], subfolder.UID())
        self.assertDictEqual(
            read_pickle(self.path("2_folder.pkl")),
            {u"700": {"path": u"/folders/700"}, u"701": {"path": u"/folders/700/701"}},
        )

    def test_part_l(self):
        self._prepare_mappings()
        # the base pipeline, not adapted for the customer, stops
        with self.assertRaises(KeyError):
            self.run_pipeline(u"l")
        self.assertIn(
            u"l__1_type_insert: error getting value for eid 1001 (name 'FAIL' is not defined)", self.log(errors=True)
        )
        self.assertIsNone(self.portal["incoming-mail"].get("202301"))
        # adapted pipeline, dry run
        storage = self.run_pipeline(u"l", **L_TYPE)
        self.assertEqual(storage["parts"], u"abcel")
        self.assertEqual(storage["course"]["l__1_E_only"], 6)
        self.assertDictEqual(storage["count"]["l__count"], {u"e_mail_i": {"c": 3}})
        self.assertIn(u"1:e_mail_i:1001,IM,N,/incoming-mail/202301/1001", self.log())
        self.assertEqual(self.log(errors=True), u"")
        self.assertFalse(os.path.exists(self.path("2_e_mail_i.pkl")))  # pickles are only dumped with -c1
        self.assertNotIn(u"STARTING 'abcel'", read_text(self.path("plone_dt_commit.log")))  # only the e run
        # incoming mail with free text sender, mapped service, nature, source and category
        mail = self.get("incoming-mail/202301/1001")
        self.assertEqual(mail.portal_type, "dmsincomingmail")
        self.assertEqual(mail.title, u"Facture d'électricité")
        self.assertEqual(mail.internal_number, u"1001")
        self.assertEqual(mail.internal_reference_no, u"E0010")  # the counter continues after the existing mails
        self.assertEqual(mail.reception_date, datetime(2023, 1, 5))
        self.assertEqual(mail.original_mail_date, date(2023, 1, 3))
        self.assertIsNone(mail.due_date)
        self.assertEqual(mail.external_reference_no, u"REF/2023/1")
        self.assertEqual(mail.treating_groups, self.secr.UID())
        self.assertEqual(mail.mail_type, u"facture")
        self.assertEqual(mail.send_modes, [u"post"])
        self.assertEqual(mail.classification_categories, [self.category(u"-1.753").UID()])
        self.assertEqual([rel.to_object for rel in mail.sender], [self.portal.contacts.notencoded])
        self.assertEqual(mail.description, u"EXPÉDITEUR LIBRE: Jean Dupont\r\nREMARQUES: À payer.")
        self.assertEqual(
            mail.data_transfer, u"EXPÉDITEUR LIBRE: Jean Dupont, Rue de la Gare 1\r\nNATURE: Facture.\r\nSOURCE: Poste."
        )
        self.assertEqual((mail.creation_date, mail.modification_date), (DateTime(2023, 1, 5), DateTime(2023, 1, 5)))
        self.assertEqual(api.content.get_state(mail), "closed")
        self.assertEqual(self.last_wf_status(mail), ("close", "closed", "csvimport", DateTime(2023, 1, 5)))
        self.assertEqual(mail.creators[0], u"csvimport")
        self.assertEqual(mail.getOwner().getId(), "csvimport")
        self.assertEqual(api.content.find(internal_number=u"1001", review_state="closed")[0].getObject(), mail)
        # internal mail with a contact sender (pickle of part i), default service, dates from the reception date
        mail = self.get("incoming-mail/202306/1002")
        self.assertEqual((mail.portal_type, mail.title), ("dmsincomingmail", u"Sans titre"))
        self.assertEqual(mail.creation_date, DateTime(2023, 2, 10))
        self.assertEqual(mail.original_mail_date, date(2023, 2, 10))
        self.assertEqual(mail.treating_groups, self.reprise.UID())
        self.assertEqual((mail.mail_type, mail.send_modes), (u"courrier", [u"post"]))
        self.assertIsNone(mail.classification_categories)
        self.assertEqual([rel.to_object for rel in mail.sender], [self.portal.contacts.electrabel])
        self.assertEqual(mail.description, u"ENVOI N°: RR123.")
        self.assertEqual(mail.data_transfer, u"LIEN AVEC UN POINT DELIB.\r\nCOURRIER CONFIDENTIEL.")
        # incoming email, title split on 300 chars
        mail = self.get("incoming-mail/202309/1003")
        self.assertEqual(mail.portal_type, "dmsincoming_email")
        self.assertTrue(len(mail.title) <= 300)
        self.assertTrue(mail.description.startswith(u"SUITE TITRE: "))
        self.assertEqual(mail.title + mail.description[len(u"SUITE TITRE: "):], LONG_TITLE)
        self.assertEqual(mail.due_date, date(2023, 3, 15))
        self.assertEqual(mail.treating_groups, self.dg.UID())
        self.assertEqual((mail.mail_type, mail.send_modes), (u"facture", [u"email"]))
        self.assertEqual(mail.data_transfer, u"NATURE: Facture.\r\nSOURCE: Courriel.")

    def test_part_m(self):
        self._import_mails(outgoing=False)
        self.assertDictEqual(
            read_pickle(self.path("2_e_mail_i.pkl")),
            {
                u"1001": {"path": u"/incoming-mail/202301/1001"},
                u"1002": {"path": u"/incoming-mail/202306/1002"},
                u"1003": {"path": u"/incoming-mail/202309/1003"},
            },
        )
        self.assertTrue(os.path.exists(self.path("plone_dt_commit.log")))
        storage = self.run_pipeline(u"m")
        self.assertEqual(storage["parts"], u"acm")
        self.assertEqual(storage["course"]["m__1_already_only"], 6)  # 3 for other mails are dropped
        self.assertEqual(storage["course"]["m__1_contact_id_only"], 3)  # 1 without contact is dropped
        self.assertEqual(storage["course"]["m__1_assigned_user"], 2)
        mail = self.get("incoming-mail/202301/1001")
        self.assertEqual(mail.assigned_user, u"agent")  # principal recipient, editor in the treating group
        self.assertListEqual(
            mail.data_transfer.split(u"\r\n")[-2:],
            [
                u"DESTINATAIRE PRINCIPAL: Agent Fred, Pour traitement",
                u"DESTINATAIRE: Doe John, Pour info, message=Voir annexe",
            ],
        )
        self.assertIn(u"2:i_assigned_user:5002,IM,U,/incoming-mail/202301/1001", self.log())
        self.assertIsNone(self.get("incoming-mail/202306/1002").assigned_user)
        self.assertFalse(os.path.exists(self.path("2_e_mail_ass_user.pkl")))

    def test_part_t(self):
        self._import_mails(outgoing=False)
        mail = self.get("incoming-mail/202301/1001")
        # Plone 4 behaviour (bug?): transmogrify.dexterity schemaupdater sets the missing default value of
        # IIconifiedCategorization.default_titles, a read-only property (collective.iconifiedcategory 0.76):
        # the first dmsmainfile update stops the run.
        with self.assertRaises(AttributeError):
            self.run_pipeline(u"t")
        self.assertEqual(mail.objectIds(), ["6001"])
        self.assertEqual(mail["6001"].portal_type, "dmsmainfile")
        # the run being interrupted, the documentviewer auto conversion stays disabled
        self.assertFalse(GlobalSettings(self.portal).auto_convert)
        self.assertIn(u"Reading '{}'".format(self.path("eCourriersFichiers.csv")), self.log())

    def test_part_p(self):
        self._prepare_mappings()
        self.run_pipeline(u"d")
        storage = self.run_pipeline(u"p")
        self.assertEqual(storage["parts"], u"abcep")
        self.assertEqual(storage["course"]["p__1_S_only"], 6)
        self.assertDictEqual(storage["count"]["p__count"], {u"e_mail_o": {"c": 3}})
        self.assertEqual(self.log(errors=True), u"")
        self.assertFalse(os.path.exists(self.path("2_e_mail_o.pkl")))
        # outgoing mail sent by a matched user, mapped service, nature, source and created category
        mail = self.get("outgoing-mail/202302/2001")
        self.assertEqual((mail.portal_type, mail.title), ("dmsoutgoingmail", u"Réponse à la facture"))
        self.assertEqual((mail.internal_number, mail.internal_reference_no), (u"2001", u"S0010"))
        self.assertEqual(mail.outgoing_date, datetime(2023, 1, 11))
        self.assertEqual(mail.mail_date, date(2023, 1, 10))
        self.assertEqual(mail.treating_groups, self.dg.UID())
        self.assertEqual(mail.sender, self.pf["agent"][self.dg.UID()].UID())  # held position created by part d
        self.assertIsNone(mail.recipients)
        self.assertEqual((mail.mail_type, mail.send_modes), (u"courrier-ordinaire", [u"post"]))
        self.assertEqual(mail.classification_categories, [self.category(u"1.84").UID()])
        self.assertEqual(mail.data_transfer, u"NATURE: Courrier ordinaire.\r\nSOURCE: Poste.")
        self.assertEqual(api.content.get_state(mail), "sent")
        self.assertEqual(self.last_wf_status(mail), ("mark_as_sent", "sent", "csvimport", DateTime(2023, 1, 11)))
        # sender without plone user, dates from the outgoing date
        mail = self.get("outgoing-mail/202307/2002")
        self.assertEqual(mail.creation_date, DateTime(2023, 2, 12))
        self.assertEqual(mail.mail_date, date(2023, 2, 12))
        self.assertEqual(mail.sender, self.pf["john-doe"][self.secr.UID()].UID())
        self.assertEqual(mail.treating_groups, self.secr.UID())
        self.assertEqual((mail.mail_type, mail.data_transfer), (u"courrier", None))
        # no sender
        mail = self.get("outgoing-mail/202311/2003")
        self.assertEqual(mail.sender, self.pf["reprise-donnees"][self.reprise.UID()].UID())
        self.assertEqual(mail.treating_groups, self.reprise.UID())

    def test_part_q(self):
        self._import_mails(incoming=False)
        storage = self.run_pipeline(u"q")
        self.assertEqual(storage["parts"], u"q")  # q__need is not in the pipeline
        self.assertEqual(storage["course"]["q__1_already_only"], 6)
        self.assertEqual(storage["course"]["q__1_recipients"], 2)
        mail = self.get("outgoing-mail/202302/2001")
        self.assertEqual([rel.to_object for rel in mail.recipients], [self.portal.contacts.electrabel])
        self.assertEqual(mail.description, u"")
        mail = self.get("outgoing-mail/202307/2002")
        self.assertEqual([rel.to_object for rel in mail.recipients], [self.portal.contacts.notencoded])
        self.assertEqual(mail.description, u"DESTINATAIRE LIBRE: Madame Durant")
        self.assertEqual(mail.data_transfer, u"DESTINATAIRE LIBRE: Madame Durant, Rue Haute 5")
        self.assertIn(u"1:e_mail_recipient:5005,OM,U,/outgoing-mail/202302/2001", self.log())

    def test_part_r(self):
        self._import_mails()
        storage = self.run_pipeline(u"r")
        self.assertEqual(storage["parts"], u"ar")
        self.assertEqual(storage["course"]["r__1_recipient_groups_update"], 2)
        self.assertEqual(storage["course"]["r__read_label_for_recipient_group"], 1)  # only for the incoming mail
        self.assertEqual(self.get("incoming-mail/202301/1001").recipient_groups, [self.dg.UID()])
        self.assertEqual(self.get("outgoing-mail/202302/2001").recipient_groups, [self.secr.UID()])
        self.assertIsNone(self.get("outgoing-mail/202307/2002").recipient_groups)

    def test_part_s(self):
        self._import_mails()
        # folders not yet created by part f: their title is put in the description
        storage = self.run_pipeline(u"s")
        self.assertEqual(storage["parts"], u"es")
        self.assertEqual(storage["course"]["s__1_classification_folders_update"], 4)
        mail = self.get("incoming-mail/202301/1001")
        # Plone 4 behaviour (bug?): s__1_folder_store stores '_title' but the folder csv column is 'title'
        self.assertEqual(mail.description, u"EXPÉDITEUR LIBRE: Jean Dupont\r\nREMARQUES: À payer.\r\nDOSSIER: None")
        self.assertIsNone(mail.classification_folders)
        errors = self.log(errors=True)
        self.assertIn(u"e_mail_folder: _eid 1001, Cannot find folder_id in plone '700' => put in in description", errors)
        self.assertIn(u"e_mail_folder: _eid 1002, Cannot find folder_id '702' in existing folders", errors)
        # the empty mail category is filled with the folder one
        mail = self.get("incoming-mail/202309/1003")
        self.assertEqual(mail.classification_categories, [self.category(u"-1.753").UID()])
        # with the folders created
        self.run_pipeline(u"f")
        self.run_pipeline(u"s")
        folder = self.portal.folders["700"]
        self.assertEqual(self.get("incoming-mail/202301/1001").classification_folders, [folder.UID()])
        self.assertEqual(self.get("incoming-mail/202309/1003").classification_folders, [folder.UID()])
        self.assertEqual(self.get("outgoing-mail/202302/2001").classification_folders, [folder["701"].UID()])

    def test_part_u(self):
        self._import_mails(incoming=False)
        mail = self.get("outgoing-mail/202302/2001")
        # Plone 4 behaviour (bug?): as in part t, the schemaupdater cannot set IIconifiedCategorization.default_titles
        with self.assertRaises(AttributeError):
            self.run_pipeline(u"u")
        self.assertEqual(mail.objectIds(), ["6005"])
        self.assertEqual(mail["6005"].portal_type, "dmsommainfile")
        self.assertFalse(GlobalSettings(self.portal).auto_convert)

    def test_part_x(self):
        self._import_mails()
        storage = self.run_pipeline(u"x")
        self.assertEqual(storage["parts"], u"x")
        self.assertEqual(storage["course"]["x__1_already_only"], 2)  # the link to an unknown mail is dropped
        self.assertEqual(storage["course"]["x__1_reply_to_update"], 1)
        omail = self.get("outgoing-mail/202302/2001")
        self.assertEqual([rel.to_object for rel in omail.reply_to], [self.get("incoming-mail/202301/1001")])
        self.assertIsNone(self.get("outgoing-mail/202307/2002").reply_to)
        self.assertIn(u"1:reply_to:2001,OM,U,/outgoing-mail/202302/2001", self.log())

    def test_part_y(self):
        self._import_mails()
        self.run_pipeline(u"x")  # modifies the outgoing mail
        omail = self.get("outgoing-mail/202302/2001")
        self.assertNotEqual(omail.modification_date, omail.creation_date)
        storage = self.run_pipeline(u"y")
        self.assertEqual(storage["parts"], u"y")
        self.assertEqual(storage["course"]["y__mails_load"], 6)
        self.assertEqual(storage["course"]["y__type"], 1)  # only the modified mail is updated
        self.assertEqual(omail.modification_date, omail.creation_date)
        self.assertIn(u"e_mail:2001,OM,?,/outgoing-mail/202302/2001", self.log())

