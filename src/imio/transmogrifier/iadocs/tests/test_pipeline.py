"""End-to-end runs of the I8s base pipeline data-transfer.cfg.1, done as execute_pipeline.py does them.

Each test copies the tiny source csv files of tests/data/pipeline1 to a temporary imports dir and runs the parts
in the order a user does: templates (0_*.csv) written, completed by hand (1_*.csv), applied, then the mails.
"""

from collective.classification.tree.utils import iterate_over_tree
from collective.contact.plonegroup.config import get_registry_organizations
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
import json
import logging
import os
import pickle
import shutil
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
LONG_TITLE = " ".join([f"Demande d'information n°{i}" for i in range(1, 15)])
DEFAULT_CREATION_DATE = DateTime("2009/09/22 12:00")  # [config] default_creation_date


def read_csv(path):
    with open(path, encoding="utf8", newline="") as fh:
        return list(csv.reader(fh))


def write_csv(path, rows):
    with open(path, "w", encoding="utf8", newline="") as fh:
        csv.writer(fh).writerows(rows)


def read_text(path):
    with open(path, encoding="utf8") as fh:
        return fh.read()


def read_bytes(path):
    with open(path, "rb") as fh:
        return fh.read()


def read_pickle(path):
    """Loads a pickle dumped by the pipeline itself in the temporary imports dir (trusted)."""
    with open(path, "rb") as fh:
        return pickle.load(fh)


def auto_parts(func_part, parts=""):
    """Parts computed as execute_pipeline.auto_parts does (the script only runs under 'instance run')."""
    config = _load_config(CONFIG_ID)
    needed = ""
    for section in config["transmogrifier"]["pipeline"].splitlines():
        if (
            section
            and config[section]["blueprint"] == "imio.transmogrifier.iadocs.need_other"
            and func_part in (get_related_parts(section) or "")
        ):
            needed = config[section].get("parts", "")
            break
    return "".join(sorted(set(needed + parts + func_part)))


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
                CONFIG_ID, "", "", os.path.join(PKG_DIR, "data-transfer.cfg.1")
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

    def run_pipeline(self, func_part, parts="", commit=False, **overrides):
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
        orgs = {row[1]: row[0] for row in read_csv(self.path("0_imio_service.csv"))[1:]}
        match = {"1": "Direction générale", "2": "Direction générale - Secrétariat", "3x": "Reprise de données"}
        rows = read_csv(self.path("0_service_match.csv"))
        for row in rows[1:]:
            title = match.get(row[0], "")
            row.extend([orgs.get(title, ""), title])
        write_csv(self.path("1_service_match.csv"), rows)

    def _complete_mailtypes(self):
        match = {
            ("10", "E"): ["facture", "Facture", "1"],
            ("11", "S"): ["courrier-ordinaire", "Courrier ordinaire", "1"],
            ("20", "E"): ["post", "Courrier postal", "1"],
            ("20", "S"): ["post", "Courrier postal", "1"],
            ("295", "E"): ["email", "Email", "1"],
        }
        rows = read_csv(self.path("0_mailtype_match.csv"))
        for row in rows[1:]:
            row.extend(match[(row[0], row[1])])
        write_csv(self.path("1_mailtype_match.csv"), rows)

    def _complete_users(self):
        match = {"u1": ["agent", "Fred Agent"], "u3": ["scanner", "Scanner"]}  # u2 has no plone user
        rows = read_csv(self.path("0_user_match.csv"))
        for row in rows[1:]:
            row.extend(match.get(row[0], ["", ""]))
        write_csv(self.path("1_user_match.csv"), rows)

    def _prepare_mappings(self):
        """Parts a, b, c and e as a user runs them: template written, completed by hand, applied (e with -c1)."""
        self.run_pipeline("a")
        self._complete_services()
        self.run_pipeline("a")
        self.run_pipeline("b")
        self._complete_mailtypes()
        self.run_pipeline("c")
        self._complete_users()
        self.run_pipeline("e")
        shutil.copy(self.path("0_category_match.csv"), self.path("1_category_match.csv"))
        self.run_pipeline("e", commit=True)
        # contacts imported by part i (not covered here) are passed to the next parts in this pickle
        with open(self.path("2_e_contact.pkl"), "wb") as fh:
            pickle.dump({"301": {"path": "/contacts/electrabel", "_is_user": False}}, fh, 2)

    def _import_mails(self, incoming=True, outgoing=True):
        """Mappings, then parts l and/or d + p run with -c1 so that their pickles are dumped for the next parts."""
        self._prepare_mappings()
        if incoming:
            self.run_pipeline("l", commit=True, **L_TYPE)
        if outgoing:
            self.run_pipeline("d")
            self.run_pipeline("p", commit=True)

    def test_part_a(self):
        self.assertNotIn("reprise-de-donnees", self.portal.contacts["plonegroup-organization"])
        # first run: the matching template and the plone services list are written
        storage = self.run_pipeline("a")
        self.assertEqual(storage["parts"], "a")
        self.assertIn(self.reprise.UID(), get_registry_organizations())  # generic service created and selected
        self.assertListEqual(
            read_csv(self.path("0_service_match.csv")),
            [
                ["eid", "parent_eid", "etitle", "uid", "full_title"],
                ["1", "", "Direction générale"],  # NULL parent becomes None, written empty
                ["2", "1", "Secrétariat"],
                ["3x", "", "Service inconnu"],
                ["4", "1", "Service supprimé"],
            ],
        )
        rows = read_csv(self.path("0_imio_service.csv"))
        self.assertEqual(rows[0], ["uid", "full_title", "title"])
        self.assertIn([self.secr.UID(), "Direction générale - Secrétariat", "Secrétariat"], rows)
        self.assertIn([self.reprise.UID(), "Reprise de données", "Reprise de données"], rows)  # created by init
        titles = [row[1] for row in rows[1:]]
        self.assertEqual(len(titles), 24)
        self.assertListEqual(titles, sorted(titles))
        self.assertEqual(storage["course"]["a__service_match_write"], 4)
        self.assertEqual(storage["course"]["a__plone_service_write"], 1)
        self.assertIn("Writing '{}'".format(self.path("0_service_match.csv")), self.log())
        self.assertIsNone(self.secr.internal_number)
        # the completed matching file is only applied when part a is run alone
        self._complete_services()
        os.remove(self.path("0_service_match.csv"))
        storage = self.run_pipeline("a", parts="b")
        self.assertEqual(storage["parts"], "ab")
        self.assertNotIn("a__1_service_read", storage["course"])
        self.assertIsNone(self.secr.internal_number)
        # second run of part a alone: the external ids are stored on the plonegroup organizations
        storage = self.run_pipeline("a")
        self.assertEqual(storage["course"]["a__service_update"], 4)
        self.assertFalse(os.path.exists(self.path("0_service_match.csv")))  # template not written again
        # dry run (commit 0): the ZODB is changed anyway, only the final commit of execute_pipeline is skipped
        self.assertEqual(self.dg.internal_number, "1")
        self.assertEqual(self.secr.internal_number, "2")
        self.assertEqual(self.reprise.internal_number, "3x")
        self.assertEqual([b.UID for b in api.content.find(internal_number="2")], [self.secr.UID()])
        self.assertIn("Part a: some services have been updated.", self.log())
        self.assertIn("e_service: _eid 4, Cannot find uid '' matched with service", self.log(errors=True))
        # a new run doesn't add the same external id twice
        self.run_pipeline("a")
        self.assertEqual(self.secr.internal_number, "2")
        self.assertNotIn("Part a: some services have been updated.", self.log())

    def test_part_b(self):
        mt_key = "imio.dms.mail.browser.settings.IImioDmsMailConfig.{}"
        mail_types = api.portal.get_registry_record(mt_key.format("mail_types"))
        # first run: the matching template with the used natures and sources
        storage = self.run_pipeline("b")
        self.assertEqual(storage["parts"], "b")
        rows = read_csv(self.path("0_mailtype_match.csv"))
        self.assertListEqual(rows[0], ["eid", "etype", "etitle", "nature", "source", "type", "key", "title", "active"])
        self.assertListEqual(
            sorted(rows[1:]),
            [
                ["10", "E", "Facture", "True", "False", "te"],
                ["11", "S", "Courrier ordinaire", "True", "False", "ts"],
                ["20", "E", "Poste", "False", "True", "fee"],
                ["20", "S", "Poste", "False", "True", "fes"],
                ["295", "E", "Courriel", "False", "True", "fee"],
            ],
        )  # "Nature inutilisée" is not used by a mail
        rows = read_csv(self.path("0_imio_mailtype.csv"))
        self.assertListEqual(rows[0], ["type", "key", "title", "active"])
        self.assertIn(["te", "retour-recommande", "Retour recommandé", "True"], rows)
        self.assertIn(["ts", "courrier", "Courrier", "True"], rows)
        self.assertEqual(len(rows), 15)
        self.assertEqual(storage["course"]["b__1_mail_read"], 6)
        self.assertEqual(storage["course"]["b__1_etype_correct"], 1)  # the internal mail is considered as incoming
        self.assertEqual(storage["course"]["b__1_mailtype_check"], 4)
        self.assertEqual(storage["course"]["b__mailtype_match_write"], 5)
        # second run with the completed matching file: the missing types are added in the configuration
        self._complete_mailtypes()
        os.remove(self.path("0_mailtype_match.csv"))
        storage = self.run_pipeline("b")
        self.assertEqual(storage["course"]["b__mailtype_update"], 5)
        self.assertFalse(os.path.exists(self.path("0_mailtype_match.csv")))
        omail_types = api.portal.get_registry_record(mt_key.format("omail_types"))
        self.assertListEqual(
            [dict(dic) for dic in omail_types],
            [
                {"value": "courrier", "dtitle": "Courrier", "active": True},
                {"value": "courrier-ordinaire", "dtitle": "Courrier ordinaire", "active": True},
            ],
        )
        self.assertListEqual(api.portal.get_registry_record(mt_key.format("mail_types")), mail_types)
        self.assertIn("Part b: updating 'ts' mail types with new value", self.log())

    def test_part_c(self):
        storage = self.run_pipeline("c")
        self.assertEqual(storage["parts"], "c")
        self.assertListEqual(
            read_csv(self.path("0_user_match.csv")),
            [
                ["eid", "nom", "prenom", "p_uid", "p_fullname"],
                ["u1", "Agent", "Fred"],
                ["u2", "Doe", "John"],
                ["u3", "Scanner", ""],
            ],
        )
        rows = read_csv(self.path("0_imio_user.csv"))
        self.assertListEqual(rows[0], ["uid", "fullname"])
        self.assertIn(["agent", "Fred Agent"], rows)
        self.assertIn(["csvimport", "csvimport"], rows)  # [config] creator, created by the initialization
        self.assertListEqual([row[1] for row in rows[1:]], sorted([row[1] for row in rows[1:]]))
        self.assertEqual(api.user.get(userid="csvimport").getProperty("email"), "csv@imio.be")
        # second run: the matching file is only loaded (used by parts d and m)
        self._complete_users()
        os.remove(self.path("0_user_match.csv"))
        storage = self.run_pipeline("c")
        self.assertFalse(os.path.exists(self.path("0_user_match.csv")))
        self.assertDictEqual(
            storage["data"]["e_user_match"]["u1"],
            {"_nom": "Agent", "_prenom": "Fred", "_p_userid": "agent", "_fullname": "Fred Agent"},
        )
        self.assertEqual(storage["data"]["e_user_match"]["u2"]["_p_userid"], "")

    def test_part_d(self):
        self.run_pipeline("a")
        self._complete_services()
        self.run_pipeline("a")
        self.run_pipeline("c")
        self._complete_users()
        storage = self.run_pipeline("d")
        self.assertEqual(storage["parts"], "cd")
        self.assertEqual(storage["course"]["d__om_sender_creation"], 3)
        self.assertEqual(storage["course"]["d__personnel_creation"], 3)
        self.assertEqual(storage["course"]["post_actions"], 3)
        self.assertIn("Part d: some internal persons or/and held positions have been added.", self.log())
        # OM 2001 sender: existing person of the matched plone user, new held position in the mail service
        agent = self.pf["agent"]
        self.assertEqual(agent.internal_number, "u1")
        self.assertEqual(api.content.get_state(agent), "active")
        hp = agent[self.dg.UID()]
        self.assertEqual(hp.portal_type, "held_position")
        self.assertEqual(hp.position.to_object, self.dg)
        self.assertEqual(api.content.get_state(hp), "deactivated")
        self.assertEqual(hp.creation_date, DEFAULT_CREATION_DATE)
        self.assertEqual(self.last_wf_status(hp), ("deactivate", "deactivated", "csvimport", DEFAULT_CREATION_DATE))
        self.assertEqual(hp.creators[0], "csvimport")
        # OM 2002 sender: external user without plone user
        person = self.pf["john-doe"]
        self.assertEqual((person.firstname, person.lastname, person.userid), ("John", "Doe", None))
        self.assertEqual(person.internal_number, "u2")
        self.assertEqual(api.content.get_state(person), "deactivated")
        self.assertEqual(person[self.secr.UID()].position.to_object, self.secr)
        # OM 2003 without sender
        person = self.pf["reprise-donnees"]
        self.assertEqual((person.firstname, person.lastname), ("", "Reprise données"))
        self.assertEqual(person.internal_number, "None")
        self.assertEqual(person[self.reprise.UID()].position.to_object, self.reprise)
        # contact linked to a plone user without person: personnel created, without held position
        person = self.pf["scanner"]
        self.assertEqual((person.lastname, person.userid, person.internal_number), ("Scanner", "scanner", "u3"))
        self.assertEqual(api.content.get_state(person), "deactivated")
        self.assertEqual(person.objectIds(), [])
        self.assertEqual(person.getOwner().getId(), "csvimport")

    def test_part_e(self):
        storage = self.run_pipeline("e")
        self.assertEqual(storage["parts"], "e")
        armes = self.category("-1.753")
        self.assertListEqual(
            read_csv(self.path("0_category_match.csv")),
            [
                ["eid", "ecode", "etitle", "eactive", "pcode", "puid", "ptitle", "pactive"],
                ["500", "-1.753", "Contrôle des armes", "True", "-1.753", armes.UID(), armes.title, "True"],
                ["501", "1.84", "Nouvelle catégorie/test", "True", "", "", "", ""],  # "1.84." stripped
            ],
        )  # 502 is not used by a mail or a folder
        rows = read_csv(self.path("0_imio_category.csv"))
        self.assertListEqual(rows[0], ["pcode", "puid", "ptitle", "pactive"])
        self.assertIn(["-1.753", armes.UID(), "Contrôle des armes et munitions.", "True"], rows)
        self.assertEqual(len(rows), 13)
        self.assertEqual(storage["course"]["e__1_category_reject"], 8)  # 6 mails + 2 folders
        self.assertEqual(storage["course"]["e__category_used_only"], 3)
        # second run (-c1) with the template used as matching file: missing categories created, full match dumped
        shutil.copy(self.path("0_category_match.csv"), self.path("1_category_match.csv"))
        storage = self.run_pipeline("e", commit=True)
        self.assertEqual(storage["course"]["e__category_update"], 2)
        new = self.category("1.84")
        self.assertEqual((new.title, new.enabled), ("Nouvelle catégorie-test", True))
        parent = new.aq_parent
        self.assertEqual((parent.identifier, parent.title, parent.enabled), ("1.8", "1.8", False))
        self.assertEqual((parent.aq_parent.identifier, parent.aq_parent.enabled), ("1", False))
        self.assertEqual(armes.title, "Contrôle des armes et munitions.")  # matched category unchanged
        self.assertListEqual(
            read_csv(self.path("2_category_match.csv"))[1:],
            [
                ["500", "-1.753", "Contrôle des armes", "True", "-1.753", armes.UID(), armes.title, "True"],
                ["501", "1.84", "Nouvelle catégorie/test", "True", "1.84", new.UID(), new.title, "True"],
            ],
        )
        self.assertIn("e_category_match:501,ClassificationCategory,N,Nouvelle catégorie-test", self.log())
        self.assertTrue(os.path.exists(self.path("plone_dt_commit.log")))
        # next runs use the full matching file only
        count = len(list(iterate_over_tree(self.portal.tree)))
        storage = self.run_pipeline("e")
        self.assertEqual(storage["course"]["e__category_full_match_store"], 2)
        self.assertNotIn("e__category_update", storage["course"])
        self.assertEqual(len(list(iterate_over_tree(self.portal.tree))), count)
        self.assertEqual(storage["data"]["e_category_full_match"]["501"]["_puid"], new.UID())

    def test_part_f(self):
        self._prepare_mappings()
        storage = self.run_pipeline("f", commit=True)
        self.assertEqual(storage["parts"], "aef")
        self.assertListEqual(
            read_csv(self.path("0_folder_match.csv")),
            [
                ["eid", "etitle", "earchive", "eparent", "eservice", "puid", "ptitle", "pparent", "origin"],
                ["700", "Dossier armes", "False", "", "Direction générale - Secrétariat", "", "", "", ""],
                ["701", "Sous-dossier armes", "True", "700", "", "", "", "", ""],
            ],
        )
        rows = read_csv(self.path("0_imio_folder.csv"))
        self.assertListEqual(rows[0], ["uid", "title", "parent_uid", "reference_nb", "eid", "treating_group"])
        self.assertEqual(len(rows), 23)
        folder1 = self.portal.folders["ordre-public-reglement-general-de-police"]
        self.assertIn([folder1.UID(), folder1.title, "", "F0001", "F0001", "Direction générale - Secrétariat"], rows)
        folder = self.portal.folders["700"]
        self.assertEqual((folder.portal_type, folder.title), ("ClassificationFolder", "Dossier armes"))
        self.assertEqual(folder.internal_reference_no, "700")
        self.assertEqual(folder.treating_groups, self.secr.UID())
        self.assertEqual(folder.classification_categories, [self.category("-1.753").UID()])
        self.assertEqual(folder.classification_informations, "LOCALISATION: Armoire 1.")
        self.assertFalse(folder.archived)
        self.assertEqual(api.content.get_state(folder), "active")
        self.assertEqual(folder.creation_date, DEFAULT_CREATION_DATE)
        subfolder = folder["701"]
        self.assertEqual((subfolder.portal_type, subfolder.title), ("ClassificationSubfolder", "Sous-dossier armes"))
        self.assertEqual(subfolder.treating_groups, self.reprise.UID())  # [config] default_tg service
        self.assertIsNone(subfolder.classification_categories)
        self.assertEqual(subfolder.classification_informations, "REMARQUE: Remarque.")
        self.assertTrue(subfolder.archived)
        self.assertEqual(api.content.get_state(subfolder), "deactivated")
        self.assertEqual(self.last_wf_status(subfolder)[:3], ("deactivate", "deactivated", "csvimport"))
        # with -c1: created folders list and pickle
        rows = read_csv(self.path("3_folder_created.csv"))
        self.assertEqual(len(rows), 3)
        self.assertListEqual(
            rows[1][:3] + rows[1][5:7],
            ["700", "Dossier armes", folder.UID(), "/folders/700", "ClassificationFolder"],
        )
        self.assertEqual(rows[2][2], subfolder.UID())
        self.assertDictEqual(
            read_pickle(self.path("2_folder.pkl")),
            {"700": {"path": "/folders/700"}, "701": {"path": "/folders/700/701"}},
        )

    def test_part_l(self):
        self._prepare_mappings()
        # the base pipeline, not adapted for the customer, stops
        with self.assertRaises(KeyError):
            self.run_pipeline("l")
        self.assertIn(
            "l__1_type_insert: error getting value for eid 1001 (name 'FAIL' is not defined)", self.log(errors=True)
        )
        self.assertIsNone(self.portal["incoming-mail"].get("202301"))
        # adapted pipeline, dry run
        storage = self.run_pipeline("l", **L_TYPE)
        self.assertEqual(storage["parts"], "abcel")
        self.assertEqual(storage["course"]["l__1_E_only"], 6)
        self.assertDictEqual(storage["count"]["l__count"], {"e_mail_i": {"c": 3}})
        self.assertIn("1:e_mail_i:1001,IM,N,/incoming-mail/202301/1001", self.log())
        self.assertEqual(self.log(errors=True), "")
        self.assertFalse(os.path.exists(self.path("2_e_mail_i.pkl")))  # pickles are only dumped with -c1
        self.assertNotIn("STARTING 'abcel'", read_text(self.path("plone_dt_commit.log")))  # only the e run
        # incoming mail with free text sender, mapped service, nature, source and category
        mail = self.get("incoming-mail/202301/1001")
        self.assertEqual(mail.portal_type, "dmsincomingmail")
        self.assertEqual(mail.title, "Facture d'électricité")
        self.assertEqual(mail.internal_number, "1001")
        self.assertEqual(mail.internal_reference_no, "E0010")  # the counter continues after the existing mails
        self.assertEqual(mail.reception_date, datetime(2023, 1, 5))
        self.assertEqual(mail.original_mail_date, date(2023, 1, 3))
        self.assertIsNone(mail.due_date)
        self.assertEqual(mail.external_reference_no, "REF/2023/1")
        self.assertEqual(mail.treating_groups, self.secr.UID())
        self.assertEqual(mail.mail_type, "facture")
        self.assertEqual(mail.send_modes, ["post"])
        self.assertEqual(mail.classification_categories, [self.category("-1.753").UID()])
        self.assertEqual([rel.to_object for rel in mail.sender], [self.portal.contacts.notencoded])
        self.assertEqual(mail.description, "EXPÉDITEUR LIBRE: Jean Dupont\r\nREMARQUES: À payer.")
        self.assertEqual(
            mail.data_transfer, "EXPÉDITEUR LIBRE: Jean Dupont, Rue de la Gare 1\r\nNATURE: Facture.\r\nSOURCE: Poste."
        )
        self.assertEqual((mail.creation_date, mail.modification_date), (DateTime(2023, 1, 5), DateTime(2023, 1, 5)))
        self.assertEqual(api.content.get_state(mail), "closed")
        self.assertEqual(self.last_wf_status(mail), ("close", "closed", "csvimport", DateTime(2023, 1, 5)))
        self.assertEqual(mail.creators[0], "csvimport")
        self.assertEqual(mail.getOwner().getId(), "csvimport")
        self.assertEqual(api.content.find(internal_number="1001", review_state="closed")[0].getObject(), mail)
        # internal mail with a contact sender (pickle of part i), default service, dates from the reception date
        mail = self.get("incoming-mail/202306/1002")
        self.assertEqual((mail.portal_type, mail.title), ("dmsincomingmail", "Sans titre"))
        self.assertEqual(mail.creation_date, DateTime(2023, 2, 10))
        self.assertEqual(mail.original_mail_date, date(2023, 2, 10))
        self.assertEqual(mail.treating_groups, self.reprise.UID())
        self.assertEqual((mail.mail_type, mail.send_modes), ("courrier", ["post"]))
        self.assertIsNone(mail.classification_categories)
        self.assertEqual([rel.to_object for rel in mail.sender], [self.portal.contacts.electrabel])
        self.assertEqual(mail.description, "ENVOI N°: RR123.")
        self.assertEqual(mail.data_transfer, "LIEN AVEC UN POINT DELIB.\r\nCOURRIER CONFIDENTIEL.")
        # incoming email, title split on 300 chars
        mail = self.get("incoming-mail/202309/1003")
        self.assertEqual(mail.portal_type, "dmsincoming_email")
        self.assertTrue(len(mail.title) <= 300)
        self.assertTrue(mail.description.startswith("SUITE TITRE: "))
        self.assertEqual(mail.title + mail.description[len("SUITE TITRE: ") :], LONG_TITLE)
        self.assertEqual(mail.due_date, date(2023, 3, 15))
        self.assertEqual(mail.treating_groups, self.dg.UID())
        self.assertEqual((mail.mail_type, mail.send_modes), ("facture", ["email"]))
        self.assertEqual(mail.data_transfer, "NATURE: Facture.\r\nSOURCE: Courriel.")

    def test_part_m(self):
        self._import_mails(outgoing=False)
        self.assertDictEqual(
            read_pickle(self.path("2_e_mail_i.pkl")),
            {
                "1001": {"path": "/incoming-mail/202301/1001"},
                "1002": {"path": "/incoming-mail/202306/1002"},
                "1003": {"path": "/incoming-mail/202309/1003"},
            },
        )
        self.assertTrue(os.path.exists(self.path("plone_dt_commit.log")))
        storage = self.run_pipeline("m")
        self.assertEqual(storage["parts"], "acm")
        self.assertEqual(storage["course"]["m__1_already_only"], 6)  # 3 for other mails are dropped
        self.assertEqual(storage["course"]["m__1_contact_id_only"], 3)  # 1 without contact is dropped
        self.assertEqual(storage["course"]["m__1_assigned_user"], 2)
        mail = self.get("incoming-mail/202301/1001")
        self.assertEqual(mail.assigned_user, "agent")  # principal recipient, editor in the treating group
        self.assertListEqual(
            mail.data_transfer.split("\r\n")[-2:],
            [
                "DESTINATAIRE PRINCIPAL: Agent Fred, Pour traitement",
                "DESTINATAIRE: Doe John, Pour info, message=Voir annexe",
            ],
        )
        self.assertIn("2:i_assigned_user:5002,IM,U,/incoming-mail/202301/1001", self.log())
        self.assertIsNone(self.get("incoming-mail/202306/1002").assigned_user)
        self.assertFalse(os.path.exists(self.path("2_e_mail_ass_user.pkl")))

    def test_part_t(self):
        self._import_mails(outgoing=False)
        mail = self.get("incoming-mail/202301/1001")
        # On Plone 4, transmogrify.dexterity's schemaupdater stopped the run on the first file: it set the missing
        # default of IIconifiedCategorization.default_titles, a read-only property (collective.iconifiedcategory 0.76)
        storage = self.run_pipeline("t")
        self.assertEqual(storage["course"]["t__1_dmsfile_creation"], 3)  # 6003 is an old version
        self.assertEqual(storage["course"]["t__count"], 2)
        self.assertEqual(self.log(errors=True), "(E) e_file: _eid 6004, Cannot open filename 'PDF/perdu.pdf'\n")
        self.assertEqual(mail.objectIds(), ["6001", "6002"])
        main, annex = mail["6001"], mail["6002"]
        self.assertEqual((main.portal_type, annex.portal_type), ("dmsmainfile", "dmsappendixfile"))
        self.assertEqual((main.title, main.file.filename), ("scan facture.pdf", "scan facture.pdf"))
        self.assertEqual((annex.title, annex.file.filename), ("annexe.pdf", "annexe.pdf"))  # extension added
        self.assertEqual(main.file.data, read_bytes(self.path("files/PDF/6001.pdf")))
        self.assertEqual((main.creation_date, main.modification_date), (DateTime(2023, 1, 5, 10, 0),) * 2)
        self.assertEqual(main.content_category, "plone-annexes_types_-_incoming_dms_files_-_incoming-dms-file")
        self.assertEqual(
            annex.content_category, "plone-annexes_types_-_incoming_appendix_files_-_incoming-appendix-file"
        )
        self.assertEqual(main.creators, ("csvimport",))
        self.assertIn("Reading '{}'".format(self.path("eCourriersFichiers.csv")), self.log())

    def test_part_p(self):
        self._prepare_mappings()
        self.run_pipeline("d")
        storage = self.run_pipeline("p")
        self.assertEqual(storage["parts"], "abcep")
        self.assertEqual(storage["course"]["p__1_S_only"], 6)
        self.assertDictEqual(storage["count"]["p__count"], {"e_mail_o": {"c": 3}})
        self.assertEqual(self.log(errors=True), "")
        self.assertFalse(os.path.exists(self.path("2_e_mail_o.pkl")))
        # outgoing mail sent by a matched user, mapped service, nature, source and created category
        mail = self.get("outgoing-mail/202302/2001")
        self.assertEqual((mail.portal_type, mail.title), ("dmsoutgoingmail", "Réponse à la facture"))
        self.assertEqual((mail.internal_number, mail.internal_reference_no), ("2001", "S0010"))
        self.assertEqual(mail.outgoing_date, datetime(2023, 1, 11))
        self.assertEqual(mail.mail_date, date(2023, 1, 10))
        self.assertEqual(mail.treating_groups, self.dg.UID())
        self.assertEqual(mail.sender, self.pf["agent"][self.dg.UID()].UID())  # held position created by part d
        self.assertIsNone(mail.recipients)
        self.assertEqual((mail.mail_type, mail.send_modes), ("courrier-ordinaire", ["post"]))
        self.assertEqual(mail.classification_categories, [self.category("1.84").UID()])
        self.assertEqual(mail.data_transfer, "NATURE: Courrier ordinaire.\r\nSOURCE: Poste.")
        self.assertEqual(api.content.get_state(mail), "sent")
        self.assertEqual(self.last_wf_status(mail), ("mark_as_sent", "sent", "csvimport", DateTime(2023, 1, 11)))
        # sender without plone user, dates from the outgoing date
        mail = self.get("outgoing-mail/202307/2002")
        self.assertEqual(mail.creation_date, DateTime(2023, 2, 12))
        self.assertEqual(mail.mail_date, date(2023, 2, 12))
        self.assertEqual(mail.sender, self.pf["john-doe"][self.secr.UID()].UID())
        self.assertEqual(mail.treating_groups, self.secr.UID())
        self.assertEqual((mail.mail_type, mail.data_transfer), ("courrier", None))
        # no sender
        mail = self.get("outgoing-mail/202311/2003")
        self.assertEqual(mail.sender, self.pf["reprise-donnees"][self.reprise.UID()].UID())
        self.assertEqual(mail.treating_groups, self.reprise.UID())

    def test_part_q(self):
        self._import_mails(incoming=False)
        storage = self.run_pipeline("q")
        self.assertEqual(storage["parts"], "q")  # q__need is not in the pipeline
        self.assertEqual(storage["course"]["q__1_already_only"], 6)
        self.assertEqual(storage["course"]["q__1_recipients"], 2)
        mail = self.get("outgoing-mail/202302/2001")
        self.assertEqual([rel.to_object for rel in mail.recipients], [self.portal.contacts.electrabel])
        self.assertEqual(mail.description, "")
        mail = self.get("outgoing-mail/202307/2002")
        self.assertEqual([rel.to_object for rel in mail.recipients], [self.portal.contacts.notencoded])
        self.assertEqual(mail.description, "DESTINATAIRE LIBRE: Madame Durant")
        self.assertEqual(mail.data_transfer, "DESTINATAIRE LIBRE: Madame Durant, Rue Haute 5")
        self.assertIn("1:e_mail_recipient:5005,OM,U,/outgoing-mail/202302/2001", self.log())

    def test_part_r(self):
        self._import_mails()
        storage = self.run_pipeline("r")
        self.assertEqual(storage["parts"], "ar")
        self.assertEqual(storage["course"]["r__1_recipient_groups_update"], 2)
        self.assertEqual(storage["course"]["r__read_label_for_recipient_group"], 1)  # only for the incoming mail
        self.assertEqual(self.get("incoming-mail/202301/1001").recipient_groups, [self.dg.UID()])
        self.assertEqual(self.get("outgoing-mail/202302/2001").recipient_groups, [self.secr.UID()])
        self.assertIsNone(self.get("outgoing-mail/202307/2002").recipient_groups)

    def test_part_s(self):
        self._import_mails()
        # folders not yet created by part f: their title is put in the description
        storage = self.run_pipeline("s")
        self.assertEqual(storage["parts"], "es")
        self.assertEqual(storage["course"]["s__1_classification_folders_update"], 4)
        mail = self.get("incoming-mail/202301/1001")
        # Plone 4 behaviour (bug?): s__1_folder_store stores '_title' but the folder csv column is 'title'
        self.assertEqual(mail.description, "EXPÉDITEUR LIBRE: Jean Dupont\r\nREMARQUES: À payer.\r\nDOSSIER: None")
        self.assertIsNone(mail.classification_folders)
        errors = self.log(errors=True)
        self.assertIn("e_mail_folder: _eid 1001, Cannot find folder_id in plone '700' => put in in description", errors)
        self.assertIn("e_mail_folder: _eid 1002, Cannot find folder_id '702' in existing folders", errors)
        # the empty mail category is filled with the folder one
        mail = self.get("incoming-mail/202309/1003")
        self.assertEqual(mail.classification_categories, [self.category("-1.753").UID()])
        # with the folders created
        self.run_pipeline("f")
        self.run_pipeline("s")
        folder = self.portal.folders["700"]
        self.assertEqual(self.get("incoming-mail/202301/1001").classification_folders, [folder.UID()])
        self.assertEqual(self.get("incoming-mail/202309/1003").classification_folders, [folder.UID()])
        self.assertEqual(self.get("outgoing-mail/202302/2001").classification_folders, [folder["701"].UID()])

    def test_part_u(self):
        self._import_mails(incoming=False)
        mail = self.get("outgoing-mail/202302/2001")
        # On Plone 4, as in part t, the schemaupdater stopped the run on the first file
        storage = self.run_pipeline("u")
        self.assertEqual(storage["course"]["u__count"], 1)
        self.assertEqual(self.log(errors=True), "")
        self.assertEqual(mail.objectIds(), ["6005"])
        omain = mail["6005"]
        self.assertEqual(omain.portal_type, "dmsommainfile")
        self.assertEqual((omain.title, omain.file.filename), ("réponse.pdf", "réponse.pdf"))
        self.assertEqual((omain.creation_date, omain.modification_date), (DateTime(2023, 1, 11, 10, 0),) * 2)
        self.assertEqual(omain.content_category, "plone-annexes_types_-_outgoing_dms_files_-_outgoing-scanned-dms-file")
        self.assertEqual((omain.to_sign, omain.signed), (True, True))

    def test_part_x(self):
        self._import_mails()
        storage = self.run_pipeline("x")
        self.assertEqual(storage["parts"], "x")
        self.assertEqual(storage["course"]["x__1_already_only"], 2)  # the link to an unknown mail is dropped
        self.assertEqual(storage["course"]["x__1_reply_to_update"], 1)
        omail = self.get("outgoing-mail/202302/2001")
        self.assertEqual([rel.to_object for rel in omail.reply_to], [self.get("incoming-mail/202301/1001")])
        self.assertIsNone(self.get("outgoing-mail/202307/2002").reply_to)
        self.assertIn("1:reply_to:2001,OM,U,/outgoing-mail/202302/2001", self.log())

    def test_part_y(self):
        self._import_mails()
        self.run_pipeline("x")  # modifies the outgoing mail
        omail = self.get("outgoing-mail/202302/2001")
        self.assertNotEqual(omail.modification_date, omail.creation_date)
        storage = self.run_pipeline("y")
        self.assertEqual(storage["parts"], "y")
        self.assertEqual(storage["course"]["y__mails_load"], 6)
        self.assertEqual(storage["course"]["y__type"], 1)  # only the modified mail is updated
        self.assertEqual(omail.modification_date, omail.creation_date)
        self.assertIn("e_mail:2001,OM,?,/outgoing-mail/202302/2001", self.log())
