# -*- coding: utf-8 -*-
"""Setup tests for this package."""
from collections import OrderedDict
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from imio.transmogrifier.iadocs.utils import add_key_if_value
from imio.transmogrifier.iadocs.utils import course_print
from imio.transmogrifier.iadocs.utils import course_store
from imio.transmogrifier.iadocs.utils import encode_list
from imio.transmogrifier.iadocs.utils import full_name
from imio.transmogrifier.iadocs.utils import get_categories
from imio.transmogrifier.iadocs.utils import get_file_content
from imio.transmogrifier.iadocs.utils import get_folders
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_org_ancestors
from imio.transmogrifier.iadocs.utils import get_org_level
from imio.transmogrifier.iadocs.utils import get_personnel
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import get_users
from imio.transmogrifier.iadocs.utils import get_users_groups
from imio.transmogrifier.iadocs.utils import get_values_string
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from imio.transmogrifier.iadocs.utils import print_item
from imio.transmogrifier.iadocs.utils import short_log
from imio.transmogrifier.iadocs.utils import time_display
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from Products.CMFPlone.utils import safe_unicode
from zope.testing.loggingsupport import InstalledHandler

import io
import os
import re
import shutil
import six
import sys
import tempfile
import unittest


class Section(object):
    """A blueprint section stand-in: the utils only use some of its attributes."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class TestUtils(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.storage = get_storage(self.portal)
        self.logs = InstalledHandler("dti", "dto")
        self.addCleanup(self.logs.uninstall)

    def messages(self):
        return [(rec.name, rec.levelname, rec.getMessage()) for rec in self.logs.records]

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

    def test_add_key_if_value(self):
        dic = {}
        add_key_if_value(dic, "none", None)
        add_key_if_value(dic, "empty", u"")
        add_key_if_value(dic, "zero", 0)
        add_key_if_value(dic, "s_none", None, strict=True)
        add_key_if_value(dic, "s_empty", u"", strict=True)
        add_key_if_value(dic, "s_value", u"x", strict=True)
        self.assertEqual(dic, {"empty": u"", "zero": 0, "s_value": u"x"})

    def test_course_store(self):
        section = Section(name="a__sec", storage={"course": OrderedDict([("init", 1)])})
        course_store(section, None)
        course_store(section, {u"_eid": u"1"})
        self.assertEqual(list(section.storage["course"].items()), [("init", 1), ("a__sec", 2)])

    def test_course_print(self):
        course_print(Section(storage={"course": OrderedDict([("init", 1), ("a__sec", 2)])}))
        self.assertEqual(
            self.messages(), [("dto", "INFO", "COURSE:"), ("dto", "INFO", "> init: 1"), ("dto", "INFO", "> a__sec: 2")]
        )

    def test_encode_list(self):
        res = encode_list([u"Été", b"x", 1, None], "utf8")
        self.assertEqual(res, [b"\xc3\x89t\xc3\xa9", b"x", 1, None])
        self.assertIsInstance(res[0], bytes)
        self.assertEqual(encode_list([u"Été"], "latin1"), [b"\xc9t\xe9"])
        self.assertEqual(encode_list([], "utf8"), [])

    def test_get_file_content(self):
        filesp = safe_unicode(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, filesp)
        for path, content in (
            (u"PDF/12.pdf", b"%PDF 12"),
            (u"DOC/12.odt", b"odt 12"),
            (u"DOC/12.doc", b"doc 12"),
            (u"DOC/13.PDF", b"%PDF 13"),
            (u"x/14.txt", b"txt 14"),
        ):
            os.path.isdir(os.path.join(filesp, os.path.dirname(path))) or os.makedirs(
                os.path.join(filesp, os.path.dirname(path))
            )
            with io.open(os.path.join(filesp, path), "wb") as fh:
                fh.write(content)
        disk_files = {
            u"12": {"f": [(u".odt", u"DOC"), (u".pdf", u"PDF"), (u".doc", u"DOC")]},
            u"13": {"f": [(u".PDF", u"DOC")]},
            u"15": {"f": [(u".pdf", u"PDF")]},
        }
        section = Section(disk_files=disk_files, storage={"filesp": filesp})
        # nothing on item or on disk
        self.assertEqual(get_file_content(section, {u"_eid": u"99"}), (None, None, None))
        # pdf taken first, _fs_path set
        item = {u"_eid": u"12"}
        self.assertEqual(get_file_content(section, item), (u".pdf", u"12.pdf", b"%PDF 12"))
        self.assertEqual(item[u"_fs_path"], u"PDF/12.pdf")
        # _fs_path on item is also considered (here the same path)
        self.assertEqual(get_file_content(section, item), (u".pdf", u"12.pdf", b"%PDF 12"))
        # no pdf: lowest extension, _filename extension adapted or added
        section.disk_files[u"12"]["f"] = [(u".odt", u"DOC"), (u".doc", u"DOC")]
        item = {u"_eid": u"12", u"_filename": u"Lettre.txt"}
        self.assertEqual(get_file_content(section, item), (u".doc", u"Lettre.doc", b"doc 12"))
        self.assertEqual(item[u"_fs_path"], u"DOC/12.doc")
        item = {u"_eid": u"12", u"_filename": u"Lettre"}
        self.assertEqual(get_file_content(section, item), (u".doc", u"Lettre.doc", b"doc 12"))
        # extension is lowered for the result but not for the path
        item = {u"_eid": u"13"}
        self.assertEqual(get_file_content(section, item), (u".pdf", u"13.pdf", b"%PDF 13"))
        self.assertEqual(item[u"_fs_path"], u"DOC/13.PDF")
        # _fs_path without extension completed with _ext
        item = {u"_eid": u"14", u"_fs_path": u"x/14", u"_ext": u"txt"}
        ext, filename, content = get_file_content(section, item)
        self.assertEqual((ext, filename, content), (u".txt", u"14.txt", b"txt 14"))
        self.assertIsInstance(filename, six.text_type)
        self.assertIsInstance(content, bytes)
        # Plone 4 behaviour (bug?): when no path exists on disk, the first path is returned instead of the extension
        self.assertEqual(get_file_content(section, {u"_eid": u"15"}), (u"PDF/15.pdf", None, None))

    def test_get_org_ancestors(self):
        relation = {u"2": {"_parent_id": u"1"}, u"3": {"_parent_id": u"2"}, u"4": {"_parent_id": u"9"}}
        self.assertEqual(get_org_ancestors(relation, u"1"), [u"1"])
        self.assertEqual(get_org_ancestors(relation, u"3"), [u"1", u"2", u"3"])
        self.assertEqual(get_org_ancestors(relation, u"4"), [u"9", u"4"])

    def test_get_org_level(self):
        relation = {u"2": {"_parent_id": u"1"}, u"3": {"_parent_id": u"2"}}
        self.assertEqual([get_org_level(relation, oid) for oid in (u"1", u"2", u"3", u"9")], [1, 2, 3, 1])

    def test_get_values_string(self):
        item = {u"a": u"1", u"b": u"Été"}
        self.assertEqual(get_values_string(item, [u"a", u"c", u"b"]), u"1::Été")
        self.assertEqual(get_values_string(item, [u"b", u"a"], sep=u"-"), u"Été-1")
        self.assertEqual(get_values_string(item, []), u"")

    def test_full_name(self):
        self.assertEqual(full_name(u"Jean", u"Dupont"), u"Jean Dupont")
        self.assertEqual(full_name(u"Jean", u"Dupont", fn_first=False), u"Dupont Jean")
        self.assertEqual(full_name(u"", u"Dupont"), u"Dupont")
        self.assertEqual(full_name(None, u"Dupont", fn_first=False), u"Dupont")

    def test_log_error(self):
        item = {u"_bpk": u"e_mail", u"_eid": u"1", u"_id": u"élément"}
        log_error(item, u"bad value")
        self.assertTrue(item[u"_error"])
        log_error(item, u"no id", level="critical", fld=u"_id")
        log_error({u"_eid": 2}, u"no bpk")
        self.assertEqual(
            self.messages(),
            [
                ("dti", "ERROR", u"e_mail: _eid 1, bad value"),
                ("dti", "CRITICAL", u"e_mail: _id élément, no id"),
                ("dti", "ERROR", u": _eid 2, no bpk"),
            ],
        )
        self.assertRaises(KeyError, log_error, {}, u"no eid")

    def test_print_item(self):
        orig_stdout = sys.stdout
        sys.stdout = out = six.StringIO()
        self.addCleanup(setattr, sys, "stdout", orig_stdout)
        print_item({u"b": u"Été", u"a": 1, u"file": u"content"})
        self.assertEqual(out.getvalue(), u"a: 1\nb: Été\n")
        out.truncate(0)
        out.seek(0)
        print_item({u"b": u"Été", u"a": 1, u"file": u"content"}, remove=[u"a"], with_separator=u"=")
        self.assertEqual(out.getvalue(), u"b: Été\nfile: content\n{}\n".format(u"=" * 30))

    def test_short_log(self):
        item = {u"_bpk": u"e_mail", u"_eid": u"1", u"_type": u"dmsoutgoingmail", u"_act": u"U", u"_path": u"/om/1"}
        self.assertEqual(short_log(item), u"e_mail:1,OM,U,/om/1")
        self.assertEqual(short_log(item, count=5), u"5:e_mail:1,OM,U,/om/1")
        self.assertEqual(short_log({u"_bpk": u"e", u"title": u"Été"}, count=0), u"e:,,?,Été")
        self.assertRaises(KeyError, short_log, {})

    def test_time_display(self):
        time_display(Section(name="a__sec", storage={"o_logger": o_logger}), {u"_eid": u"7"})
        time_display(Section(name="a__sec", storage={"o_logger": o_logger}), {})
        self.assertEqual(len(self.messages()), 2)
        self.assertTrue(re.match(r"^\d\d:\d\d:\d\d\.\d{6} : a__sec, 7$", self.messages()[0][2]), self.messages())
        self.assertTrue(self.messages()[1][2].endswith(" : a__sec, "), self.messages())


class TestUtilsDms(unittest.TestCase):
    """Utils reading an ia.docs site."""

    layer = IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.pgo = self.portal.contacts["plonegroup-organization"]
        self.logs = InstalledHandler("dti", "dto")
        self.addCleanup(self.logs.uninstall)

    def messages(self):
        return [(rec.name, rec.levelname, rec.getMessage()) for rec in self.logs.records]

    def test_get_categories(self):
        cats = get_categories(self.portal)
        self.assertEqual(
            sorted(cats),
            [
                u"-1",
                u"-1.7",
                u"-1.75",
                u"-1.753",
                u"-1.754",
                u"-1.754.2",
                u"-1.754.21",
                u"-1.758",
                u"-1.758.1",
                u"-1.758.2",
                u"-1.758.3",
                u"-1.758.5",
            ],
        )
        cat = cats[u"-1.754.2"]
        self.assertEqual(sorted(cat), ["enabled", "obj", "title", "uid"])
        self.assertEqual(cat["title"], u"Usage de la voie publique. (*)")
        self.assertTrue(cat["enabled"])
        self.assertEqual(cat["obj"].identifier, u"-1.754.2")
        self.assertEqual(cat["uid"], cat["obj"].UID())
        self.assertEqual(self.messages(), [])

    def test_get_folders(self):
        csvp = safe_unicode(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, csvp)
        section = Section(portal=self.portal, storage={"csvp": csvp})
        parent = self.portal.folders["usage-de-la-voie-publique-stationnement-et-amarrage"]
        folder = parent["cas-particuliers-demandes-en-autorisation-2013-2021"]
        uids, irn_to_folder, titles = get_folders(section)
        self.assertEqual(len(uids), 22)
        full_title = u"Usage de la voie publique : Stationnement et amarrage \u23e9 Cas particuliers : Demandes en " \
                     u"autorisation \u2013 2021"
        self.assertEqual(
            uids[folder.UID()],
            {
                "title": u"Cas particuliers : Demandes en autorisation \u2013 2021",
                "path": "/plone/folders/usage-de-la-voie-publique-stationnement-et-amarrage/"
                        "cas-particuliers-demandes-en-autorisation-2013-2021",
                "full_title": full_title,
                "parent": parent.UID(),
                "irn": u"F0007-02",
                "peid": u"F0007-02",
                "tgft": u"Direction générale - Secrétariat",
            },
        )
        self.assertIsNone(uids[parent.UID()]["parent"])
        self.assertEqual(uids[parent.UID()]["full_title"], u"Usage de la voie publique : Stationnement et amarrage")
        self.assertEqual(
            sorted(irn_to_folder),
            sorted(
                [u"F00{:02d}".format(i) for i in (1, 3, 5, 7, 9, 11)]
                + [u"F00{:02d}-0{}".format(i, j) for i in (1, 3, 5, 11) for j in (1, 2, 3)]
                + [u"F00{:02d}-0{}".format(i, j) for i in (7, 9) for j in (1, 2)]
            ),
        )
        self.assertEqual(irn_to_folder[u"F0007-02"], {"uid": folder.UID()})
        self.assertEqual(irn_to_folder[u"F0007"], {"uid": parent.UID()})
        self.assertEqual(len(titles), 22)
        self.assertEqual(titles[full_title], {"uids": [folder.UID()]})
        self.assertEqual(self.messages(), [])
        # irn with a slash: its first part is the parent eid, already found irn is logged
        folder.internal_reference_no = u"F0001/9"
        uids, irn_to_folder, titles = get_folders(section)
        self.assertEqual(uids[folder.UID()]["irn"], u"F0001/9")
        self.assertEqual(uids[folder.UID()]["peid"], u"F0001")
        self.assertNotIn(u"F0007-02", irn_to_folder)
        self.assertNotEqual(irn_to_folder[u"F0001"], {"uid": folder.UID()})
        self.assertEqual(len(self.messages()), 1)
        self.assertEqual(self.messages()[0][:2], ("dto", "ERROR"))
        self.assertTrue(
            self.messages()[0][2].startswith(u"Already found folder irn 'F0001' (/plone/folders/usage-de-la-voie"),
            self.messages(),
        )
        # dumped dicts are loaded when the 3 files exist
        for i, filename in enumerate(
            (u"2_folder_folders_uids.dic", u"2_folder_irn_to_folder.dic", u"2_folder_folders_titles.dic")
        ):
            if i < 2:
                with io.open(os.path.join(csvp, filename), "w", encoding="utf8") as fh:
                    fh.write(u"{'k%d': %d}" % (i, i))
                self.assertEqual(len(get_folders(section)[0]), 22)
        with io.open(os.path.join(csvp, u"2_folder_folders_titles.dic"), "w", encoding="utf8") as fh:
            fh.write(u"{'k2': 2}")
        self.assertEqual(get_folders(section), ({"k0": 0}, {"k1": 1}, {"k2": 2}))

    def test_get_mailtypes(self):
        email = {"active": True, "dtitle": u"Email"}
        post = {"active": True, "dtitle": u"Courrier postal"}
        reg = {"active": True, "dtitle": u"Courrier recommandé"}
        reg_ar = {"active": True, "dtitle": u"Courrier recommandé avec AR"}
        self.assertEqual(
            get_mailtypes(self.portal),
            {
                "te": {
                    u"certificat": {"active": True, "dtitle": u"Certificat médical"},
                    u"courrier": {"active": True, "dtitle": u"Courrier"},
                    u"facture": {"active": True, "dtitle": u"Facture"},
                    u"retour-ar": {"active": True, "dtitle": u"Retour AR"},
                    u"retour-recommande": {"active": True, "dtitle": u"Retour recommandé"},
                },
                "ts": {u"courrier": {"active": True, "dtitle": u"Courrier"}},
                "fee": {u"email": email, u"post": post, u"post_registered": reg, u"post_registered_dr": reg_ar},
                "fes": {u"email": email, u"post": post, u"post_registered": reg, u"post_registered_dr": reg_ar},
            },
        )

    def test_get_personnel(self):
        pf = self.portal.contacts["personnel-folder"]
        userids = ("agent", "agent1", "bourgmestre", "chef", "dirg", "encodeur", "lecteur")
        p_userid, e_userid, hps = get_personnel(self.portal)
        self.assertEqual(p_userid, dict((uid, pf[uid].UID()) for uid in userids))
        self.assertEqual(e_userid, {})
        self.assertEqual(sorted(hps), sorted(pf[uid].UID() for uid in userids))
        agent1 = pf["agent1"]
        evenements = self.pgo["evenements"]
        self.assertEqual(
            hps[agent1.UID()],
            {
                "path": "/contacts/personnel-folder/agent1",
                "state": "active",
                "hps": {
                    evenements.UID(): {
                        "path": "/contacts/personnel-folder/agent1/agent-evenements",
                        "state": "active",
                        "puid": agent1["agent-evenements"].UID(),
                    }
                },
            },
        )
        self.assertEqual(len(hps[pf["chef"].UID()]["hps"]), 12)
        dg = self.pgo["direction-generale"]
        self.assertEqual(
            hps[pf["dirg"].UID()]["hps"],
            {
                dg.UID(): {
                    "path": "/contacts/personnel-folder/dirg/directeur-general",
                    "state": "deactivated",
                    "puid": pf["dirg"]["directeur-general"].UID(),
                }
            },
        )
        # external user ids, other eid field
        agent1.internal_number = u"E1,E2"
        self.assertEqual(get_personnel(self.portal)[1], {u"E1": agent1.UID(), u"E2": agent1.UID()})
        self.assertEqual(get_personnel(self.portal, eid_fld="firstname")[1][u"Stef"], agent1.UID())

    def test_get_plonegroup_orgs(self):
        # Plone 4 behaviour: organizations need the internal number behavior (added by the init section)
        self.assertRaises(AttributeError, get_plonegroup_orgs, self.portal)
        fti = self.portal.portal_types.organization
        fti._updateProperty(
            "behaviors", tuple(fti.behaviors) + ("collective.behavior.internalnumber.behavior.IInternalNumberBehavior",)
        )
        secretariat = self.pgo["direction-generale"]["secretariat"]
        secretariat.internal_number = u"S1,S2"
        api.content.transition(obj=self.pgo["conseil-communal"], transition="deactivate")
        generic = api.content.create(
            container=self.pgo, type="organization", id="reprise-de-donnees", title=u"Reprise de données"
        )
        all_orgs, eid_to_orgs = get_plonegroup_orgs(self.portal)
        self.assertEqual(len(all_orgs), 23)
        self.assertNotIn(self.pgo["conseil-communal"].UID(), all_orgs)
        self.assertEqual(
            all_orgs[secretariat.UID()],
            {
                "p": "/contacts/plonegroup-organization/direction-generale/secretariat",
                "t": u"Secrétariat",
                "ft": u"Direction générale - Secrétariat",
                "eids": [u"S1", u"S2"],
                "st": "active",
                "sl": True,
            },
        )
        self.assertEqual(
            all_orgs[generic.UID()],
            {
                "p": "/contacts/plonegroup-organization/reprise-de-donnees",
                "t": u"Reprise de données",
                "ft": u"Reprise de données",
                "eids": [],
                "st": "active",
                "sl": False,
            },
        )
        self.assertEqual(
            eid_to_orgs,
            {u"S1": {"uid": secretariat.UID()}, u"S2": {"uid": secretariat.UID()}, None: {"uid": generic.UID()}},
        )

    def test_get_users(self):
        self.assertEqual(
            get_users(self.portal),
            {
                "agent": {"fullname": u"Fred Agent"},
                "agent1": {"fullname": u"Stef Agent"},
                "bourgmestre": {"fullname": u"Paul BM"},
                "chef": {"fullname": u"Michel Chef"},
                "dirg": {"fullname": u"Maxime DG"},
                "encodeur": {"fullname": u"Jean Encodeur"},
                "lecteur": {"fullname": u"Jef Lecteur"},
                "scanner": {"fullname": u"Scanner"},
                "siteadmin": {"fullname": u"siteadmin"},
                "test_user_1_": {"fullname": u"test_user_1_"},
            },
        )

    def test_get_users_groups(self):
        ev_uid = self.pgo["evenements"].UID()
        self.assertEqual(
            get_users_groups(self.portal, {"agent1": {}, "dirg": {}, "test_user_1_": {}}),
            {
                "agent1": {"editeur": {ev_uid: {}}, "encodeur": {ev_uid: {}}},
                "dirg": {
                    "": {"audit": {}, "createurs": {}, "dir": {}, "lecteurs": {}},
                    "lecteur": {self.pgo["direction-generale"].UID(): {}},
                },
                "test_user_1_": {},
            },
        )
        self.assertEqual(get_users_groups(self.portal, {}), {})
