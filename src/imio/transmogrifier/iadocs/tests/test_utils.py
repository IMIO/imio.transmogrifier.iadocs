"""Setup tests for this package."""

from collections import OrderedDict
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from imio.transmogrifier.iadocs.utils import add_key_if_value
from imio.transmogrifier.iadocs.utils import course_print
from imio.transmogrifier.iadocs.utils import course_store
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
from plone.base.utils import safe_text
from zope.testing.loggingsupport import InstalledHandler

import io
import os
import re
import shutil
import sys
import tempfile
import unittest


class Section:
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
        add_key_if_value(dic, "empty", "")
        add_key_if_value(dic, "zero", 0)
        add_key_if_value(dic, "s_none", None, strict=True)
        add_key_if_value(dic, "s_empty", "", strict=True)
        add_key_if_value(dic, "s_value", "x", strict=True)
        self.assertEqual(dic, {"empty": "", "zero": 0, "s_value": "x"})

    def test_course_store(self):
        section = Section(name="a__sec", storage={"course": OrderedDict([("init", 1)])})
        course_store(section, None)
        course_store(section, {"_eid": "1"})
        self.assertEqual(list(section.storage["course"].items()), [("init", 1), ("a__sec", 2)])

    def test_course_print(self):
        course_print(Section(storage={"course": OrderedDict([("init", 1), ("a__sec", 2)])}))
        self.assertEqual(
            self.messages(), [("dto", "INFO", "COURSE:"), ("dto", "INFO", "> init: 1"), ("dto", "INFO", "> a__sec: 2")]
        )

    def test_get_file_content(self):
        filesp = safe_text(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, filesp)
        for path, content in (
            ("PDF/12.pdf", b"%PDF 12"),
            ("DOC/12.odt", b"odt 12"),
            ("DOC/12.doc", b"doc 12"),
            ("DOC/13.PDF", b"%PDF 13"),
            ("x/14.txt", b"txt 14"),
        ):
            os.path.isdir(os.path.join(filesp, os.path.dirname(path))) or os.makedirs(
                os.path.join(filesp, os.path.dirname(path))
            )
            with open(os.path.join(filesp, path), "wb") as fh:
                fh.write(content)
        disk_files = {
            "12": {"f": [(".odt", "DOC"), (".pdf", "PDF"), (".doc", "DOC")]},
            "13": {"f": [(".PDF", "DOC")]},
            "15": {"f": [(".pdf", "PDF")]},
        }
        section = Section(disk_files=disk_files, storage={"filesp": filesp})
        # nothing on item or on disk
        self.assertEqual(get_file_content(section, {"_eid": "99"}), (None, None, None))
        # pdf taken first, _fs_path set
        item = {"_eid": "12"}
        self.assertEqual(get_file_content(section, item), (".pdf", "12.pdf", b"%PDF 12"))
        self.assertEqual(item["_fs_path"], "PDF/12.pdf")
        # _fs_path on item is also considered (here the same path)
        self.assertEqual(get_file_content(section, item), (".pdf", "12.pdf", b"%PDF 12"))
        # no pdf: lowest extension, _filename extension adapted or added
        section.disk_files["12"]["f"] = [(".odt", "DOC"), (".doc", "DOC")]
        item = {"_eid": "12", "_filename": "Lettre.txt"}
        self.assertEqual(get_file_content(section, item), (".doc", "Lettre.doc", b"doc 12"))
        self.assertEqual(item["_fs_path"], "DOC/12.doc")
        item = {"_eid": "12", "_filename": "Lettre"}
        self.assertEqual(get_file_content(section, item), (".doc", "Lettre.doc", b"doc 12"))
        # extension is lowered for the result but not for the path
        item = {"_eid": "13"}
        self.assertEqual(get_file_content(section, item), (".pdf", "13.pdf", b"%PDF 13"))
        self.assertEqual(item["_fs_path"], "DOC/13.PDF")
        # _fs_path without extension completed with _ext
        item = {"_eid": "14", "_fs_path": "x/14", "_ext": "txt"}
        ext, filename, content = get_file_content(section, item)
        self.assertEqual((ext, filename, content), (".txt", "14.txt", b"txt 14"))
        self.assertIsInstance(filename, str)
        self.assertIsInstance(content, bytes)
        # Plone 4 behaviour (bug?): when no path exists on disk, the first path is returned instead of the extension
        self.assertEqual(get_file_content(section, {"_eid": "15"}), ("PDF/15.pdf", None, None))

    def test_get_org_ancestors(self):
        relation = {"2": {"_parent_id": "1"}, "3": {"_parent_id": "2"}, "4": {"_parent_id": "9"}}
        self.assertEqual(get_org_ancestors(relation, "1"), ["1"])
        self.assertEqual(get_org_ancestors(relation, "3"), ["1", "2", "3"])
        self.assertEqual(get_org_ancestors(relation, "4"), ["9", "4"])

    def test_get_org_level(self):
        relation = {"2": {"_parent_id": "1"}, "3": {"_parent_id": "2"}}
        self.assertEqual([get_org_level(relation, oid) for oid in ("1", "2", "3", "9")], [1, 2, 3, 1])

    def test_get_values_string(self):
        item = {"a": "1", "b": "Été"}
        self.assertEqual(get_values_string(item, ["a", "c", "b"]), "1::Été")
        self.assertEqual(get_values_string(item, ["b", "a"], sep="-"), "Été-1")
        self.assertEqual(get_values_string(item, []), "")

    def test_full_name(self):
        self.assertEqual(full_name("Jean", "Dupont"), "Jean Dupont")
        self.assertEqual(full_name("Jean", "Dupont", fn_first=False), "Dupont Jean")
        self.assertEqual(full_name("", "Dupont"), "Dupont")
        self.assertEqual(full_name(None, "Dupont", fn_first=False), "Dupont")

    def test_log_error(self):
        item = {"_bpk": "e_mail", "_eid": "1", "_id": "élément"}
        log_error(item, "bad value")
        self.assertTrue(item["_error"])
        log_error(item, "no id", level="critical", fld="_id")
        log_error({"_eid": 2}, "no bpk")
        self.assertEqual(
            self.messages(),
            [
                ("dti", "ERROR", "e_mail: _eid 1, bad value"),
                ("dti", "CRITICAL", "e_mail: _id élément, no id"),
                ("dti", "ERROR", ": _eid 2, no bpk"),
            ],
        )
        self.assertRaises(KeyError, log_error, {}, "no eid")

    def test_print_item(self):
        orig_stdout = sys.stdout
        sys.stdout = out = io.StringIO()
        self.addCleanup(setattr, sys, "stdout", orig_stdout)
        print_item({"b": "Été", "a": 1, "file": "content"})
        self.assertEqual(out.getvalue(), "a: 1\nb: Été\n")
        out.truncate(0)
        out.seek(0)
        print_item({"b": "Été", "a": 1, "file": "content"}, remove=["a"], with_separator="=")
        self.assertEqual(out.getvalue(), "b: Été\nfile: content\n{}\n".format("=" * 30))

    def test_short_log(self):
        item = {"_bpk": "e_mail", "_eid": "1", "_type": "dmsoutgoingmail", "_act": "U", "_path": "/om/1"}
        self.assertEqual(short_log(item), "e_mail:1,OM,U,/om/1")
        self.assertEqual(short_log(item, count=5), "5:e_mail:1,OM,U,/om/1")
        self.assertEqual(short_log({"_bpk": "e", "title": "Été"}, count=0), "e:,,?,Été")
        self.assertRaises(KeyError, short_log, {})

    def test_time_display(self):
        time_display(Section(name="a__sec", storage={"o_logger": o_logger}), {"_eid": "7"})
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
                "-1",
                "-1.7",
                "-1.75",
                "-1.753",
                "-1.754",
                "-1.754.2",
                "-1.754.21",
                "-1.758",
                "-1.758.1",
                "-1.758.2",
                "-1.758.3",
                "-1.758.5",
            ],
        )
        cat = cats["-1.754.2"]
        self.assertEqual(sorted(cat), ["enabled", "obj", "title", "uid"])
        self.assertEqual(cat["title"], "Usage de la voie publique. (*)")
        self.assertTrue(cat["enabled"])
        self.assertEqual(cat["obj"].identifier, "-1.754.2")
        self.assertEqual(cat["uid"], cat["obj"].UID())
        self.assertEqual(self.messages(), [])

    def test_get_folders(self):
        csvp = safe_text(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, csvp)
        section = Section(portal=self.portal, storage={"csvp": csvp})
        parent = self.portal.folders["usage-de-la-voie-publique-stationnement-et-amarrage"]
        # the id comes from the title: "...-2013-2021" on Plone 4 (U+2013 normalized as its code), "...-2021" on 6
        folder = [obj for obj in parent.objectValues() if obj.title.endswith("\u2013 2021")][0]
        uids, irn_to_folder, titles = get_folders(section)
        self.assertEqual(len(uids), 22)
        full_title = (
            "Usage de la voie publique : Stationnement et amarrage \u23e9 Cas particuliers : Demandes en "
            "autorisation \u2013 2021"
        )
        self.assertEqual(
            uids[folder.UID()],
            {
                "title": "Cas particuliers : Demandes en autorisation \u2013 2021",
                "path": "/".join(folder.getPhysicalPath()),
                "full_title": full_title,
                "parent": parent.UID(),
                "irn": "F0007-02",
                "peid": "F0007-02",
                "tgft": "Direction générale - Secrétariat",
            },
        )
        self.assertIsNone(uids[parent.UID()]["parent"])
        self.assertEqual(uids[parent.UID()]["full_title"], "Usage de la voie publique : Stationnement et amarrage")
        self.assertEqual(
            sorted(irn_to_folder),
            sorted(
                [f"F00{i:02d}" for i in (1, 3, 5, 7, 9, 11)]
                + [f"F00{i:02d}-0{j}" for i in (1, 3, 5, 11) for j in (1, 2, 3)]
                + [f"F00{i:02d}-0{j}" for i in (7, 9) for j in (1, 2)]
            ),
        )
        self.assertEqual(irn_to_folder["F0007-02"], {"uid": folder.UID()})
        self.assertEqual(irn_to_folder["F0007"], {"uid": parent.UID()})
        self.assertEqual(len(titles), 22)
        self.assertEqual(titles[full_title], {"uids": [folder.UID()]})
        self.assertEqual(self.messages(), [])
        # irn with a slash: its first part is the parent eid, already found irn is logged
        folder.internal_reference_no = "F0001/9"
        uids, irn_to_folder, titles = get_folders(section)
        self.assertEqual(uids[folder.UID()]["irn"], "F0001/9")
        self.assertEqual(uids[folder.UID()]["peid"], "F0001")
        self.assertNotIn("F0007-02", irn_to_folder)
        self.assertNotEqual(irn_to_folder["F0001"], {"uid": folder.UID()})
        self.assertEqual(len(self.messages()), 1)
        self.assertEqual(self.messages()[0][:2], ("dto", "ERROR"))
        self.assertTrue(
            self.messages()[0][2].startswith("Already found folder irn 'F0001' (/plone/folders/usage-de-la-voie"),
            self.messages(),
        )
        # dumped dicts are loaded when the 3 files exist
        for i, filename in enumerate(
            ("2_folder_folders_uids.dic", "2_folder_irn_to_folder.dic", "2_folder_folders_titles.dic")
        ):
            if i < 2:
                with open(os.path.join(csvp, filename), "w", encoding="utf8") as fh:
                    fh.write("{'k%d': %d}" % (i, i))
                self.assertEqual(len(get_folders(section)[0]), 22)
        with open(os.path.join(csvp, "2_folder_folders_titles.dic"), "w", encoding="utf8") as fh:
            fh.write("{'k2': 2}")
        self.assertEqual(get_folders(section), ({"k0": 0}, {"k1": 1}, {"k2": 2}))

    def test_get_mailtypes(self):
        email = {"active": True, "dtitle": "Email"}
        post = {"active": True, "dtitle": "Courrier postal"}
        reg = {"active": True, "dtitle": "Courrier recommandé"}
        reg_ar = {"active": True, "dtitle": "Courrier recommandé avec AR"}
        self.assertEqual(
            get_mailtypes(self.portal),
            {
                "te": {
                    "certificat": {"active": True, "dtitle": "Certificat médical"},
                    "courrier": {"active": True, "dtitle": "Courrier"},
                    "facture": {"active": True, "dtitle": "Facture"},
                    "retour-ar": {"active": True, "dtitle": "Retour AR"},
                    "retour-recommande": {"active": True, "dtitle": "Retour recommandé"},
                },
                "ts": {"courrier": {"active": True, "dtitle": "Courrier"}},
                "fee": {"email": email, "post": post, "post_registered": reg, "post_registered_dr": reg_ar},
                "fes": {"email": email, "post": post, "post_registered": reg, "post_registered_dr": reg_ar},
            },
        )

    def test_get_personnel(self):
        pf = self.portal.contacts["personnel-folder"]
        userids = ("agent", "agent1", "bourgmestre", "chef", "dirg", "encodeur", "lecteur")
        p_userid, e_userid, hps = get_personnel(self.portal)
        self.assertEqual(p_userid, {uid: pf[uid].UID() for uid in userids})
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
        agent1.internal_number = "E1,E2"
        self.assertEqual(get_personnel(self.portal)[1], {"E1": agent1.UID(), "E2": agent1.UID()})
        self.assertEqual(get_personnel(self.portal, eid_fld="firstname")[1]["Stef"], agent1.UID())

    def test_get_plonegroup_orgs(self):
        # Plone 4 behaviour: organizations need the internal number behavior (added by the init section)
        self.assertRaises(AttributeError, get_plonegroup_orgs, self.portal)
        fti = self.portal.portal_types.organization
        fti._updateProperty(
            "behaviors", tuple(fti.behaviors) + ("collective.behavior.internalnumber.behavior.IInternalNumberBehavior",)
        )
        secretariat = self.pgo["direction-generale"]["secretariat"]
        secretariat.internal_number = "S1,S2"
        api.content.transition(obj=self.pgo["conseil-communal"], transition="deactivate")
        generic = api.content.create(
            container=self.pgo, type="organization", id="reprise-de-donnees", title="Reprise de données"
        )
        all_orgs, eid_to_orgs = get_plonegroup_orgs(self.portal)
        self.assertEqual(len(all_orgs), 23)
        self.assertNotIn(self.pgo["conseil-communal"].UID(), all_orgs)
        self.assertEqual(
            all_orgs[secretariat.UID()],
            {
                "p": "/contacts/plonegroup-organization/direction-generale/secretariat",
                "t": "Secrétariat",
                "ft": "Direction générale - Secrétariat",
                "eids": ["S1", "S2"],
                "st": "active",
                "sl": True,
            },
        )
        self.assertEqual(
            all_orgs[generic.UID()],
            {
                "p": "/contacts/plonegroup-organization/reprise-de-donnees",
                "t": "Reprise de données",
                "ft": "Reprise de données",
                "eids": [],
                "st": "active",
                "sl": False,
            },
        )
        self.assertEqual(
            eid_to_orgs,
            {"S1": {"uid": secretariat.UID()}, "S2": {"uid": secretariat.UID()}, None: {"uid": generic.UID()}},
        )

    def test_get_users(self):
        self.assertEqual(
            get_users(self.portal),
            {
                "agent": {"fullname": "Fred Agent"},
                "agent1": {"fullname": "Stef Agent"},
                "bourgmestre": {"fullname": "Paul BM"},
                "chef": {"fullname": "Michel Chef"},
                "dirg": {"fullname": "Maxime DG"},
                "encodeur": {"fullname": "Jean Encodeur"},
                "lecteur": {"fullname": "Jef Lecteur"},
                "scanner": {"fullname": "Scanner"},
                "siteadmin": {"fullname": "siteadmin"},
                "test_user_1_": {"fullname": "test_user_1_"},
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
