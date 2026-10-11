# -*- coding: utf-8 -*-
"""Blueprints tests for this package."""
from Acquisition import aq_base
from collective.contact.plonegroup.config import get_registry_organizations
from collective.documentviewer.settings import GlobalSettings
from datetime import date
from datetime import datetime
from DateTime import DateTime
from imio.helpers.transmogrifier import relative_path
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.blueprints import main
from imio.transmogrifier.iadocs.blueprints.main import AddDataInItem
from imio.transmogrifier.iadocs.blueprints.main import CommonInputChecks
from imio.transmogrifier.iadocs.blueprints.main import DependencySorter
from imio.transmogrifier.iadocs.blueprints.main import FilesList
from imio.transmogrifier.iadocs.blueprints.main import Initialization
from imio.transmogrifier.iadocs.blueprints.main import InsertPath
from imio.transmogrifier.iadocs.blueprints.main import LastSection
from imio.transmogrifier.iadocs.blueprints.main import PickleData
from imio.transmogrifier.iadocs.blueprints.main import ReadFromData
from imio.transmogrifier.iadocs.blueprints.main import SetIconifiedAttributes
from imio.transmogrifier.iadocs.blueprints.main import SetOwner
from imio.transmogrifier.iadocs.blueprints.main import SetState
from imio.transmogrifier.iadocs.blueprints.main import StoreInData
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import get_transmogrifier
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from imio.transmogrifier.iadocs.utils import get_folders
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_personnel
from imio.transmogrifier.iadocs.utils import get_users
from imio.transmogrifier.iadocs.utils import get_users_groups
from plone import api
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from Products.CMFPlone.utils import safe_unicode
from Products.cron4plone.browser.configlets.cron_configuration import ICronConfiguration
from six.moves import cPickle
from zope.annotation import IAnnotations
from zope.component import queryUtility
from zope.testing.loggingsupport import InstalledHandler

import io
import json
import os
import re
import shutil
import six
import tempfile
import unittest


INB = "collective.behavior.internalnumber.behavior.IInternalNumberBehavior"
DTB = "imio.dms.mail.content.behaviors.IDmsMailDataTransfer"


class TestBluePrintMain(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.storage = get_storage(self.portal)
        self.logs = InstalledHandler("dti", "dto")
        self.addCleanup(self.logs.uninstall)

    def get_transmogrifier(self, parts="a", data=None, **storage):
        transmogrifier = get_transmogrifier(self.portal)
        transmogrifier.storage.update(dict(storage, parts=parts, data=data if data is not None else {}))
        return transmogrifier

    def messages(self):
        return [(rec.name, rec.levelname, rec.getMessage()) for rec in self.logs.records]

    def tempdir(self):
        path = safe_unicode(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, path)
        return path

    def test_common_input_checks(self):
        self.storage.update({"parts": "a", "csv": {"cip": {"fd": [u"1", u"2"]}}})
        # without options
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip"}, None)
        bp.previous = [{u"1": u"xx", u"2": u"yy"}]
        self.assertDictEqual(next(iter(bp)), bp.previous[0])
        # check strip_chars
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "strip_chars": "1 $. s"}, None)
        bp.previous = [{u"1": u"aa.$", u"2": u"bb"}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa", u"2": u"bb"})
        # check clean_value
        bp = CommonInputChecks(
            self.portal,
            "a__cip",
            {"bp_key": "cip", "clean_value": '1 python:"\\n" " " "python:[(r\'^["" ]+$\', u\'\')]" python:"\\r\\n"'},
            None,
        )
        self.assertListEqual(bp.cleans, [(u"1", u"\n", u" ", [('^[" ]+$', u"")], u"\r\n")])
        bp.previous = [{u"1": u'aa\n" "\nbb', u"2": None}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa\r\nbb", u"2": None})
        # check replace_newline
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "replace_newline": '1 " - " 2 " - "'}, None)
        bp.previous = [{u"1": u"aa\n", u"2": u"aa\nbb"}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa", u"2": u"aa - bb"})
        # check invalids
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "invalids": '1 0 2 ",|0, 0|0, 23"'}, None)
        bp.previous = [{u"1": u"aa", u"2": u"0, 23"}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa", u"2": None})
        bp.previous = [{u"1": u"0", u"2": u""}]
        self.assertDictEqual(next(iter(bp)), {u"1": None, u"2": u""})
        # check concatenate
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "concatenate": "1 3 4 ,"}, None)
        bp.previous = [{u"1": u"aa", u"2": u"bb"}]
        # nothing done because 3 field doesn't exist
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa", u"2": u"bb"})
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "concatenate": "1 2 res ,"}, None)
        bp.previous = [{u"1": u"aa", u"2": u"bb"}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa", u"2": u"bb", u"res": u"aa,bb"})  # in new field
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "concatenate": "1 2 2 ,"}, None)
        bp.previous = [{u"1": u"aa", u"2": u"bb"}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa", u"2": u"aa,bb"})  # in existing field
        # check split_text
        bp = CommonInputChecks(
            self.portal,
            "a__cip",
            {"bp_key": "cip", "split_text": "1 11 2 0 python:'\\n' python:'\\r\\n' \"Suite: \""},
            None,
        )
        self.assertListEqual(bp.splits, [(u"1", 11, u"2", 0, u"\n", u"\r\n", u"Suite: ")])
        bp.previous = [{u"1": u"aa bb cc dd ee", u"2": None}]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa bb cc dd", u"2": u"Suite:  ee"})
        bp.previous = [{u"1": u"aa bb cc \ndd", u"2": u"Première\r\nDeuxième"}]
        bp.splits = [(u"1", 6, u"2", 50, u"\n", u"\r\n", u"Suite: ")]
        self.assertDictEqual(next(iter(bp)), {u"1": u"aa bb ", u"2": u"Première\r\nDeuxième\r\nSuite: cc \r\ndd"})
        # check booleans
        self.storage["csv"]["cip"]["fd"] = [u"1", u"2", u"3", u"4", u"5", u"6", u"7"]
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "booleans": "1 2 3 4 5 6"}, None)
        bp.previous = [
            {
                u"_bpk": "cip",
                u"_eid": u"0",
                u"1": u"",
                u"2": u"true",
                u"3": u"False",
                u"4": u"1",
                u"5": "0",
                u"6": u"aa",
                u"7": u"aa",
            }
        ]
        self.assertDictEqual(
            next(iter(bp)),
            {
                u"_bpk": "cip",
                u"_eid": u"0",
                u"1": False,
                u"2": True,
                u"3": False,
                u"4": True,
                u"5": False,
                u"6": False,
                u"7": u"aa",
                u"_error": True,
            },
        )
        # check dates
        self.storage["csv"]["cip"]["fd"] = [u"1", u"2", u"3"]
        bp = CommonInputChecks(
            self.portal, "a__cip", {"bp_key": "cip", "dates": '1 "%Y%m%d %H:%M" 0 2 %Y%m%d 1 3 "%Y%m%d" 1'}, None
        )
        bp.previous = [{u"_bpk": "cip", u"_eid": u"0", u"1": u"19840212 12:01", u"2": u"20010212", u"3": u"2023 04 16"}]
        self.assertDictEqual(
            next(iter(bp)),
            {
                u"_bpk": "cip",
                u"_eid": u"0",
                u"1": datetime(1984, 2, 12, 12, 1),
                u"2": date(2001, 2, 12),
                u"3": None,
                u"_error": True,
            },
        )
        # check combination
        self.storage["csv"]["cip"]["fd"] = [u"1", u"2", u"3"]
        bp = CommonInputChecks(
            self.portal,
            "a__cip",
            {"bp_key": "cip", "strip_chars": "1 ! l", "replace_newline": "1 ;", "invalids": "1 néant", "booleans": "1"},
            None,
        )
        bp.previous = [{u"_bpk": "cip", u"_eid": u"0", u"1": u"!néant\n"}]
        self.assertDictEqual(next(iter(bp)), {u"_bpk": "cip", u"_eid": u"0", u"1": False})
        # strip types, csv_key, fields not in csv fieldnames ignored, evals, condition
        self.storage["csv"]["other"] = {"fd": [u"1", u"2", u"3"]}
        bp = CommonInputChecks(
            self.portal,
            "a__cip",
            {
                "bp_key": "cip",
                "csv_key": "other",
                "strip_chars": "1 x l 2 x r 9 x s",
                "booleans": "9",
                "evals": "3 9",
                "condition": "python:item.get('skip') is None",
            },
            None,
        )
        self.assertListEqual(bp.strips, [(u"1", u"x", u"l"), (u"2", u"x", u"r")])
        self.assertListEqual(bp.booleans, [])
        self.assertListEqual(bp.evals, [u"3"])
        bp.previous = [{u"1": u"xax", u"2": u"xbx", u"3": u"[1, u'é']"}, {u"1": u"xax", u"skip": 1}]
        self.assertListEqual(list(bp), [{u"1": u"ax", u"2": u"xb", u"3": [1, u"é"]}, {u"1": u"xax", u"skip": 1}])
        # dates are not checked against csv fieldnames, dates before 1900 are refused
        bp = CommonInputChecks(self.portal, "a__cip", {"bp_key": "cip", "dates": "1 %Y%m%d 1 2 %Y%m%d 0 9 %Y 1"}, None)
        bp.previous = [{u"_bpk": "cip", u"_eid": u"0", u"1": u"18991231", u"2": u"19000101"}]
        self.assertDictEqual(
            next(iter(bp)),
            {u"_bpk": "cip", u"_eid": u"0", u"1": None, u"2": datetime(1900, 1, 1), u"9": None, u"_error": True},
        )
        # not in part
        bp = CommonInputChecks(self.portal, "b__cip", {"bp_key": "cip", "booleans": "1"}, None)
        bp.previous = [{u"1": u"1"}]
        self.assertListEqual(list(bp), [{u"1": u"1"}])

    def test_add_data_in_item(self):
        data = {
            u"e_user": {u"u1": {u"name": u"Ann", u"mail": u"a@b"}},
            u"e_user_srv": {u"u1": {u"s1": {u"role": u"chef"}}},
        }
        tr = self.get_transmogrifier(data=data)
        # Plone 4 behaviour (bug?): the store key of an unfound item is replaced with an empty value
        options = {"bp_key": "e_mail", "related_storage": "e_user", "store_key": "_uid", "fieldnames": "name"}
        self.assertListEqual(
            list(AddDataInItem(tr, "a__add", options, iter([{u"_uid": u"u1"}, {u"_uid": u"u2"}]))),
            [{u"_uid": u"u1", u"name": u"Ann"}, {u"_uid": u"", u"name": u""}],
        )
        self.assertEqual(dict(tr.storage["course"]), {"a__add": 2})
        # prefix and marker
        options.update({"fieldnames": "name mail", "prefix": "p_", "marker": "found"})
        self.assertListEqual(
            list(AddDataInItem(tr, "a__add", options, iter([{u"_uid": u"u1"}, {u"_uid": u"u2"}]))),
            [
                {u"_uid": u"u1", u"p_name": u"Ann", u"p_mail": u"a@b", u"p__uid": u"u1", "_mar_ker": u"found"},
                {u"_uid": u"u2", u"p_name": u"", u"p_mail": u"", u"p__uid": u"", "_mar_ker": u""},
            ],
        )
        # store keys not added, condition
        options = {
            "bp_key": "e_mail",
            "related_storage": "e_user",
            "store_key": "_uid",
            "fieldnames": "name",
            "add_store_keys": "0",
            "condition": "python:'skip' not in item",
        }
        items = [{u"_uid": u"u2"}, {u"_uid": u"u1", u"skip": 1}]
        self.assertListEqual(
            list(AddDataInItem(tr, "a__add", options, iter(items))),
            [{u"_uid": u"u2", u"name": u""}, {u"_uid": u"u1", u"skip": 1}],
        )
        # store_subkey
        options = {
            "bp_key": "e_mail",
            "related_storage": "e_user_srv",
            "store_key": "_uid",
            "store_subkey": "_sid",
            "fieldnames": "role",
        }
        items = [{u"_uid": u"u1", u"_sid": u"s1"}, {u"_uid": u"u1", u"_sid": u"s9"}]
        self.assertListEqual(
            list(AddDataInItem(tr, "a__add", options, iter(items))),
            [{u"_uid": u"u1", u"_sid": u"s1", u"role": u"chef"}, {u"_uid": u"u1", u"_sid": u"", u"role": u""}],
        )
        # Plone 4 behaviour (bug?): without fieldnames, nothing is added (docstring says all)
        options = {"bp_key": "e_mail", "related_storage": "e_user", "store_key": "_uid", "add_store_keys": "0"}
        self.assertListEqual(list(AddDataInItem(tr, "a__add", options, iter([{u"_uid": u"u1"}]))), [{u"_uid": u"u1"}])
        # unknown related storage
        options = {"bp_key": "e_mail", "related_storage": "e_xx", "store_key": "_uid", "fieldnames": "name"}
        self.assertListEqual(
            list(AddDataInItem(tr, "a__add", options, iter([{u"_uid": u"u1"}]))), [{u"_uid": u"", u"name": u""}]
        )
        # b_condition false or not in part: unchanged
        options["b_condition"] = "python:'e_xx' in storage['data']"
        self.assertListEqual(list(AddDataInItem(tr, "a__add", options, iter([{u"_uid": u"u1"}]))), [{u"_uid": u"u1"}])
        options = {"bp_key": "e_mail", "related_storage": "e_user", "store_key": "_uid", "fieldnames": "name"}
        self.assertListEqual(list(AddDataInItem(tr, "b__add", options, iter([{u"_uid": u"u1"}]))), [{u"_uid": u"u1"}])

    def test_dependency_sorter(self):
        data = {
            u"e_service": {
                u"1": {u"title": u"A"},
                u"2": {u"title": u"B"},
                u"3": {u"title": u"C"},
                u"4": {u"title": u"D"},
                u"5": {u"title": u"skip"},
            },
            u"e_service_rel": {u"2": {"_parent_id": u"1"}, u"3": {"_parent_id": u"2"}, u"4": {"_parent_id": u"9"}},
        }
        tr = self.get_transmogrifier(data=data)
        options = {
            "bp_key": "e_service",
            "store_key": "_eid",
            "parent_relation": "e_service_rel",
            "condition": "python:item[u'title'] != u'skip'",
        }
        # only previous items are yielded, the levels are set in the data dict
        self.assertListEqual(list(DependencySorter(tr, "dep", options, iter([{u"prev": 1}]))), [{u"prev": 1}])
        self.assertDictEqual(
            data[u"e_service"],
            {
                u"1": {u"title": u"A", "_level": 1},
                u"2": {u"title": u"B", "_level": 2},
                u"3": {u"title": u"C", "_level": 3},
                u"4": {u"title": u"D", "_level": 2},
                u"5": {u"title": u"skip"},
            },
        )
        self.assertEqual(
            self.messages(), [("dti", "ERROR", u"e_service: _eid 4, Parent '9' not found: please correct csv")]
        )
        self.assertEqual(dict(tr.storage["course"]), {"dep": 5})
        # unknown parent relation
        options["parent_relation"] = "e_xx"
        self.assertListEqual(list(DependencySorter(tr, "dep2", options, iter([{u"prev": 1}]))), [{u"prev": 1}])
        self.assertNotIn("dep2", tr.storage["course"])

    def test_files_list(self):
        filesp = self.tempdir()
        for path in (u"a/1.pdf", u"a/1.odt", u"b/1.pdf", u"b/2.PDF", u"x/3.pdf"):
            if not os.path.exists(os.path.join(filesp, os.path.dirname(path))):
                os.makedirs(os.path.join(filesp, os.path.dirname(path)))
            io.open(os.path.join(filesp, path), "wb").close()
        tr = self.get_transmogrifier(parts="0", filesp=filesp)
        options = {"bp_key": "e_files", "exclude_patterns": "python:['^x']"}
        self.assertListEqual(list(FilesList(tr, "0__files", options, iter([{u"prev": 1}]))), [{u"prev": 1}])
        expected = {u"1": {"f": [(u".odt", u"a"), (u".pdf", u"a"), (u".pdf", u"b")]}, u"2": {"f": [(u".PDF", u"b")]}}
        self.assertDictEqual(tr.storage["data"][u"e_files"], expected)
        self.assertIsInstance(list(tr.storage["data"][u"e_files"])[0], six.text_type)
        self.assertIsInstance(tr.storage["data"][u"e_files"][u"1"]["f"][0][0], six.text_type)
        self.assertEqual(dict(tr.storage["course"]), {"0__files": 1})
        self.assertEqual(self.messages(), [("dto", "INFO", "Stored 2 files info")])
        # already stored files are not duplicated, no exclude pattern
        FilesList(tr, "0__files", {"bp_key": "e_files"}, iter([]))
        expected[u"3"] = {"f": [(u".pdf", u"x")]}
        self.assertDictEqual(tr.storage["data"][u"e_files"], expected)
        # condition (evaluated once with storage)
        FilesList(tr, "0__files", {"bp_key": "e_cond", "condition": "python:'xx' in storage"}, iter([]))
        self.assertDictEqual(tr.storage["data"][u"e_cond"], {})
        # not in part
        FilesList(tr, "a__files", {"bp_key": "e_part"}, iter([]))
        self.assertNotIn(u"e_part", tr.storage["data"])

    def test_pickle_data(self):
        csvp = self.tempdir()
        path = os.path.join(csvp, u"2_e_mail.pkl")
        # file doesn't exist: storage initialized, dump registered with default False condition
        tr = self.get_transmogrifier(csvp=csvp, lastsection={"pkl_dump": []})
        options = {"filename": "2_e_mail.pkl", "store_key": "e_mail"}
        self.assertListEqual(list(PickleData(tr, "a__pkl", options, iter([{u"prev": 1}]))), [{u"prev": 1}])
        self.assertDictEqual(tr.storage["data"], {u"e_mail": {}})
        self.assertEqual(len(tr.storage["lastsection"]["pkl_dump"]), 1)
        filename, store_key, condition = tr.storage["lastsection"]["pkl_dump"][0]
        self.assertEqual((filename, store_key), (path, u"e_mail"))
        self.assertFalse(condition(None, storage=tr.storage, filename=filename))
        # existing file is loaded (replacing storage), dump condition
        with io.open(path, "wb") as fh:
            cPickle.dump({u"1": {u"title": u"Été"}}, fh, 2)
        tr = self.get_transmogrifier(csvp=csvp, lastsection={"pkl_dump": []}, data={u"e_mail": {u"0": {}}})
        options["d_condition"] = "python:filename.endswith('.pkl') and 'data' in storage"
        PickleData(tr, "a__pkl", options, iter([]))
        self.assertDictEqual(tr.storage["data"][u"e_mail"], {u"1": {u"title": u"Été"}})
        self.assertIsInstance(tr.storage["data"][u"e_mail"][u"1"][u"title"], six.text_type)
        filename, store_key, condition = tr.storage["lastsection"]["pkl_dump"][0]
        self.assertTrue(condition(None, storage=tr.storage, filename=filename))
        self.assertIn(("dto", "INFO", u"Loading '{}'".format(path)), self.messages())
        # update storage, absolute filename
        tr = self.get_transmogrifier(csvp=csvp, lastsection={"pkl_dump": []}, data={u"e_mail": {u"0": {}}})
        PickleData(tr, "a__pkl", {"filename": path, "store_key": "e_mail", "update": "1"}, iter([]))
        self.assertDictEqual(tr.storage["data"][u"e_mail"], {u"0": {}, u"1": {u"title": u"Été"}})
        # empty filename or not in part: nothing done
        tr = self.get_transmogrifier(csvp=csvp, lastsection={"pkl_dump": []})
        PickleData(tr, "a__pkl", {"filename": "", "store_key": "e_mail"}, iter([]))
        PickleData(tr, "b__pkl", options, iter([]))
        self.assertDictEqual(tr.storage["data"], {})
        self.assertListEqual(tr.storage["lastsection"]["pkl_dump"], [])

    def test_read_from_data(self):
        data = {
            u"e_mail": {u"2": {u"title": u"B", u"order": 1, u"x": 1}, u"1": {u"title": u"A", u"order": 2, u"x": 2}},
            u"e_sub": {u"1": {u"b": {u"t": u"B"}, u"a": {u"t": u"A"}}, u"0": {u"c": {u"t": u"C"}}},
        }
        tr = self.get_transmogrifier(data=data)
        # previous items first, sorted on key, filtered fields
        options = {"bp_key": "e_read", "related_storage": "e_mail", "store_key": "_eid", "fieldnames": "title missing"}
        self.assertListEqual(
            list(ReadFromData(tr, "a__read", options, iter([{u"prev": 1}]))),
            [
                {u"prev": 1},
                {"_bpk": u"e_read", u"_eid": u"1", u"title": u"A", u"missing": None},
                {"_bpk": u"e_read", u"_eid": u"2", u"title": u"B", u"missing": None},
            ],
        )
        self.assertEqual(dict(tr.storage["course"]), {"a__read": 2})
        self.assertEqual(self.messages(), [("dto", "INFO", u"Reading data from 'e_mail'")])
        # bp_key as related storage, sort_value, all fields copied, condition
        options = {"bp_key": "e_mail", "store_key": "_eid", "sort_value": "python:data[k]['order']"}
        items = list(ReadFromData(tr, "a__read2", options, iter([])))
        self.assertListEqual(
            items,
            [
                {"_bpk": u"e_mail", u"_eid": u"2", u"title": u"B", u"order": 1, u"x": 1},
                {"_bpk": u"e_mail", u"_eid": u"1", u"title": u"A", u"order": 2, u"x": 2},
            ],
        )
        items[0][u"title"] = u"changed"
        self.assertEqual(data[u"e_mail"][u"2"][u"title"], u"B")
        options["condition"] = "python:item['x'] == 2"
        self.assertListEqual([it[u"_eid"] for it in ReadFromData(tr, "a__read3", options, iter([]))], [u"1"])
        self.assertEqual(tr.storage["course"]["a__read3"], 2)
        # store_subkey, condition with storage
        options = {
            "bp_key": "e_s",
            "related_storage": "e_sub",
            "store_key": "_eid",
            "store_subkey": "_sid",
            "condition": "python:'data' in storage and item['_sid'] != u'b'",
        }
        self.assertListEqual(
            list(ReadFromData(tr, "a__read", options, iter([]))),
            [
                {"_bpk": u"e_s", u"_eid": u"0", u"_sid": u"c", u"t": u"C"},
                {"_bpk": u"e_s", u"_eid": u"1", u"_sid": u"a", u"t": u"A"},
            ],
        )
        # Plone 4 behaviour (bug?): without store_subkey, storage is not given to the condition
        options = {"bp_key": "e_mail", "store_key": "_eid", "condition": "python:'data' in storage"}
        self.assertRaises(NameError, list, ReadFromData(tr, "a__read", options, iter([])))
        # unknown related storage is created
        options = {"bp_key": "e_new", "store_key": "_eid"}
        self.assertListEqual(list(ReadFromData(tr, "a__read", options, iter([{u"prev": 1}]))), [{u"prev": 1}])
        self.assertDictEqual(data[u"e_new"], {})
        # b_condition false or not in part: only previous items
        options = {"bp_key": "e_mail", "store_key": "_eid", "b_condition": "python:'e_xx' in storage['data']"}
        self.assertListEqual(list(ReadFromData(tr, "a__read", options, iter([{u"prev": 1}]))), [{u"prev": 1}])
        options = {"bp_key": "e_mail", "store_key": "_eid"}
        self.assertListEqual(list(ReadFromData(tr, "b__read", options, iter([{u"prev": 1}]))), [{u"prev": 1}])

    def test_store_in_data(self):
        tr = self.get_transmogrifier()
        # stored with fieldnames, not yielded by default
        options = {"bp_key": "e_mail", "store_key": "_eid", "fieldnames": "title"}
        items = [{u"_bpk": u"e_mail", u"_eid": u"1", u"title": u"A", u"x": 1}, {u"_eid": u"2", u"title": u"B"}]
        self.assertListEqual(list(StoreInData(tr, "a__store", options, iter(items))), [])
        self.assertDictEqual(tr.storage["data"], {u"e_mail": {u"1": {u"title": u"A"}, u"2": {u"title": u"B"}}})
        self.assertEqual(dict(tr.storage["course"]), {"a__store": 2})
        # already stored key: error logged, values updated
        items = [{u"_bpk": u"e_mail", u"_eid": u"1", u"title": u"A2"}]
        self.assertListEqual(list(StoreInData(tr, "a__store", options, iter(items))), [])
        self.assertDictEqual(tr.storage["data"][u"e_mail"][u"1"], {u"title": u"A2"})
        self.assertEqual(self.messages(), [("dti", "ERROR", u"e_mail: _eid 1, Key '1' already in 'e_mail' data dict")])
        # no uniqueness check, all fields (copied), yield, condition with storage
        options = {
            "bp_key": "e_mail",
            "store_key": "_eid",
            "check_key_uniqueness": "0",
            "yield": "1",
            "condition": "python:'data' in storage and 'skip' not in item",
        }
        items = [{u"_eid": u"2", u"title": u"B2", u"y": [1]}, {u"_eid": u"3", u"skip": 1}]
        self.assertListEqual(list(StoreInData(tr, "a__store", options, iter(items))), items)
        self.assertDictEqual(tr.storage["data"][u"e_mail"][u"2"], {u"_eid": u"2", u"title": u"B2", u"y": [1]})
        self.assertIsNot(tr.storage["data"][u"e_mail"][u"2"][u"y"], items[0][u"y"])
        self.assertNotIn(u"3", tr.storage["data"][u"e_mail"])
        self.assertEqual(len(self.messages()), 1)
        # store_subkey: the error flag set on the item is also stored
        options = {"bp_key": "e_srv", "store_key": "_eid", "store_subkey": "_sid"}
        items = [{u"_bpk": u"e_srv", u"_eid": u"1", u"_sid": u"a"}, {u"_bpk": u"e_srv", u"_eid": u"1", u"_sid": u"a"}]
        list(StoreInData(tr, "a__store", options, iter(items)))
        self.assertDictEqual(
            tr.storage["data"][u"e_srv"],
            {u"1": {u"a": {u"_bpk": u"e_srv", u"_eid": u"1", u"_sid": u"a", u"_error": True}}},
        )
        self.assertEqual(
            self.messages()[1], ("dti", "ERROR", u"e_srv: _eid 1, Subkey 'a' of key '1' already in 'e_srv' data dict")
        )
        options["check_subkey_uniqueness"] = "0"
        list(StoreInData(tr, "a__store", options, iter([{u"_eid": u"1", u"_sid": u"a"}])))
        self.assertEqual(len(self.messages()), 2)
        # not in part
        options = {"bp_key": "e_part", "store_key": "_eid"}
        self.assertListEqual(list(StoreInData(tr, "b__store", options, iter([{u"_eid": u"1"}]))), [{u"_eid": u"1"}])
        self.assertNotIn(u"e_part", tr.storage["data"])


class TestBluePrintMainDms(unittest.TestCase):
    """Blueprints acting on an ia.docs site."""

    layer = IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        setRoles(self.portal, TEST_USER_ID, ["Manager"])
        self.logs = InstalledHandler("dti", "dto")
        self.addCleanup(self.logs.uninstall)

    def get_transmogrifier(self, parts="a", data=None, **storage):
        transmogrifier = get_transmogrifier(self.portal)
        transmogrifier.storage.update(dict(storage, parts=parts, data=data if data is not None else {}))
        return transmogrifier

    def messages(self):
        return [(rec.name, rec.levelname, rec.getMessage()) for rec in self.logs.records]

    def tempdir(self):
        path = safe_unicode(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, path)
        return path

    def path(self, obj):
        return relative_path(self.portal, "/".join(obj.getPhysicalPath()))

    def patch_archive_site(self):
        self.addCleanup(setattr, main, "ARCHIVE_SITE", main.ARCHIVE_SITE)
        main.ARCHIVE_SITE = True

    def restore_handlers(self, logger, handlers):
        for handler in logger.handlers:
            if handler not in handlers:
                handler.close()
        logger.handlers = handlers

    def test_initialization(self):
        for logger in (e_logger, o_logger):  # file handlers are added by the section
            self.addCleanup(self.restore_handlers, logger, list(logger.handlers))
        wp = self.tempdir()
        pr_tool = self.portal.portal_repository
        orig_vct = tuple(pr_tool._versionable_content_types)
        orig_vpm = dict(pr_tool._version_policy_mapping)
        # no im and only one om: mail counters will be reset
        brains = api.content.find(portal_type=["dmsincomingmail", "dmsincoming_email"])
        api.content.delete(objects=[b.getObject() for b in brains], check_linkintegrity=False)
        brains = api.content.find(portal_type="dmsoutgoingmail")
        api.content.delete(objects=[b.getObject() for b in brains[1:]], check_linkintegrity=False)
        options = {"parts": "afs", "commit": False, "commit_nb": 3, "batch_nb": 4}
        self.portal.REQUEST.set("_transmo_options_", json.dumps(options))
        config = {
            "internal_number_behavior_types": "organization person",
            "data_transfer_behavior_types": "dmsincomingmail dmsoutgoingmail",
            "creator": "csvimport",
            "default_contact": 'contacts/notencoded "Non encodé"',
            "generic_service": 'contacts/plonegroup-organization/reprise-de-donnees "Reprise de données"',
            "default_creation_date": "20090922-1200",
            "default_category": '"-1.9" "Catégorie par défaut"',
        }
        tr = get_transmogrifier(self.portal, config=config, pipeline=u"init\nf__to_correct_only", storage={})
        bp = Initialization(tr, "init", {"basepath": wp}, iter([{u"prev": 1}]))
        self.assertListEqual(list(bp), [{u"prev": 1}])
        storage = tr.storage
        self.assertIs(storage, IAnnotations(tr)[ANNOTATION_KEY])
        self.assertIsNotNone(self.portal.acl_users.getUserById("csvimport"))
        self.assertListEqual(sorted(os.listdir(wp)), [u"plone_dt.log", u"plone_dt_input_errors.log"])
        self.assertDictEqual(
            dict((k, v) for (k, v) in storage.items() if k not in ("data", "plone", "o_logger")),
            {
                "wp": wp,
                "csvp": wp,
                "filesp": wp,
                "creation_date": datetime(2009, 9, 22, 12, 0),
                "csv": {},
                "desc_only": False,
                "course": {"init": 1},
                "parts": u"afs",
                "commit": False,
                "commit_nb": 3,
                "batch_nb": 4,
                "lastsection": {"pkl_dump": []},
            },
        )
        self.assertIsInstance(storage["wp"], six.text_type)
        self.assertIs(storage["o_logger"], o_logger)
        # plone objects: generic service created and selected, default category created
        contacts = self.portal.contacts
        generic = contacts["plonegroup-organization"]["reprise-de-donnees"]
        self.assertEqual(generic.title, u"Reprise de données")
        self.assertIn(generic.UID(), get_registry_organizations())
        data = storage["data"]
        category = data["p_category"][u"-1.9"]["obj"]
        self.assertEqual((category.title, category.enabled), (u"Catégorie par défaut", False))
        self.assertEqual(
            data["p_category"][u"-1.9"],
            {"title": u"Catégorie par défaut", "enabled": False, "uid": category.UID(), "obj": category},
        )
        plone = storage["plone"]
        self.assertListEqual(
            sorted(plone), ["def_category", "def_contact", "directory", "directory_path", "firstname_first"]
        )
        self.assertIs(aq_base(plone["directory"]), aq_base(contacts))
        self.assertIs(aq_base(plone["def_contact"]), aq_base(contacts["notencoded"]))
        self.assertEqual(
            (plone["directory_path"], plone["firstname_first"], plone["def_category"]),
            ("/contacts", True, category.UID()),
        )
        # data
        self.assertListEqual(
            sorted(data),
            [
                "p_categories",
                "p_category",
                "p_dir_org_levels",
                "p_dir_org_levels_len",
                "p_dir_org_types",
                "p_dir_org_types_len",
                "p_eid_to_orgs",
                "p_euid_to_pers",
                "p_folder_full_title",
                "p_folder_uid",
                "p_hps",
                "p_irn_to_folder",
                "p_mail_ids",
                "p_mailtype",
                "p_orgs_all",
                "p_user",
                "p_user_service",
                "p_userid_to_pers",
            ],
        )
        self.assertListEqual(
            list(data["p_dir_org_types"].items()),
            [
                (u"non-defini", {"name": u"Non défini"}),
                (u"sa", {"name": u"SA"}),
                (u"commune", {"name": u"Commune"}),
                (u"cpas", {"name": u"CPAS"}),
                (u"intercommunale", {"name": u"Intercommunale"}),
                (u"zp", {"name": u"Zone de police"}),
                (u"zs", {"name": u"Zone de secours"}),
            ],
        )
        self.assertListEqual(
            list(data["p_dir_org_levels"].items()),
            [
                (u"non-defini", {"name": u"Non défini"}),
                (u"department", {"name": u"Département"}),
                (u"service", {"name": u"Service"}),
            ],
        )
        self.assertEqual((data["p_dir_org_types_len"], data["p_dir_org_levels_len"]), (7, 3))
        self.assertDictEqual(
            data["p_orgs_all"][generic.UID()],
            {
                "p": "/contacts/plonegroup-organization/reprise-de-donnees",
                "t": u"Reprise de données",
                "ft": u"Reprise de données",
                "eids": [],
                "st": "active",
                "sl": True,
            },
        )
        self.assertEqual(len(data["p_orgs_all"]), 24)
        self.assertDictEqual(data["p_eid_to_orgs"], {None: {"uid": generic.UID()}})
        self.assertDictEqual(data["p_mailtype"], get_mailtypes(self.portal))
        self.assertDictEqual(data["p_user"], get_users(self.portal))
        self.assertIn("csvimport", data["p_user"])
        self.assertTupleEqual(
            (data["p_userid_to_pers"], data["p_euid_to_pers"], data["p_hps"]), get_personnel(self.portal)
        )
        self.assertEqual(len(data["p_category"]), 13)
        self.assertEqual(len(data["p_categories"]), 12)  # disabled default category not in vocabulary
        self.assertTupleEqual(
            (data["p_folder_uid"], data["p_irn_to_folder"], data["p_folder_full_title"]), get_folders(bp)
        )
        self.assertDictEqual(data["p_mail_ids"], {})
        self.assertDictEqual(data["p_user_service"], get_users_groups(self.portal, data["p_user"]))
        # site configuration
        self.assertTrue(self.portal.portal_quickinstaller.isProductInstalled("collective.behavior.internalnumber"))
        self.assertListEqual(
            api.portal.get_registry_record(
                "collective.behavior.internalnumber.browser.settings.IInternalNumberConfig.portal_type_config"
            ),
            [
                {"portal_type": u"organization", "uniqueness": True, "default_expression": None, "default_number": 1},
                {"portal_type": u"person", "uniqueness": True, "default_expression": None, "default_number": 1},
            ],
        )
        for typ, behavior in (
            ("organization", INB),
            ("person", INB),
            ("dmsincomingmail", DTB),
            ("dmsoutgoingmail", DTB),
        ):
            self.assertIn(behavior, self.portal.portal_types[typ].behaviors)
        for field in ("imail_fields", "omail_fields"):
            self.assertDictEqual(
                api.portal.get_registry_record("imio.dms.mail.browser.settings.IImioDmsMailConfig." + field)[-1],
                {
                    "field_name": u"IDmsMailDataTransfer.data_transfer",
                    "read_tal_condition": None,
                    "write_tal_condition": None,
                },
            )
        reg = "collective.dms.mailcontent.browser.settings.IDmsMailConfig.{}"
        self.assertEqual(api.portal.get_registry_record(reg.format("incomingmail_number")), 1)
        self.assertEqual(api.portal.get_registry_record(reg.format("outgoingmail_number")), 2)
        self.assertTrue(GlobalSettings(self.portal).auto_convert)
        # versioning deactivated, original values stored
        self.assertEqual((pr_tool._versionable_content_types, pr_tool._version_policy_mapping), ([], {}))
        annot = IAnnotations(self.portal)
        self.assertEqual((annot["transmo.pr_vct"], annot["transmo.pr_vpm"]), (orig_vct, orig_vpm))
        # logs
        self.assertListEqual(
            self.messages(),
            [
                ("dto", "INFO", "Installed collective.behavior.internalnumber"),
                ("dto", "INFO", "Added internalnumber config for type organization"),
                ("dto", "INFO", "Added internalnumber behavior on type organization"),
                ("dto", "INFO", "Added internalnumber config for type person"),
                ("dto", "INFO", "Added internalnumber behavior on type person"),
                ("dto", "INFO", "Added datatransfer behavior on type dmsincomingmail"),
                ("dto", "INFO", "Added data_transfer field in type dmsincomingmail"),
                ("dto", "INFO", "Added datatransfer behavior on type dmsoutgoingmail"),
                ("dto", "INFO", "Added data_transfer field in type dmsoutgoingmail"),
                ("dto", "INFO", "Created object 'generic_service'"),
                ("dto", "INFO", "Created default category -1.9"),
            ],
        )
        with io.open(os.path.join(wp, u"plone_dt.log"), encoding="utf8") as fh:
            self.assertIn(u"(I) Created object 'generic_service'\n", fh.read())

        # second run: already configured site, commit logs, other paths, t and 9 parts, archive site
        self.logs.clear()
        self.patch_archive_site()
        os.mkdir(os.path.join(wp, u"sub"))
        options = {"parts": "t9", "commit": True, "commit_nb": 0, "batch_nb": 0}
        self.portal.REQUEST.set("_transmo_options_", json.dumps(options))
        config = {
            "creator": "csvimport",
            "default_contact": 'contacts/notencoded "Non encodé"',
            "generic_service": 'contacts/plonegroup-organization/reprise-de-donnees "Reprise de données"',
            "default_creation_date": "20200101-0000",
            "default_category": '"-1.7" "Other title"',
            "profils_donnee_E": " P1  P2 ",
            "profils_donnee_S": "P3",
            "profils_dossier": "P4",
            "fields": '"P1" "eF1" "pf1" "x"\n"P3" "eF3" "pf3" "y"',
        }
        tr = get_transmogrifier(self.portal, config=config, pipeline=u"init", storage={})
        options = {"basepath": wp, "subpath": "sub", "csvpath": "dt_csv", "filespath": wp}
        Initialization(tr, "init", options, iter([]))
        storage = tr.storage
        self.assertTupleEqual(
            (storage["wp"], storage["csvp"], storage["filesp"]), (os.path.join(wp, u"sub"), u"dt_csv", wp)
        )
        self.assertListEqual(
            sorted(os.listdir(os.path.join(wp, u"sub"))),
            [
                u"plone_dt.log",
                u"plone_dt_commit.log",
                u"plone_dt_input_errors.log",
                u"plone_dt_input_errors_commit.log",
            ],
        )
        with io.open(os.path.join(wp, u"sub", u"plone_dt_commit.log"), encoding="utf8") as fh:
            self.assertTrue(re.match(r"^\(I\) STARTING 't9' parts at \d{8}-\d{4}\n$", fh.read()))
        self.assertEqual((storage["desc_only"], storage["creation_date"]), (True, datetime(2020, 1, 1, 0, 0)))
        self.assertEqual(storage["plone"]["def_category"], storage["data"]["p_category"][u"-1.7"]["uid"])
        self.assertDictEqual(
            storage["data"]["e_9_fields"],
            {
                "P1": {"typ": "E", u"eF1": u"pf1"},
                "P2": {"typ": "E"},
                "P3": {"typ": "S", u"eF3": u"pf3"},
                "P4": {"typ": "F"},
            },
        )
        for key in ("p_categories", "p_folder_uid", "p_irn_to_folder", "p_folder_full_title"):
            self.assertNotIn(key, storage["data"])
        self.assertFalse(GlobalSettings(self.portal).auto_convert)
        self.assertTrue(self.portal.portal_css.getResource("imiodmsmail_archives.css").getEnabled())
        self.assertListEqual(queryUtility(ICronConfiguration, "cron4plone_config").cronjobs, [])
        self.assertEqual(IAnnotations(self.portal)["transmo.pr_vct"], orig_vct)
        self.assertListEqual(
            self.messages(), [("dto", "INFO", u"STARTING 't9' parts at {}".format(self.messages()[0][2][-13:]))]
        )

    def test_insert_path(self):
        tr = self.get_transmogrifier(parts="i", csv={u"e_contact": {"fd": [u"_lastname", u"_eid", u"title"]}})
        options = {"bp_key": "e_contact", "id_keys": "_lastname _firstname"}
        bp = InsertPath(tr, "i__path", dict(options, csv_key="e_xx"), None)
        self.assertListEqual(bp.id_keys, [u"_lastname", u"_firstname"])  # all id keys without csv fieldnames
        items = [
            # Plone 4 behaviour (bug?): get_correct_path gets an unicode path that OFS traverses as a sequence of
            # characters: an existing object is not detected (and an absolute path would be checked from zope root)
            {u"_eid": u"1", u"title": u"Électrabel", u"_parenth": u"/contacts"},
            {u"_eid": u"2", u"title": u"Electrabel", u"_parenth": u"contacts"},
            # with a native string path, an existing object is detected
            {u"_eid": u"10", u"title": u"Electrabel", u"_parenth": "contacts", u"_id": "electrabel"},
            # id from id keys (only those in csv fieldnames), given ids
            {u"_eid": u"3", u"_lastname": u"Dupont", u"_parenth": u"/contacts", u"_id": u"Été 2"},
            {u"_eid": u"4", u"_lastname": u"Dupont", u"_parenth": u"/contacts", u"_id": u"dup4"},
            {u"_eid": u"7", u"_lastname": u"Dupont", u"_firstname": u"Jean", u"_parenth": u"/contacts"},
            # eid already handled
            {u"_eid": u"1", u"title": u"Other", u"_parenth": u"/x"},
            # path already set
            {u"_eid": u"5", u"_path": u"/contacts/swde"},
            {u"_eid": u"5", u"_path": u"/contacts/other", u"_act": u"N"},
            {u"_eid": u"6", u"_path": u"/contacts/new", u"_act": u"N"},
        ]
        expected = [dict(item) for item in items]  # items are modified in place
        for exp, path, act in zip(
            expected,
            [
                u"/contacts/electrabel",
                u"contacts/electrabel",
                "contacts/electrabel-1",
                u"/contacts/ete-2",
                u"/contacts/dup4",
                u"/contacts/dupont",
                u"/contacts/electrabel",
                u"/contacts/swde",
                u"/contacts/other",
                u"/contacts/new",
            ],
            ["N", "N", "N", "N", "N", "N", "U", "N", "U", "N"],
        ):
            exp.update({u"_path": path, u"_act": act})
        expected[8]["_error"] = True
        self.assertListEqual(list(InsertPath(tr, "i__path", options, iter(items))), expected)
        self.assertDictEqual(
            tr.storage["data"][u"e_contact"],
            {
                u"1": {"path": u"/contacts/electrabel"},
                u"2": {"path": u"contacts/electrabel"},
                u"10": {"path": "contacts/electrabel-1"},
                u"3": {"path": u"/contacts/ete-2"},
                u"4": {"path": u"/contacts/dup4"},
                u"5": {"path": u"/contacts/other"},
                u"6": {"path": u"/contacts/new"},
                u"7": {"path": u"/contacts/dupont"},
            },
        )
        self.assertEqual(dict(tr.storage["course"]), {"i__path": 10})
        self.assertListEqual(
            self.messages(),
            [("dti", "ERROR", u": _eid 5, 2 differents path: new '/contacts/other <> existing '/contacts/swde'")],
        )
        # no title: critical error and exception, or item not yielded
        self.logs.clear()
        items = [{u"_bpk": u"e_contact", u"_eid": u"8", u"_parenth": u"/contacts"}]
        bp = InsertPath(tr, "i__path", options, iter(items))
        six.assertRaisesRegex(self, Exception, r"^No title ! See log\.\.\.$", list, bp)
        options["raise_on_error"] = "0"
        self.assertListEqual(list(InsertPath(tr, "i__path", options, iter(items))), [])
        self.assertEqual(len(self.messages()), 2)
        self.assertEqual(self.messages()[1][:2], ("dti", "CRITICAL"))
        six.assertRegex(
            self, self.messages()[1][2], r"^e_contact: _eid 8, cannot get an id from id keys \[u?'_lastname'\]$"
        )
        # condition, not in part
        items = [{u"_eid": u"9", u"title": u"T", u"_parenth": u"/contacts"}]
        bp = InsertPath(tr, "i__path", dict(options, condition="python:False"), iter(items))
        self.assertListEqual(list(bp), [{u"_eid": u"9", u"title": u"T", u"_parenth": u"/contacts"}])
        bp = InsertPath(tr, "a__path", options, iter(items))
        self.assertListEqual(list(bp), [{u"_eid": u"9", u"title": u"T", u"_parenth": u"/contacts"}])
        self.assertNotIn(u"9", tr.storage["data"][u"e_contact"])

    def test_last_section(self):
        csvp = self.tempdir()
        pr_tool = self.portal.portal_repository
        orig_vct = list(pr_tool._versionable_content_types)
        annot = IAnnotations(self.portal)
        # as done by the init section
        annot["transmo.pr_vct"] = tuple(orig_vct)
        annot["transmo.pr_vpm"] = dict(pr_tool._version_policy_mapping)
        pr_tool._versionable_content_types = []
        pr_tool._version_policy_mapping = {}
        GlobalSettings(self.portal).auto_convert = False
        tr = self.get_transmogrifier(parts="at", csvp=csvp, lastsection={"pkl_dump": []})
        tr.storage["course"]["a__read"] = 3
        # dumps registered by pickle_data sections
        d_condition = "python:'dumped' in filename and storage['parts'] == 'at'"
        options = {"filename": "2_dumped.pkl", "store_key": "e_dumped", "d_condition": d_condition}
        PickleData(tr, "a__pkl1", options, None)
        PickleData(tr, "a__pkl2", {"filename": "2_not.pkl", "store_key": "e_not"}, None)
        tr.storage["data"][u"e_dumped"] = {u"1": {u"title": u"Été"}}
        self.assertListEqual(list(LastSection(tr, "lastsection", {}, iter([{u"prev": 1}]))), [{u"prev": 1}])
        with io.open(os.path.join(csvp, u"2_dumped.pkl"), "rb") as fh:
            self.assertDictEqual(cPickle.load(fh), {u"1": {u"title": u"Été"}})
        self.assertFalse(os.path.exists(os.path.join(csvp, u"2_not.pkl")))
        # documentviewer auto convert enabled again (t part)
        self.assertTrue(GlobalSettings(self.portal).auto_convert)
        # versioning: content types restored
        self.assertListEqual(pr_tool._versionable_content_types, orig_vct)
        self.assertNotIn("transmo.pr_vct", annot)
        # Plone 4 behaviour (bug?): the policy mapping is not restored but its stored value is removed
        self.assertDictEqual(pr_tool._version_policy_mapping, {})
        self.assertNotIn("transmo.pr_vpm", annot)
        # course
        self.assertListEqual(list(tr.storage["course"].items()), [("a__read", 3), ("lastsection", 1)])
        self.assertListEqual(
            self.messages(),
            [
                ("dto", "INFO", u"Dumping '{}'".format(os.path.join(csvp, u"2_dumped.pkl"))),
                ("dto", "INFO", "COURSE:"),
                ("dto", "INFO", "> a__read: 3"),
                ("dto", "INFO", "> lastsection: 1"),
            ],
        )
        # archive site, no tuv part: nothing restored
        self.patch_archive_site()
        annot["transmo.pr_vct"] = (u"Document",)
        pr_tool._versionable_content_types = []
        GlobalSettings(self.portal).auto_convert = False
        tr = self.get_transmogrifier(parts="a", lastsection={"pkl_dump": []})
        list(LastSection(tr, "lastsection", {}, iter([])))
        self.assertListEqual(pr_tool._versionable_content_types, [])
        self.assertIn("transmo.pr_vct", annot)
        self.assertFalse(GlobalSettings(self.portal).auto_convert)

    def test_set_owner(self):
        tr = self.get_transmogrifier()
        org = self.portal.contacts["electrabel"]
        creators = tuple(org.listCreators())
        swde_owner = self.portal.contacts["swde"].getOwner().getId()
        options = {"owner": "agent", "condition": "python:'skip' not in item"}
        items = [{u"_path": u"/contacts/electrabel"}, {u"_path": u"/contacts/swde", u"skip": 1}]
        self.assertListEqual(list(SetOwner(tr, "owner", options, iter(items))), items)
        self.assertTupleEqual(org.listCreators(), ("agent",) + creators)
        self.assertEqual(org.getOwner().getId(), "agent")
        self.assertEqual(self.portal.contacts["swde"].getOwner().getId(), swde_owner)
        self.assertNotEqual(swde_owner, "agent")
        self.assertEqual(dict(tr.storage["course"]), {"owner": 1})
        # owner already in creators
        list(SetOwner(tr, "owner", options, iter(items[:1])))
        self.assertTupleEqual(org.listCreators(), ("agent",) + creators)
        # unknown owner: stop
        self.assertRaises(SystemExit, SetOwner, tr, "owner", {"owner": "unknown"}, None)
        self.assertListEqual(self.messages(), [("dto", "ERROR", "Section name 'owner': owner not found 'unknown' !")])

    def test_set_state(self):
        tr = self.get_transmogrifier(parts="l")
        wkf = "incomingmail_workflow"
        brains = sorted(api.content.find(portal_type="dmsincomingmail"), key=lambda b: b.getId)
        im1, im2 = brains[0].getObject(), brains[1].getObject()
        path1, path2 = self.path(im1), self.path(im2)
        time2 = im2.workflow_history[wkf][0]["time"]
        mdate = datetime(2020, 1, 2, 3, 4)
        # replace last state
        options = {
            "workflow_id": wkf,
            "state_id": "closed",
            "action_id": "close",
            "replace": "created",
            "date_key": "_date",
            "actor": "agent",
            "condition": "python:'skip' not in item",
        }
        items = [{u"_eid": u"1", u"_path": path1, u"_date": mdate}, {u"_eid": u"2", u"_path": path2, u"skip": 1}]
        self.assertListEqual(list(SetState(tr, "l__state", options, iter(items))), items)
        self.assertTupleEqual(
            im1.workflow_history[wkf],
            ({"action": "close", "actor": "agent", "comments": "", "review_state": "closed", "time": DateTime(mdate)},),
        )
        self.assertEqual(api.content.get_state(im1), "closed")
        self.assertEqual(api.content.get_state(im2), "created")
        self.assertEqual(dict(tr.storage["course"]), {"l__state": 1})
        # replace on another state: nothing changed
        list(SetState(tr, "l__state", dict(options, replace="proposed_to_agent"), iter(items[:1])))
        self.assertEqual(im1.workflow_history[wkf][-1]["review_state"], "closed")
        # replace without date value, default actor
        options = {"workflow_id": wkf, "action_id": "reopen", "replace": "created"}
        list(SetState(tr, "l__state", options, iter([{u"_eid": u"2", u"_path": path2}])))
        self.assertTupleEqual(
            im2.workflow_history[wkf],
            ({"action": "reopen", "actor": TEST_USER_ID, "comments": "", "review_state": "created", "time": time2},),
        )
        # new state appended
        options = {"workflow_id": wkf, "state_id": "proposed_to_agent", "action_id": "propose", "date_key": "_date"}
        item = {u"_bpk": u"e_mail", u"_eid": u"2", u"_path": path2, u"_date": mdate}
        list(SetState(tr, "l__state", options, iter([dict(item)])))
        self.assertEqual(len(im2.workflow_history[wkf]), 2)
        self.assertDictEqual(
            im2.workflow_history[wkf][-1],
            {
                "action": "propose",
                "actor": TEST_USER_ID,
                "comments": "",
                "review_state": "proposed_to_agent",
                "time": DateTime(mdate),
            },
        )
        self.assertEqual(api.content.get_state(im2), "proposed_to_agent")
        self.assertListEqual(self.messages(), [])
        # history already longer than 1: error logged, state appended anyway
        list(SetState(tr, "l__state", options, iter([dict(item)])))
        self.assertEqual(len(im2.workflow_history[wkf]), 3)
        self.assertEqual(len(self.messages()), 1)
        self.assertTrue(
            self.messages()[0][2].startswith(u"e_mail: _eid 2, workflow_history len > 1: "), self.messages()
        )
        # unknown path: error logged, item not yielded
        self.logs.clear()
        item = {u"_bpk": u"e_mail", u"_eid": u"9", u"_path": u"/incoming-mail/unknown"}
        self.assertListEqual(list(SetState(tr, "l__state", options, iter([item]))), [])
        self.assertListEqual(
            self.messages(),
            [("dti", "ERROR", u"e_mail: _eid 9, The corresponding object '/incoming-mail/unknown' cannot be found")],
        )
        # Plone 4 behaviour (bug?): unknown workflow id is logged, then fails
        self.logs.clear()
        item = {u"_bpk": u"e_mail", u"_eid": u"1", u"_path": path1, u"_date": mdate}
        bp = SetState(tr, "l__state", dict(options, workflow_id="xx_workflow"), iter([item]))
        self.assertRaises(KeyError, list, bp)
        self.assertTrue(self.messages()[0][2].startswith(u"e_mail: _eid 1, Cannot find 'xx_workflow' wkf id in"))
        # Plone 4 behaviour (bug?): appending a state without date_key fails
        options = {"workflow_id": wkf, "state_id": "closed", "action_id": "close"}
        self.assertRaises(KeyError, list, SetState(tr, "l__state", options, iter([item])))
        # not in part
        self.assertListEqual(list(SetState(tr, "a__state", options, iter([item]))), [item])

    def test_set_iconified_attributes(self):
        tr = self.get_transmogrifier(parts="u")
        brains = sorted(api.content.find(portal_type="dmsommainfile"), key=lambda b: b.getPath())
        fil1, fil2 = brains[0].getObject(), brains[1].getObject()
        self.assertTupleEqual((fil1.to_print, fil1.to_sign, fil1.signed), (True, True, False))
        options = {
            "values": "python:{'to_print': False, 'signed': item['_signed']}",
            "condition": "python:'skip' not in item",
        }
        items = [{u"_eid": u"1", u"_path": self.path(fil1), u"_signed": True}, {u"_path": self.path(fil2), u"skip": 1}]
        self.assertListEqual(list(SetIconifiedAttributes(tr, "u__icon", options, iter(items))), items)
        self.assertTupleEqual((fil1.to_print, fil1.to_sign, fil1.signed), (False, True, True))
        cat_elem = fil1.aq_parent.categorized_elements[fil1.UID()]
        self.assertTupleEqual((cat_elem["to_print"], cat_elem["to_sign"], cat_elem["signed"]), (False, True, True))
        self.assertTupleEqual((fil2.to_print, fil2.signed), (True, False))
        self.assertTrue(fil2.aq_parent.categorized_elements[fil2.UID()]["to_print"])
        self.assertEqual(dict(tr.storage["course"]), {"u__icon": 1})
        # unknown path: error logged, item yielded
        item = {u"_bpk": u"e_file", u"_eid": u"9", u"_path": u"/outgoing-mail/unknown", u"_signed": True}
        self.assertListEqual(list(SetIconifiedAttributes(tr, "u__icon", options, iter([item]))), [item])
        self.assertListEqual(
            self.messages(),
            [("dti", "ERROR", u"e_file: _eid 9, The corresponding object '/outgoing-mail/unknown' cannot be found")],
        )
        # not in part
        self.assertListEqual(list(SetIconifiedAttributes(tr, "a__icon", options, iter(items[:1]))), items[:1])
