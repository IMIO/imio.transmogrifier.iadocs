"""csv_files blueprints tests."""

from imio.transmogrifier.iadocs.blueprints.csv_files import CSVReader
from imio.transmogrifier.iadocs.blueprints.csv_files import CSVWriter
from imio.transmogrifier.iadocs.blueprints.csv_files import writerow
from imio.transmogrifier.iadocs.testing import get_transmogrifier
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING
from plone.base.utils import safe_text
from zope.testing.loggingsupport import InstalledHandler

import os
import shutil
import tempfile
import unittest


SERVICES_CSV = b'"eid","title","_A","desc"\n"1"," \xc3\x89t\xc3\xa9 ","x","NULL"\n"2","Hiver","y",""\n'


class TestBluePrintCsvFiles(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.csvp = safe_text(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.csvp)
        self.logs = InstalledHandler("dti", "dto")
        self.addCleanup(self.logs.uninstall)

    def get_transmogrifier(self, config=None, data=None):
        transmogrifier = get_transmogrifier(self.portal, config=config)
        transmogrifier.storage.update({"parts": "a", "csvp": self.csvp, "data": data or {}})
        return transmogrifier

    def path(self, filename):
        return os.path.join(self.csvp, filename)

    def write(self, filename, content):
        with open(self.path(filename), "wb") as fh:
            fh.write(content)

    def read(self, filename):
        with open(self.path(filename), "rb") as fh:
            return fh.read()

    def messages(self):
        return [(rec.name, rec.levelname, rec.getMessage()) for rec in self.logs.records]

    def test_csv_reader(self):
        self.write("services.csv", SERVICES_CSV)
        # header line skipped, _X columns removed, values stripped and decoded, none_value from config
        tr = self.get_transmogrifier(config={"none_value": "NULL"})
        options = {"filename": "services.csv", "fieldnames": "eid title _A desc", "bp_key": "e_service"}
        bp = CSVReader(tr, "a__csv", options, iter([{"prev": 1}]))
        csv_d = tr.storage["csv"]["e_service"]
        self.assertEqual(csv_d["fp"], self.path("services.csv"))
        self.assertEqual(csv_d["fn"], "services.csv")
        self.assertEqual(csv_d["fd"], ["eid", "title", "_A", "desc"])
        self.assertIsNotNone(csv_d["fh"])
        items = list(bp)
        self.assertEqual(
            items,
            [
                {"prev": 1},
                {"_bpk": "e_service", "_ln": 2, "eid": "1", "title": "Été", "desc": None},
                {"_bpk": "e_service", "_ln": 3, "eid": "2", "title": "Hiver", "desc": ""},
            ],
        )
        self.assertIsInstance(items[1]["title"], str)
        self.assertIsInstance(items[2]["desc"], str)
        self.assertEqual(csv_d["fd"], ["eid", "title", "desc"])
        self.assertIsNone(csv_d["fh"])
        self.assertEqual(dict(tr.storage["course"]), {"a__csv": 2})
        self.assertIn(("dto", "INFO", "Reading '{}'".format(self.path("services.csv"))), self.messages())
        # none_value option wins over config, csv_key, absolute filename
        tr = self.get_transmogrifier(config={"none_value": "NULL"})
        options = {
            "filename": self.path("services.csv"),
            "fieldnames": "eid title _A desc",
            "bp_key": "e_service",
            "csv_key": "e_serv",
            "none_value": "Hiver",
        }
        items = list(CSVReader(tr, "a__csv", options, iter([])))
        self.assertEqual(sorted(tr.storage["csv"]), ["e_serv"])
        self.assertEqual([it["title"] for it in items], ["Été", None])
        self.assertEqual([it["desc"] for it in items], ["NULL", ""])
        # a quoted value keeps its line break as is (no newline translation)
        self.write("multi.csv", b'1,"a\r\nb",x\r\n')
        tr = self.get_transmogrifier(config={"none_value": "NULL"})
        options = {"filename": "multi.csv", "fieldnames": "eid title desc", "bp_key": "e_m", "csv_headers": "python:0"}
        self.assertEqual([it["title"] for it in CSVReader(tr, "a__csv", options, iter([]))], ["a\r\nb"])
        # csv_delimiter from config, csv_encoding, csv_headers false (first line is data)
        self.write("latin.csv", b"1;\xc9t\xe9\n2;Hiver\n")
        tr = self.get_transmogrifier(config={"csv_delimiter": ";"})
        options = {
            "filename": "latin.csv",
            "fieldnames": "eid title",
            "bp_key": "e_latin",
            "csv_encoding": "latin1",
            "csv_headers": "python:False",
        }
        items = list(CSVReader(tr, "a__csv", options, iter([])))
        self.assertEqual(options["fmtparam-delimiter"], "python:';'")  # options are modified
        self.assertEqual(
            items,
            [
                {"_bpk": "e_latin", "_ln": 1, "eid": "1", "title": "Été"},
                {"_bpk": "e_latin", "_ln": 2, "eid": "2", "title": "Hiver"},
            ],
        )
        # fmtparam option wins over config csv_delimiter, dialect option
        self.write("pipe.csv", b"eid|title\n1|A\n")
        tr = self.get_transmogrifier(config={"csv_delimiter": ";"})
        options = {"filename": "pipe.csv", "fieldnames": "eid title", "bp_key": "e", "fmtparam-delimiter": "python:'|'"}
        self.assertEqual(
            list(CSVReader(tr, "a__csv", options, iter([]))), [{"_bpk": "e", "_ln": 2, "eid": "1", "title": "A"}]
        )
        self.write("tab.csv", b"eid\ttitle\n1\tA\n")
        options = {"filename": "tab.csv", "fieldnames": "eid title", "bp_key": "e", "dialect": "excel-tab"}
        self.assertEqual(
            list(CSVReader(self.get_transmogrifier(), "a__csv", options, iter([]))),
            [{"_bpk": "e", "_ln": 2, "eid": "1", "title": "A"}],
        )
        # after the first line, a short row gets empty values and a long row gets a _rest key
        self.write("rows.csv", b"eid,title\n1\n2,B,extra\n")
        options = {"filename": "rows.csv", "fieldnames": "eid title", "bp_key": "e"}
        self.assertEqual(
            list(CSVReader(self.get_transmogrifier(), "a__csv", options, iter([]))),
            [
                {"_bpk": "e", "_ln": 2, "eid": "1", "title": ""},
                {"_bpk": "e", "_ln": 3, "eid": "2", "title": "B", "_rest": ["extra"]},
            ],
        )
        # more columns in file than fieldnames (header line checked)
        self.write("cols.csv", b"_eid,title,desc\n1,A,B\n")
        options = {"filename": "cols.csv", "fieldnames": "_eid title", "bp_key": "e"}
        bp = CSVReader(self.get_transmogrifier(), "a__csv", options, iter([]))
        self.assertRaisesRegex(
            Exception, r"^Some columns for cols.csv are not defined in fieldnames: \[u?'desc'\]$", list, bp
        )
        self.logs.clear()
        options["raise_on_error"] = "0"
        self.assertEqual(list(CSVReader(self.get_transmogrifier(), "a__csv", options, iter([]))), [])
        self.assertEqual(
            self.messages()[1],
            ("dti", "CRITICAL", "e: _eid _eid, STOPPING: some columns are not defined in fieldnames: ['desc']"),
        )
        # Plone 4 behaviour (bug?): without _eid column, the error logging fails (log_error needs item['_eid'])
        options = {"filename": "cols.csv", "fieldnames": "eid title", "bp_key": "e"}
        self.assertRaises(KeyError, list, CSVReader(self.get_transmogrifier(), "a__csv", options, iter([])))
        # less columns in file than fieldnames
        options = {"filename": "cols.csv", "fieldnames": "_eid title desc extra", "bp_key": "e"}
        bp = CSVReader(self.get_transmogrifier(), "a__csv", options, iter([]))
        self.assertRaisesRegex(
            Exception, r"^To much columns for cols.csv defined in fieldnames: \[u?'extra'\]$", list, bp
        )
        self.logs.clear()
        options["raise_on_error"] = "0"
        self.assertEqual(list(CSVReader(self.get_transmogrifier(), "a__csv", options, iter([]))), [])
        self.assertEqual(self.messages()[1][:2], ("dti", "CRITICAL"))
        self.assertRegex(
            self.messages()[1][2],
            r"^e: _eid _eid, STOPPING: to much columns defined in fieldnames: \[u?'extra'\]$",
        )
        # b_condition false: file not read
        tr = self.get_transmogrifier()
        options = {
            "filename": "services.csv",
            "fieldnames": "eid title _A desc",
            "bp_key": "e",
            "b_condition": "python:not filename.endswith('services.csv')",
        }
        self.assertEqual(list(CSVReader(tr, "a__csv", options, iter([{"prev": 1}]))), [{"prev": 1}])
        self.assertEqual(tr.storage["csv"], {})
        # empty filename
        options = {"filename": "", "fieldnames": "eid", "bp_key": "e"}
        self.assertEqual(list(CSVReader(tr, "a__csv", options, iter([{"prev": 1}]))), [{"prev": 1}])
        self.assertEqual(tr.storage["csv"], {})
        # unexisting file
        options = {"filename": "unknown.csv", "fieldnames": "eid", "bp_key": "e"}
        self.assertRaisesRegex(Exception, r"^Cannot open file '.+/unknown.csv'$", CSVReader, tr, "a__csv", options, [])
        # not in part
        options = {"filename": "services.csv", "fieldnames": "eid title _A desc", "bp_key": "e"}
        self.assertEqual(list(CSVReader(tr, "b__csv", options, iter([{"prev": 1}]))), [{"prev": 1}])
        self.assertEqual(tr.storage["csv"], {})
        self.assertEqual(dict(tr.storage["course"]), {})

    def test_writerow(self):
        csv_d = {
            "fp": self.path("out.csv"),
            "fh": None,
            "append": False,
            "wp": {"dialect": "excel"},
            "hd": ["Id", "Titre"],
            "fd": ["eid", "title"],
            "we": "utf8",
        }
        writerow(csv_d, {"eid": "1", "title": "Été", "other": "x"})
        self.assertIsNotNone(csv_d["fh"])
        self.assertIsNotNone(csv_d["wh"])
        writerow(csv_d, {"eid": 2, "title": None})  # non text values are written as is
        writerow(csv_d, {"eid": "3"})  # missing value
        csv_d["fh"].close()
        self.assertEqual(self.read("out.csv"), b"Id,Titre\r\n1,\xc3\x89t\xc3\xa9\r\n2,\r\n3,\r\n")
        self.assertIn(("dto", "INFO", "Writing '{}'".format(self.path("out.csv"))), self.messages())
        # append mode on existing file: no header
        csv_d.update({"fh": None, "append": True, "we": "latin1"})
        writerow(csv_d, {"eid": "4", "title": "Été"})
        csv_d["fh"].close()
        self.assertEqual(self.read("out.csv"), b"Id,Titre\r\n1,\xc3\x89t\xc3\xa9\r\n2,\r\n3,\r\n4,\xc9t\xe9\r\n")
        # append mode on new file: header
        csv_d.update({"fh": None, "fp": self.path("new.csv"), "wp": {"dialect": "excel", "delimiter": ";"}})
        writerow(csv_d, {"eid": "1", "title": "A"})
        csv_d["fh"].close()
        self.assertEqual(self.read("new.csv"), b"Id;Titre\r\n1;A\r\n")
        # no header
        csv_d.update({"fh": None, "fp": self.path("nohd.csv"), "append": False, "hd": []})
        writerow(csv_d, {"eid": "1", "title": "A"})
        csv_d["fh"].close()
        self.assertEqual(self.read("nohd.csv"), b"1;A\r\n")
        # file cannot be created
        csv_d.update({"fh": None, "fp": self.path("unknown/out.csv")})
        self.assertRaisesRegex(Exception, r"^Cannot create file '.+/unknown/out.csv': ", writerow, csv_d, {})

    def test_csv_writer(self):
        items = [{"eid": "1", "title": "Été"}, {"eid": "2", "title": "B"}]
        # items written, headers, items yielded (default)
        tr = self.get_transmogrifier()
        options = {"filename": "out.csv", "fieldnames": "eid title", "headers": "Id Titre", "bp_key": "e_out"}
        bp = CSVWriter(tr, "a__csvw", options, iter([dict(it) for it in items]))
        csv_d = tr.storage["csv"]["e_out"]
        self.assertEqual(
            {k: v for k, v in csv_d.items() if k != "wp"},
            {
                "fp": self.path("out.csv"),
                "fh": None,
                "fn": "out.csv",
                "wh": None,
                "we": "utf8",
                "fd": ["eid", "title"],
                "hd": ["Id", "Titre"],
                "append": False,
            },
        )
        self.assertEqual(csv_d["wp"], {"dialect": "excel"})
        self.assertEqual(list(bp), items)
        self.assertEqual(self.read("out.csv"), b"Id,Titre\r\n1,\xc3\x89t\xc3\xa9\r\n2,B\r\n")
        self.assertIsNone(csv_d["fh"])
        self.assertEqual(dict(tr.storage["course"]), {"a__csvw": 2})
        # condition, yield 0, csv_key, csv_encoding, dialect, fmtparam, absolute filename
        tr = self.get_transmogrifier()
        options = {
            "filename": self.path("cond.csv"),
            "fieldnames": "eid title",
            "bp_key": "e_out",
            "csv_key": "e_csv",
            "condition": "python:item['eid'] != u'2' and 'csv' in storage",
            "yield": "0",
            "csv_encoding": "latin1",
            "dialect": "excel-tab",
            "fmtparam-lineterminator": "python:'\\n'",
        }
        bp = CSVWriter(tr, "a__csvw", options, iter([dict(it) for it in items]))
        self.assertEqual(sorted(tr.storage["csv"]), ["e_csv"])
        self.assertEqual(list(bp), [items[1]])
        self.assertEqual(self.read("cond.csv"), b"1\t\xc9t\xe9\n")
        # append mode on existing file
        tr = self.get_transmogrifier()
        options = {"filename": "out.csv", "fieldnames": "eid", "headers": "Id", "bp_key": "e", "append_mode": "1"}
        self.assertEqual(len(list(CSVWriter(tr, "a__csvw", options, iter([{"eid": "3"}])))), 1)
        self.assertEqual(self.read("out.csv"), b"Id,Titre\r\n1,\xc3\x89t\xc3\xa9\r\n2,B\r\n3\r\n")
        # from stored dict, sorted on store key, written once at first item
        data = {"e_out": {"2": {"title": "B", "order": 1}, "1": {"title": "Été", "order": 2}}}
        tr = self.get_transmogrifier(data=data)
        options = {"filename": "dic.csv", "fieldnames": "eid title order", "bp_key": "e_out", "store_key": "eid"}
        self.assertEqual(list(CSVWriter(tr, "a__csvw", options, iter([{"x": 1}, {"x": 2}]))), [{"x": 1}, {"x": 2}])
        self.assertEqual(self.read("dic.csv"), b"1,\xc3\x89t\xc3\xa9,2\r\n2,B,1\r\n")
        self.assertEqual(dict(tr.storage["course"]), {"a__csvw": 1})
        # from stored dict without items, sorted on a field
        tr = self.get_transmogrifier(data=data)
        options.update({"filename": "dic2.csv", "store_key_sort": "order"})
        self.assertEqual(list(CSVWriter(tr, "a__csvw", options, iter([]))), [])
        self.assertEqual(self.read("dic2.csv"), b"2,B,1\r\n1,\xc3\x89t\xc3\xa9,2\r\n")
        self.assertEqual(dict(tr.storage["course"]), {"a__csvw": 1})
        # from stored dict with sub key
        data = {"e_sub": {"1": {"b": {"title": "B"}, "a": {"title": "A"}}, "0": {"c": {"title": "C"}}}}
        tr = self.get_transmogrifier(data=data)
        options = {
            "filename": "sub.csv",
            "fieldnames": "eid sid title",
            "bp_key": "e_sub",
            "store_key": "eid",
            "store_subkey": "sid",
        }
        list(CSVWriter(tr, "a__csvw", options, iter([])))
        self.assertEqual(self.read("sub.csv"), b"0,c,C\r\n1,a,A\r\n1,b,B\r\n")
        # from stored dict, not sorted
        data = {"e_one": {"1": {"title": "A"}}}
        tr = self.get_transmogrifier(data=data)
        options = {
            "filename": "one.csv",
            "fieldnames": "eid title",
            "bp_key": "e_one",
            "store_key": "eid",
            "store_key_sort": "__no_sort__",
        }
        list(CSVWriter(tr, "a__csvw", options, iter([])))
        self.assertEqual(self.read("one.csv"), b"1,A\r\n")
        # from empty stored dict: no file
        tr = self.get_transmogrifier(data={"e_empty": {}})
        options = {"filename": "empty.csv", "fieldnames": "eid", "bp_key": "e_empty", "store_key": "eid"}
        self.assertEqual(list(CSVWriter(tr, "a__csvw", options, iter([{"x": 1}]))), [{"x": 1}])
        self.assertFalse(os.path.exists(self.path("empty.csv")))
        # b_condition false
        tr = self.get_transmogrifier()
        options = {
            "filename": "bcond.csv",
            "fieldnames": "eid",
            "bp_key": "e",
            "b_condition": "python:not filename.endswith('bcond.csv') and storage",
        }
        self.assertEqual(list(CSVWriter(tr, "a__csvw", options, iter([dict(items[0])]))), [items[0]])
        self.assertFalse(os.path.exists(self.path("bcond.csv")))
        self.assertEqual(tr.storage["csv"], {})
        # not in part
        options = {"filename": "part.csv", "fieldnames": "eid", "bp_key": "e"}
        self.assertEqual(list(CSVWriter(tr, "b__csvw", options, iter([dict(items[0])]))), [items[0]])
        self.assertFalse(os.path.exists(self.path("part.csv")))
