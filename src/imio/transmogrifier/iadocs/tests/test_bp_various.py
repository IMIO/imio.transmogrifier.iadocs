"""Blueprints tests for this package."""

from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.blueprints.various import Breakpoint
from imio.transmogrifier.iadocs.blueprints.various import Count
from imio.transmogrifier.iadocs.blueprints.various import EnhancedCondition
from imio.transmogrifier.iadocs.blueprints.various import EnhancedInserter
from imio.transmogrifier.iadocs.blueprints.various import EnhancedManipulator
from imio.transmogrifier.iadocs.blueprints.various import FilterItem
from imio.transmogrifier.iadocs.blueprints.various import ItemFieldSplit
from imio.transmogrifier.iadocs.blueprints.various import NeedOther
from imio.transmogrifier.iadocs.blueprints.various import PrintItem
from imio.transmogrifier.iadocs.blueprints.various import ReplaceVariables
from imio.transmogrifier.iadocs.blueprints.various import ShortLog
from imio.transmogrifier.iadocs.blueprints.various import Stop
from imio.transmogrifier.iadocs.blueprints.various import TimeDisplay
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import get_transmogrifier
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa
from zope.testing.loggingsupport import InstalledHandler

import io
import pdb
import re
import sys
import unittest


class TestBluePrintVarious(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.portal.context = self.portal
        self.storage = get_storage(self.portal)
        self.logs = InstalledHandler("dti", "dto")
        self.addCleanup(self.logs.uninstall)

    def get_transmogrifier(self, parts="a", data=None):
        transmogrifier = get_transmogrifier(self.portal)
        transmogrifier.storage.update({"parts": parts, "data": data or {}})
        return transmogrifier

    def messages(self):
        return [(rec.name, rec.levelname, rec.getMessage()) for rec in self.logs.records]

    def test_enhanced_condition(self):
        # cond1: False, cond2: False
        bp = EnhancedCondition(self.portal, "", {"condition1": "string:", "condition2": "string:"}, None)
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {})
        # cond1: True, cond2: False
        bp = EnhancedCondition(self.portal, "", {"condition1": "string:1", "condition2": "string:"}, None)
        bp.previous = [{}]
        self.assertRaises(StopIteration, next, iter(bp))
        # cond1: True, cond2: True
        bp = EnhancedCondition(self.portal, "", {"condition1": "string:1", "condition2": "string:1"}, None)
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {})
        # get_obj: obj and storage given to condition2, course counted when condition1 is matched
        tr = self.get_transmogrifier()
        options = {
            "condition1": "python:item.get('_path') and 'course' in storage",
            "condition2": "python:obj.getId() == 'portal_workflow'",
            "get_obj": "1",
        }
        items = [{"_path": "/portal_workflow"}, {"_path": "portal_catalog"}, {}]
        bp = EnhancedCondition(tr, "a__cond", options, iter(items))
        self.assertEqual(list(bp), [{"_path": "/portal_workflow"}, {}])
        self.assertEqual(dict(tr.storage["course"]), {"a__cond": 2})
        # condition2 error: item not yielded, error logged with default or given message
        options = {"condition1": "python:True", "condition2": "python:item['xx']"}
        self.assertEqual(list(EnhancedCondition(tr, "a__cond", options, iter([{"_eid": "5"}]))), [])
        options["error"] = "python:u'bad ' + item['_eid']"
        self.assertEqual(list(EnhancedCondition(tr, "a__cond", options, iter([{"_eid": "6"}]))), [])
        self.assertEqual(
            self.messages(),
            [
                ("dti", "ERROR", "a__cond: condition 2 error for eid 5 ('xx')"),
                ("dti", "ERROR", "a__cond: bad 6 ('xx')"),
            ],
        )

    def test_enhanced_inserter(self):
        # minimal options
        bp = EnhancedInserter(self.portal, "", {"key": "string:aa", "value": "python:u'bb'"}, None)
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {"aa": "bb"})
        # with False condition
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u'bb'", "condition": "python:key in item"}, None
        )
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {})
        # with True condition
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u'bb'", "condition": "python:key not in item"}, None
        )
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {"aa": "bb"})
        # without error_value
        bp = EnhancedInserter(self.portal, "", {"key": "string:aa", "value": "python: unknown"}, None)
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {})
        # with error_value
        bp = EnhancedInserter(
            self.portal,
            "",
            {"key": "string:aa", "value": "python: unknown", "error_value": "python: u'fallback'"},
            None,
        )
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {"aa": "fallback"})
        # with bad error_value
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python: unknown", "error_value": "python: nok"}, None
        )
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {})
        # with separator, without initial value
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u'bb'", "separator": "python:u'\\r\\n'"}, None
        )
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {"aa": "bb"})
        # with separator, with initial value, without value
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u''", "separator": "python:u'\\r\\n'"}, None
        )
        bp.previous = [{"aa": "one"}]
        self.assertDictEqual(next(iter(bp)), {"aa": "one"})
        # with separator, with initial value, with value
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u'bb'", "separator": "python:u'\\r\\n'"}, None
        )
        bp.previous = [{"aa": "one"}]
        self.assertDictEqual(next(iter(bp)), {"aa": "one\r\nbb"})
        # with False second condition and get_obj
        bp = EnhancedInserter(
            self.portal,
            "",
            {"key": "string:aa", "value": "python:u'bb'", "condition2": "python:'toto' in obj", "get_obj": "1"},
            None,
        )
        bp.previous = [{"_path": "portal_workflow"}]
        self.assertDictEqual(next(iter(bp)), {"_path": "portal_workflow"})
        # with True second condition and get_obj
        bp = EnhancedInserter(
            self.portal,
            "",
            {
                "key": "string:aa",
                "value": "python:u'bb'",
                "condition2": "python:'plone_workflow' in obj",
                "get_obj": "1",
            },
            None,
        )
        bp.previous = [{"_path": "portal_workflow"}]
        self.assertDictEqual(next(iter(bp)), {"_path": "portal_workflow", "aa": "bb"})
        self.logs.clear()
        # key, condition and value expressions get item, key, storage and obj; course
        tr = self.get_transmogrifier()
        options = {
            "key": "python:item['_k']",
            "value": "python:u'{} {}'.format(obj.getId(), key)",
            "condition": "python:key == u'dest' and 'course' in storage",
            "get_obj": "1",
        }
        items = [{"_k": "dest", "_path": "/portal_workflow"}, {"_k": "other", "_path": "/portal_workflow"}]
        self.assertEqual(
            list(EnhancedInserter(tr, "a__ins", options, iter(items))),
            [
                {"_k": "dest", "_path": "/portal_workflow", "dest": "portal_workflow dest"},
                {"_k": "other", "_path": "/portal_workflow"},
            ],
        )
        self.assertEqual(dict(tr.storage["course"]), {"a__ins": 1})
        # get_obj without object: error logged with default or given message, item yielded unchanged
        options = {"key": "string:aa", "value": "python:u'bb'", "get_obj": "1"}
        self.assertEqual(list(EnhancedInserter(tr, "a__ins", options, iter([{"_eid": "7"}]))), [{"_eid": "7"}])
        options["error"] = "python:u'no obj ' + item['_eid']"
        items = [{"_eid": "8", "_path": "/unknown"}]
        self.assertEqual(
            list(EnhancedInserter(tr, "a__ins", options, iter(items))), [{"_eid": "8", "_path": "/unknown"}]
        )
        self.assertEqual(
            self.messages(),
            [
                ("dti", "ERROR", "a__ins: error getting value for eid 7 (object not found for item)"),
                ("dti", "ERROR", "a__ins: no obj 8 (object not found for item)"),
            ],
        )

    def test_breakpoint(self):
        calls = []
        orig_set_trace = pdb.set_trace
        pdb.set_trace = lambda: calls.append(1)
        self.addCleanup(setattr, pdb, "set_trace", orig_set_trace)
        tr = self.get_transmogrifier()
        # default condition: False
        self.assertEqual(list(Breakpoint(tr, "bp", {}, iter([{"_eid": "1"}]))), [{"_eid": "1"}])
        self.assertEqual(calls, [])
        # matched condition (with storage)
        options = {"condition": "python:item['_eid'] == u'2' and storage['parts'] == 'a'"}
        items = [{"_eid": "1"}, {"_eid": "2"}]
        self.assertEqual(list(Breakpoint(tr, "bp", options, iter(items))), [{"_eid": "1"}, {"_eid": "2"}])
        self.assertEqual(calls, [1])

    def test_count(self):
        tr = self.get_transmogrifier()
        items = [{"_bpk": "a"}, {"_bpk": "b"}, {"_bpk": "a"}, {}, {"_bpk": "c", "skip": 1}]
        # group_key and condition
        options = {"group_key": "_bpk", "condition": "python:'skip' not in item"}
        self.assertEqual(list(Count(tr, "count_g", options, iter([dict(it) for it in items]))), items)
        self.assertEqual(tr.storage["count"], {"count_g": {"a": {"c": 2}, "b": {"c": 1}, "_bpk_missing": {"c": 1}}})
        self.assertEqual(dict(tr.storage["course"]), {"count_g": 4})
        self.assertEqual(
            sorted(self.messages()),
            [
                ("dto", "INFO", "count_g '_bpk_missing' = 1"),
                ("dto", "INFO", "count_g 'a' = 2"),
                ("dto", "INFO", "count_g 'b' = 1"),
            ],
        )
        # without group_key; a new section with the same name resets its counter
        self.logs.clear()
        self.assertEqual(len(list(Count(tr, "count", {}, iter([dict(it) for it in items])))), 5)
        self.assertEqual(tr.storage["count"]["count"], {"": {"c": 5}})
        list(Count(tr, "count_g", {}, iter([])))
        self.assertEqual(tr.storage["count"]["count_g"], {})
        self.assertEqual(self.messages(), [("dto", "INFO", "count '' = 5")])

    def test_enhanced_manipulator(self):
        tr = self.get_transmogrifier()
        lst = [1]
        # copy with regexp keys and destination expression, delete (copy + delete = move)
        options = {
            "keys": "re:^e_(.+)$\ntitle",
            "destination": "python:match is True and u'p_title' or u'p_' + match.group(1)",
            "delete": "e_old\ntitle",
            "condition": "python:'course' in storage",
        }
        items = [{"e_lst": lst, "e_old": "o", "title": "T", "other": "x"}]
        result = list(EnhancedManipulator(tr, "a__manip", options, iter(items)))
        self.assertEqual(result, [{"e_lst": [1], "p_lst": [1], "p_old": "o", "p_title": "T", "other": "x"}])
        self.assertIsNot(result[0]["p_lst"], lst)  # deep copy
        self.assertEqual(dict(tr.storage["course"]), {"a__manip": 1})
        # delete only
        result = list(EnhancedManipulator(tr, "a__manip", {"delete": "re:^_"}, iter([{"_a": 1, "_b": 2, "c": 3}])))
        self.assertEqual(result, [{"c": 3}])
        # condition not matched
        options = {"delete": "c", "condition": "python:False"}
        self.assertEqual(list(EnhancedManipulator(tr, "a__manip", options, iter([{"c": 3}]))), [{"c": 3}])

    def test_filter_item(self):
        tr = self.get_transmogrifier()
        items = [{"_eid": "1", "a": 1, "b": 2, "c": 3}, {"_eid": "2", "a": 1, "skip": 1}]
        options = {"kept_keys": "a c", "condition": "python:'skip' not in item"}
        self.assertEqual(
            list(FilterItem(tr, "a__filter", options, iter([dict(it) for it in items]))),
            [{"_eid": "1", "a": 1, "c": 3}, {"_eid": "2", "a": 1, "skip": 1}],
        )
        self.assertEqual(dict(tr.storage["course"]), {"a__filter": 1})
        # missing kept key is set to None
        result = list(FilterItem(tr, "a__filter", {"kept_keys": "a z"}, iter([dict(items[0])])))
        self.assertEqual(result, [{"_eid": "1", "a": 1, "z": None}])
        # no kept_keys or not in part: unchanged
        self.assertEqual(list(FilterItem(tr, "a__filter", {}, iter([dict(items[0])]))), [items[0]])
        options = {"kept_keys": "a"}
        self.assertEqual(list(FilterItem(tr, "b__filter", options, iter([dict(items[0])]))), [items[0]])

    def test_item_field_split(self):
        tr = self.get_transmogrifier()
        options = {"bp_key": "e_split", "field": "dest", "separator": "|", "index_key": "_idx"}
        items = [{"_bpk": "e_mail", "dest": "a|b|"}, {"_bpk": "e_mail", "dest": "c"}]
        result = list(ItemFieldSplit(tr, "a__split", options, iter([dict(it) for it in items])))
        self.assertEqual(
            result,
            [
                {"_bpk": "e_split", "dest": "a", "_idx": 0},
                {"_bpk": "e_split", "dest": "b", "_idx": 1},
                {"_bpk": "e_split", "dest": "", "_idx": 2},
                {"_bpk": "e_split", "dest": "c"},  # no index key when not split
            ],
        )
        self.assertIsInstance(ItemFieldSplit(tr, "a__split", options, iter([])).sep, str)
        self.assertEqual(dict(tr.storage["course"]), {"a__split": 2})
        # quoted separator, original yielded, condition
        options = {
            "bp_key": "e_split",
            "field": "dest",
            "separator": '", "',
            "yield_original": "1",
            "condition": "python:'skip' not in item",
        }
        items = [{"dest": "a, b"}, {"dest": "c, d", "skip": 1}]
        self.assertEqual(
            list(ItemFieldSplit(tr, "a__split", options, iter([dict(it) for it in items]))),
            [{"dest": "a, b"}, {"_bpk": "e_split", "dest": "a"}, {"_bpk": "e_split", "dest": "b"}, items[1]],
        )
        # field missing: the item copy is yielded with the new bp_key
        bp = ItemFieldSplit(tr, "a__split", options, iter([{"x": 1}]))
        self.assertEqual(list(bp), [{"x": 1}, {"_bpk": "e_split", "x": 1}])
        # Plone 4 behaviour (bug?): a None field value raises a TypeError
        self.assertRaises(TypeError, list, ItemFieldSplit(tr, "a__split", options, iter([{"dest": None}])))
        # not in part
        self.assertEqual(list(ItemFieldSplit(tr, "b__split", options, iter([dict(items[0])]))), [items[0]])

    def test_need_other(self):
        tr = self.get_transmogrifier(parts="la")
        # needed parts are there
        self.assertEqual(list(NeedOther(tr, "l__need", {"parts": "a"}, iter([{"a": 1}]))), [{"a": 1}])
        self.assertEqual(dict(tr.storage["course"]), {"l__need": 1})
        NeedOther(tr, "l__need", {}, iter([]))
        # a needed part is missing
        self.assertRaisesRegex(
            Exception,
            r"^STOPPED because 'l' part needs 'abc': missing 'b'$",
            NeedOther,
            tr,
            "l__need",
            {"parts": "abc"},
            iter([]),
        )
        # not in part
        self.assertEqual(list(NeedOther(tr, "m__need", {"parts": "z"}, iter([{"a": 1}]))), [{"a": 1}])
        self.assertEqual(dict(tr.storage["course"]), {"l__need": 3})

    def test_print_item(self):
        orig_stdout = sys.stdout
        sys.stdout = out = io.StringIO()
        self.addCleanup(setattr, sys, "stdout", orig_stdout)
        tr = self.get_transmogrifier()
        items = [{"b": "Été", "a": 1, "file": "content"}, {"skip": 1}]
        # default: file key removed, keys sorted, no separator, printed whatever the part
        options = {"condition": "python:'skip' not in item"}
        self.assertEqual(list(PrintItem(tr, "z__print", options, iter(items))), items)
        self.assertEqual(out.getvalue(), "a: 1\nb: Été\n")
        # removed_keys and separator
        out.truncate(0)
        out.seek(0)
        list(PrintItem(tr, "print", {"removed_keys": "a b", "separator": "-"}, iter(items[:1])))
        self.assertEqual(out.getvalue(), "file: content\n{}\n".format("-" * 30))

    def test_replace_variables(self):
        data = {"e_var": {"name": {"v": "World"}, "a": {"v": "%b%"}, "b": {"v": "B"}}}
        tr = self.get_transmogrifier(data=data)
        options = {"bp_key": "e_var", "dic_value_key": "v", "delimiter": "%", "modify_dict": "0", "yield": "1"}
        # item value modified, nested variables, unknown variable, lonely delimiter
        items = [{"_value": "Hello %name% %a%, %unknown% 5%"}, {"_value": "nothing"}]
        self.assertEqual(
            list(ReplaceVariables(tr, "a__rv", options, iter(items))),
            [{"_value": "Hello World B, !unknown! 5%"}, {"_value": "nothing"}],
        )
        self.assertEqual(dict(tr.storage["course"]), {"a__rv": 2})
        self.assertEqual(
            self.messages(),
            [
                (
                    "dti",
                    "ERROR",
                    'a__rv: variable "unknown" not found in dic from orig value "Hello %name% %a%, %unknown% 5%"',
                )
            ],
        )
        # dict modified, not yielded, condition, item_value_key and store_key
        options.update(
            {
                "modify_dict": "1",
                "yield": "0",
                "item_value_key": "val",
                "store_key": "key",
                "condition": "python:'skip' not in item",
            }
        )
        items = [{"key": "a", "val": "%b%"}, {"key": "name", "val": "%b%", "skip": 1}]
        self.assertEqual(list(ReplaceVariables(tr, "a__rv", options, iter(items))), [items[1]])
        self.assertEqual(data["e_var"], {"name": {"v": "World"}, "a": {"v": "B"}, "b": {"v": "B"}})
        self.assertEqual(items[0], {"key": "a", "val": "%b%"})
        # b_condition false, no data or not in part: unchanged
        options = {"bp_key": "e_var", "dic_value_key": "v", "delimiter": "%", "modify_dict": "0", "yield": "1"}
        bp = ReplaceVariables(tr, "a__rv", dict(options, b_condition="python:'e_xx' in storage['data']"), None)
        bp.previous = [{"_value": "%name%"}]
        self.assertEqual(list(bp), [{"_value": "%name%"}])
        bp = ReplaceVariables(tr, "a__rv", dict(options, bp_key="e_xx"), iter([{"_value": "%name%"}]))
        self.assertEqual(list(bp), [{"_value": "%name%"}])
        bp = ReplaceVariables(tr, "b__rv", options, iter([{"_value": "%name%"}]))
        self.assertEqual(list(bp), [{"_value": "%name%"}])

    def test_short_log(self):
        tr = self.get_transmogrifier()
        items = [
            {"_bpk": "e_mail", "_eid": "1", "_type": "dmsincomingmail", "_act": "N", "_path": "/im/1"},
            {"_bpk": "e_mail", "_type": "other", "title": "Été"},
        ]
        self.assertEqual(list(ShortLog(tr, "log", {}, iter(items))), items)
        tr.storage["count"] = {"commit_count": {"": {"c": 3}}}
        list(ShortLog(tr, "log", {}, iter(items[:1])))
        self.assertEqual(
            self.messages(),
            [
                ("dto", "INFO", "e_mail:1,IM,N,/im/1"),
                ("dto", "INFO", "e_mail:,other,?,Été"),
                ("dto", "INFO", "3:e_mail:1,IM,N,/im/1"),
            ],
        )

    def test_stop(self):
        tr = self.get_transmogrifier()
        options = {"condition": "python:item['_eid'] == u'2'"}
        bp = iter(Stop(tr, "stop", options, iter([{"_eid": "1"}, {"_eid": "2"}])))
        self.assertEqual(next(bp), {"_eid": "1"})
        self.assertRaisesRegex(Exception, "^STOP requested$", next, bp)

    def test_time_display(self):
        tr = self.get_transmogrifier()
        tr.storage["o_logger"] = o_logger
        items = [{"_eid": "1"}, {"_eid": "2"}]
        # default condition: False
        self.assertEqual(list(TimeDisplay(tr, "time", {}, iter(items))), items)
        self.assertEqual(self.messages(), [])
        options = {"condition": "python:item['_eid'] == u'2'"}
        self.assertEqual(list(TimeDisplay(tr, "time", options, iter(items))), items)
        self.assertEqual(len(self.messages()), 1)
        self.assertTrue(re.match(r"^\d\d:\d\d:\d\d\.\d{6} : time, 2$", self.messages()[0][2]), self.messages())
