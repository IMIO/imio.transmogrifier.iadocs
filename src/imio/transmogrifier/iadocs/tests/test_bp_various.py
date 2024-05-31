# -*- coding: utf-8 -*-
"""Blueprints tests for this package."""
from imio.transmogrifier.iadocs.blueprints.various import EnhancedCondition
from imio.transmogrifier.iadocs.blueprints.various import EnhancedInserter
from imio.transmogrifier.iadocs.testing import get_storage
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING  # noqa

import unittest


class TestBluePrintVarious(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.portal.context = self.portal
        self.storage = get_storage(self.portal)

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

    def test_enhanced_inserter(self):
        # minimal options
        bp = EnhancedInserter(self.portal, "", {"key": "string:aa", "value": "python:u'bb'"}, None)
        bp.previous = [{}]
        self.assertDictEqual(next(iter(bp)), {"aa": u"bb"})
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
        self.assertDictEqual(next(iter(bp)), {"aa": u"bb"})
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
        self.assertDictEqual(next(iter(bp)), {"aa": u"fallback"})
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
        self.assertDictEqual(next(iter(bp)), {"aa": u"bb"})
        # with separator, with initial value, without value
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u''", "separator": "python:u'\\r\\n'"}, None
        )
        bp.previous = [{"aa": u"one"}]
        self.assertDictEqual(next(iter(bp)), {"aa": u"one"})
        # with separator, with initial value, with value
        bp = EnhancedInserter(
            self.portal, "", {"key": "string:aa", "value": "python:u'bb'", "separator": "python:u'\\r\\n'"}, None
        )
        bp.previous = [{"aa": u"one"}]
        self.assertDictEqual(next(iter(bp)), {"aa": u"one\r\nbb"})
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
        self.assertDictEqual(next(iter(bp)), {"_path": "portal_workflow", "aa": u"bb"})
