"""execute_pipeline script tests."""

from AccessControl.SecurityManagement import getSecurityManager
from argparse import Namespace
from collective.transmogrifier.transmogrifier import configuration_registry
from imio.transmogrifier.iadocs.testing import IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING
from plone.base.utils import safe_text
from zope.testing.loggingsupport import InstalledHandler

import imio.transmogrifier.iadocs
import json
import os
import shutil
import sys
import tempfile
import unittest


SCRIPT = os.path.join(os.path.dirname(imio.transmogrifier.iadocs.__file__), "execute_pipeline.py")
PIPELINE_ID = "imio.transmogrifier.iadocs.pipeline"
PIPELINE = """[config]
permitted_sections = abcelmz

[transmogrifier]
pipeline =
    init
#    l__need
    lm__need
    l__other_need
    q__need

[init]
blueprint = imio.transmogrifier.iadocs.init

[l__need]
blueprint = imio.transmogrifier.iadocs.need_other
parts = xyz

[lm__need]
blueprint = imio.transmogrifier.iadocs.need_other
parts = cab

[l__other_need]
blueprint = imio.transmogrifier.iadocs.need_other
parts = e

[q__need]
blueprint = imio.transmogrifier.iadocs.need_other
parts =
"""


def load_script(path, **globs):
    """Executes a script body, as `instance run` does, and returns its namespace (kept alive with its functions)."""
    namespace = dict(globs, __name__=os.path.basename(path)[:-3], __file__=path)
    with open(path, "rb") as fh:
        exec(compile(fh.read(), path, "exec"), namespace)
    return namespace


def unregister_pipeline():
    if PIPELINE_ID in configuration_registry._config_info:
        del configuration_registry._config_info[PIPELINE_ID]
        configuration_registry._config_ids.remove(PIPELINE_ID)


def restore_pipeline(old):
    unregister_pipeline()
    if old:
        configuration_registry.registerConfiguration(old["id"], old["title"], old["description"], old["configuration"])


class TestExecutePipeline(unittest.TestCase):

    layer = IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING

    def setUp(self):
        self.portal = self.layer["portal"]
        self.tmp = safe_text(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp)
        self.addCleanup(restore_pipeline, configuration_registry._config_info.get(PIPELINE_ID))
        unregister_pipeline()
        self.logs = InstalledHandler("dt", "dto")
        self.addCleanup(self.logs.uninstall)
        self.namespace = load_script(SCRIPT, app=self.layer["app"], obj=self.portal)

    def write_pipeline(self, content, register=True):
        path = os.path.join(self.tmp, "pipeline.cfg")
        with open(path, "w", encoding="utf8") as fh:
            fh.write(content)
        if register:
            unregister_pipeline()
            configuration_registry.registerConfiguration(PIPELINE_ID, "", "", path)
        return path

    def set_env(self, **env):
        for key, value in env.items():
            if key in os.environ:
                self.addCleanup(os.environ.__setitem__, key, os.environ[key])
            else:
                self.addCleanup(os.environ.pop, key, None)
            os.environ[key] = value

    def run_main(self, *args):
        self.addCleanup(setattr, sys, "argv", sys.argv)
        sys.argv = ["bin/instance", "-c", SCRIPT] + list(args)
        return self.namespace["main"]()

    def test_module(self):
        # the script must be run by instance run (app and obj defined)
        self.assertRaises(SystemExit, load_script, SCRIPT)
        self.assertEqual(self.namespace["PIPELINE_ID"], PIPELINE_ID)

    def test_auto_parts(self):
        auto_parts = self.namespace["auto_parts"]
        self.write_pipeline(PIPELINE)
        # first need_other section of the func part (commented section ignored), sorted unique letters
        self.assertEqual(auto_parts(Namespace(parts="z"), "l"), ("abclz", []))
        self.assertEqual(auto_parts(Namespace(parts="a"), "m"), ("abcm", []))
        # func part without needed parts: unpermitted part
        self.assertEqual(auto_parts(Namespace(parts=""), "q"), ("q", ["q"]))
        # func part without need_other section
        self.assertEqual(auto_parts(Namespace(parts="ba"), "e"), ("abe", []))
        # without func part, the parts are kept as given
        self.assertEqual(auto_parts(Namespace(parts="bax"), ""), ("bax", ["x"]))
        # no permitted sections
        self.write_pipeline(PIPELINE.replace("permitted_sections = abcelmz", ""))
        self.assertEqual(auto_parts(Namespace(parts="x"), "l"), ("abclx", []))
        # Plone 4 behaviour (bug?): a need_other section without parts prefix fails
        self.write_pipeline(
            PIPELINE.replace("    init\n", "    init\n    need\n") + "\n[need]\nblueprint = "
            "imio.transmogrifier.iadocs.need_other\nparts = a\n"
        )
        self.assertRaises(TypeError, auto_parts, Namespace(parts=""), "l")

    def test_main(self):
        pipeline = self.write_pipeline(
            "[config]\npermitted_sections = blz\n\n[transmogrifier]\npipeline =\n", register=False
        )
        self.set_env(FUNC_PART="l", BATCH="5", COMMIT="2")
        self.run_main(pipeline, "-c0", "-p", "zb")
        self.assertEqual(
            json.loads(self.portal.REQUEST.get("_transmo_options_")),
            {"commit": False, "parts": "blz", "batch_nb": 5, "commit_nb": 2},
        )
        self.assertEqual(configuration_registry.getConfiguration(PIPELINE_ID)["configuration"], pipeline)
        self.assertEqual(getSecurityManager().getUser().getId(), "admin")
        self.assertEqual(len(self.logs.records), 1)
        self.assertEqual(self.logs.records[0].name, "dto")
        # Plone 4 behaviour: an already registered pipeline id keeps its first file
        other = os.path.join(self.tmp, "other.cfg")
        open(other, "w").close()
        self.set_env(FUNC_PART="", BATCH="0", COMMIT="0")
        self.run_main(other, "-p", "b")
        self.assertEqual(configuration_registry.getConfiguration(PIPELINE_ID)["configuration"], pipeline)
        self.assertEqual(
            json.loads(self.portal.REQUEST.get("_transmo_options_")),
            {"commit": False, "parts": "b", "batch_nb": 0, "commit_nb": 0},
        )
        # unpermitted part
        self.set_env(FUNC_PART="x")
        self.assertRaises(SystemExit, self.run_main, pipeline, "-c0")
        # unexisting pipeline file
        self.logs.clear()
        self.assertRaises(SystemExit, self.run_main, os.path.join(self.tmp, "unknown.cfg"), "-c0")
        self.assertEqual(
            [rec.getMessage() for rec in self.logs.records],
            ["Given pipeline file '{}' doesn't exist".format(os.path.join(self.tmp, "unknown.cfg"))],
        )
        # bad commit value
        self.assertRaises(SystemExit, self.run_main, pipeline, "-c2")
        # not run by instance run
        self.logs.clear()
        self.addCleanup(setattr, sys, "argv", sys.argv)
        sys.argv = ["bin/python", pipeline]
        self.assertRaises(SystemExit, self.namespace["main"])
        self.assertEqual(len(self.logs.records), 1)
        self.assertTrue(
            self.logs.records[0].getMessage().startswith("Arguments are not formatted as needed."),
            self.logs.records[0].getMessage(),
        )
