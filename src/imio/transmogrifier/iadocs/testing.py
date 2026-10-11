# -*- coding: utf-8 -*-
from collections import OrderedDict
from collective.transmogrifier.transmogrifier import configuration_registry
from collective.transmogrifier.transmogrifier import Transmogrifier
from imio.dms.mail.testing import DMSMAIL_FIXTURE
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from plone.app.testing import FunctionalTesting
from plone.app.testing import IntegrationTesting
from plone.app.testing import PLONE_FIXTURE
from plone.app.testing import PloneSandboxLayer
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from zope.annotation import IAnnotations

import collective.behavior.internalnumber
import imio.transmogrifier.iadocs


class ImioTransmogrifierIadocsLayer(PloneSandboxLayer):

    defaultBases = (PLONE_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        # Load any other ZCML that is required for your tests.
        # The z3c.autoinclude feature is disabled in the Plone fixture base
        # layer.
        self.loadZCML(package=imio.transmogrifier.iadocs, name="configure.zcml")

    def setUpPloneSite(self, portal):
        # applyProfile(portal, 'imio.transmogrifier.iadocs:testing')
        setRoles(portal, TEST_USER_ID, ["Manager"])

    def tearDownZope(self, app):
        clear_configuration_registry()


class ImioTransmogrifierIadocsDmsLayer(PloneSandboxLayer):
    """An ia.docs site (imio.dms.mail fixture, with its example content) and the blueprints."""

    defaultBases = (DMSMAIL_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        # its profile is installed by the Initialization section (internal_number_behavior_types)
        self.loadZCML(package=collective.behavior.internalnumber)
        self.loadZCML(package=imio.transmogrifier.iadocs, name="configure.zcml")

    def tearDownZope(self, app):
        clear_configuration_registry()


def clear_configuration_registry():
    """The pipeline configurations registered by ZCML (transmogrifier:registerConfig) live in a global registry
    that only zope.testing's cleanUp clears: the next layer loading the same ZCML would get duplicates."""
    configuration_registry.clear()


def get_storage(portal):
    annot = IAnnotations(portal)
    return annot.setdefault(ANNOTATION_KEY, {"course": OrderedDict()})


def get_transmogrifier(portal, config=None, pipeline=u"", storage=None):
    """A Transmogrifier whose sections are given as dicts, as built by a pipeline file.

    :param config: options of the [config] section
    :param pipeline: pipeline option of the [transmogrifier] section
    :param storage: initial storage (the Initialization section builds it in a real run)
    :return: the transmogrifier, its storage is also set as `storage` attribute
    """
    transmogrifier = Transmogrifier(portal)
    transmogrifier._raw = {"config": dict(config or {}), "transmogrifier": {"pipeline": pipeline}}
    transmogrifier._data = {}
    transmogrifier.storage = IAnnotations(transmogrifier).setdefault(
        ANNOTATION_KEY, storage if storage is not None else {"course": OrderedDict(), "data": {}, "csv": {}}
    )
    return transmogrifier


IMIO_TRANSMOGRIFIER_IADOCS_FIXTURE = ImioTransmogrifierIadocsLayer()


IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING = IntegrationTesting(
    bases=(IMIO_TRANSMOGRIFIER_IADOCS_FIXTURE,), name="ImioTransmogrifierIadocsLayer:IntegrationTesting"
)


IMIO_TRANSMOGRIFIER_IADOCS_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(IMIO_TRANSMOGRIFIER_IADOCS_FIXTURE,), name="ImioTransmogrifierIadocsLayer:FunctionalTesting"
)


IMIO_TRANSMOGRIFIER_IADOCS_DMS_FIXTURE = ImioTransmogrifierIadocsDmsLayer()


IMIO_TRANSMOGRIFIER_IADOCS_DMS_INTEGRATION_TESTING = IntegrationTesting(
    bases=(IMIO_TRANSMOGRIFIER_IADOCS_DMS_FIXTURE,), name="ImioTransmogrifierIadocsDmsLayer:IntegrationTesting"
)
