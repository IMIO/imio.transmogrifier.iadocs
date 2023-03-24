# -*- coding: utf-8 -*-
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from plone.app.testing import FunctionalTesting
from plone.app.testing import IntegrationTesting
from plone.app.testing import PLONE_FIXTURE
from plone.app.testing import PloneSandboxLayer
from plone.app.testing import setRoles
from plone.app.testing import TEST_USER_ID
from zope.annotation import IAnnotations

import imio.transmogrifier.iadocs


class ImioTransmogrifierIadocsLayer(PloneSandboxLayer):

    defaultBases = (PLONE_FIXTURE,)

    def setUpZope(self, app, configurationContext):
        # Load any other ZCML that is required for your tests.
        # The z3c.autoinclude feature is disabled in the Plone fixture base
        # layer.
        self.loadZCML(package=imio.transmogrifier.iadocs, name='configure.zcml')

    def setUpPloneSite(self, portal):
        # applyProfile(portal, 'imio.transmogrifier.iadocs:testing')
        setRoles(portal, TEST_USER_ID, ['Manager'])


def get_storage(portal):
    annot = IAnnotations(portal)
    return annot.setdefault(ANNOTATION_KEY, {})


IMIO_TRANSMOGRIFIER_IADOCS_FIXTURE = ImioTransmogrifierIadocsLayer()


IMIO_TRANSMOGRIFIER_IADOCS_INTEGRATION_TESTING = IntegrationTesting(
    bases=(IMIO_TRANSMOGRIFIER_IADOCS_FIXTURE,),
    name='ImioTransmogrifierIadocsLayer:IntegrationTesting'
)


IMIO_TRANSMOGRIFIER_IADOCS_FUNCTIONAL_TESTING = FunctionalTesting(
    bases=(IMIO_TRANSMOGRIFIER_IADOCS_FIXTURE,),
    name='ImioTransmogrifierIadocsLayer:FunctionalTesting'
)
