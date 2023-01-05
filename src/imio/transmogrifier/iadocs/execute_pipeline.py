# -*- coding: utf-8 -*-
from AccessControl.SecurityManagement import newSecurityManager
from collective.transmogrifier.transmogrifier import configuration_registry
from collective.transmogrifier.transmogrifier import Transmogrifier
from imio.helpers.security import setup_logger
from imio.pyutils.system import stop
from imio.transmogrifier.iadocs import logger
from Testing import makerequest
from zope.component.hooks import setSite
from zope.globalrequest import setRequest

import argparse
import json
import os
import sys
import transaction


PIPELINE_ID = 'imio.transmogrifier.iadocs.pipeline'
USAGE = """
Usage : bin/instance run \
src/imio.transmogrifier.iadocs/src/imio/transmogrifier/iadocs/execute_pipeline.py \
PILELINE_FILE PLONE_ID COMMIT_0_1
"""


def execute_pipeline(portal, filepath):
    try:
        configuration_registry.getConfiguration(PIPELINE_ID)
    except KeyError:
        configuration_registry.registerConfiguration(PIPELINE_ID, u'', u'', filepath)
    try:
        transmogrifier = Transmogrifier(portal)
        transmogrifier(PIPELINE_ID)
    except Exception as error:
        error_msg = u"type: '{}', msg: '{}'".format(type(error), error)
        to_send = [u'Critical error during pipeline: {}'.format(error_msg)]
#        send_report(portal, to_send)
        raise error


if 'app' not in locals() or 'obj' not in locals():
    stop("This script must be run via 'bin/instance -Oxxx run' !")

args = sys.argv
if len(args) < 3 or args[1] != '-c' or not args[2].endswith('execute_pipeline.py'):
    stop("Arguments are not formatted as needed. Has the script been run via 'instance run'? "
         "Args are '{}'".format(args), logger=logger)
args.pop(1)  # remove -c
args.pop(1)  # remove script name
parser = argparse.ArgumentParser(description='Run ia.docs data transfer.')
parser.add_argument('pipeline', help='Pipeline file')
parser.add_argument('-c', '--commit', action='store_true', dest='commit', help='To apply changes')
ns = parser.parse_args()
if not os.path.exists(ns.pipeline):
    stop("Given pipeline file '{}' doesn't exist".format(ns.pipeline), logger=logger)
portal = obj  # noqa
options = {'commit': ns.commit}
portal.REQUEST.set('_transmo_options_', json.dumps(options))
execute_pipeline(portal, ns.pipeline)
if ns.commit:
    transaction.commit()
# TODO must check if portal is correct
# TODO must pass options to transmogrifier

# plone_id = sys.argv[4]
# app = locals().get('app')
# # plone_id can be 'folder/plone'
# root = app
# for pid in plone_id.split('/'):
#     portal = root.get(pid)
#     root = portal
# setSite(portal)
# acl_users = app.acl_users
# user = acl_users.getUser('admin')
# if user:
#     user = user.__of__(acl_users)
#     newSecurityManager(None, user)
# else:
#     logger.error("Cannot find admin user ")
# app = makerequest.makerequest(app)
# # support plone.subrequest
# app.REQUEST['PARENTS'] = [app]
# setRequest(app.REQUEST)
# # can be used to increase temporary run verbosity
# # setup_logger(20)
#
# portal.REQUEST.set('_pipeline_commit_', commit)
# execute_pipeline(portal, pipeline_filepath)
