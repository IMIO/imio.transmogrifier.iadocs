# -*- coding: utf-8 -*-
from AccessControl.SecurityManagement import newSecurityManager
from collective.transmogrifier.transmogrifier import configuration_registry
from collective.transmogrifier.transmogrifier import Transmogrifier
from imio.helpers.security import setup_logger
from imio.pyutils.system import stop
from imio.transmogrifier.iadocs import logger

import argparse
import json
import os
import sys
import transaction


PIPELINE_ID = 'imio.transmogrifier.iadocs.pipeline'
USAGE = """
Usage : bin/instance run -O{plonepath} \
src/imio.transmogrifier.iadocs/src/imio/transmogrifier/iadocs/execute_pipeline.py \
{PILELINE_FILE} -h
"""

# setup_logger(20)


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
parser.add_argument('-c', '--commit', dest='commit', choices=('0', '1'), help='To commit changes (0, 1)')
ns = parser.parse_args()
if not os.path.exists(ns.pipeline):
    stop("Given pipeline file '{}' doesn't exist".format(ns.pipeline), logger=logger)
if ns.commit is None or ns.commit == '0':
    ns.commit = False
else:
    ns.commit = True
options = {'commit': ns.commit}
portal = obj  # noqa
portal.REQUEST.set('_transmo_options_', json.dumps(options))
# get admin user
acl_users = app.acl_users  # noqa
user = acl_users.getUser('admin')
if user:
    user = user.__of__(acl_users)
    newSecurityManager(None, user)
else:
    logger.error("Cannot find admin user ")

execute_pipeline(portal, ns.pipeline)

if ns.commit:
    transaction.commit()
