# -*- coding: utf-8 -*-
from AccessControl.SecurityManagement import newSecurityManager
from collective.transmogrifier.transmogrifier import _load_config
from collective.transmogrifier.transmogrifier import configuration_registry
from collective.transmogrifier.transmogrifier import Transmogrifier
# from imio.helpers.security import setup_logger
from imio.pyutils.system import stop
from imio.transmogrifier.iadocs import logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import get_related_parts

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

if 'app' not in locals() or 'obj' not in locals():
    stop("This script must be run via 'bin/instance -Oxxx run' !")


def main():
    args = sys.argv
    if len(args) < 3 or args[1] != '-c' or not args[2].endswith('execute_pipeline.py'):
        stop("Arguments are not formatted as needed. Has the script been run via 'instance run'? "
             "Args are '{}'".format(args), logger=logger)
    args.pop(1)  # remove -c
    args.pop(1)  # remove script name
    parser = argparse.ArgumentParser(description='Run ia.docs data transfer.')
    parser.add_argument('pipeline', help='Pipeline file')
    parser.add_argument('-c', '--commit', dest='commit', choices=('0', '1'), help='To commit changes (0, 1)')
    parser.add_argument('-p', '--parts', dest='parts', default='', help='Parts to run (abc...)')
    ns = parser.parse_args()
    if not os.path.exists(ns.pipeline):
        stop("Given pipeline file '{}' doesn't exist".format(ns.pipeline), logger=logger)
    if ns.commit is None or ns.commit == '0':
        ns.commit = False
    else:
        ns.commit = True

    batch_nb = int(os.getenv('BATCH', '0'))
    commit_nb = int(os.getenv('COMMIT', '0'))
    func_part = os.getenv('FUNC_PART', '')

    options = {'commit': ns.commit, 'parts': ns.parts, 'batch_nb': batch_nb, 'commit_nb': commit_nb}

    portal = obj  # noqa
    # get admin user
    acl_users = app.acl_users  # noqa
    user = acl_users.getUser('admin')
    if user:
        user = user.__of__(acl_users)
        newSecurityManager(None, user)
    else:
        logger.error("Cannot find admin user ")

    try:
        configuration_registry.getConfiguration(PIPELINE_ID)
    except KeyError:
        configuration_registry.registerConfiguration(PIPELINE_ID, u'', u'', ns.pipeline)
    #     try:
    options['parts'] = auto_parts(ns, func_part)
    o_logger.info(options)
    portal.REQUEST.set('_transmo_options_', json.dumps(options))
    transmogrifier = Transmogrifier(portal)
    transmogrifier(PIPELINE_ID)
    #     except Exception as error:
    #         error_msg = u"type: '{}', msg: '{}'".format(type(error), error)
    #         to_send = [u'Critical error during pipeline: {}'.format(error_msg)]
    # #        send_report(portal, to_send)
    #         raise error

    if ns.commit:
        transaction.commit()


def auto_parts(ns, func_part):
    """Get linked parts following main part and pipeline"""
    if not func_part:
        return ns.parts
    config = _load_config(PIPELINE_ID, seen=None)
    sections = [sec for sec in config['transmogrifier']['pipeline'].splitlines() if sec]
    needed = ''
    for section in sections:
        # we consider sections starting with func_part __
        if config[section]['blueprint'] == 'imio.transmogrifier.iadocs.need_other' and \
                func_part in get_related_parts(section) or []:
            needed = config[section]['parts']
            break
    return ''.join(sorted(set(needed + ns.parts + func_part)))


if __name__ == '__main__':
    main()
