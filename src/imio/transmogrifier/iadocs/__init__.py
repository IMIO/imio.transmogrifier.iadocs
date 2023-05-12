# -*- coding: utf-8 -*-
"""Init and utils."""
from imio.pyutils.utils import setup_logger
from zope.i18nmessageid import MessageFactory

import logging


# types shortcuts
T_S = {'dmsincomingmail': 'IM', 'dmsincoming_email': 'IE', 'dmsoutgoingmail': 'OM'}

# logging.basicConfig(format='%(asctime)s %(name)s %(levelname).1s %(message)s',
#                     datefmt='%y%m%d %H:%M:%S',
#                     )
root_logger = logging.getLogger()
root_handler = root_logger.handlers[0]
log_format = '%(asctime)s %(levelname).1s (%(name)s) %(message)s'
root_handler.setFormatter(logging.Formatter(log_format))
root_handler.formatter.datefmt = '%y%m%d %H%M%S'

logger = logging.getLogger('dt')
setup_logger(logger)   # needed to be displayed with instance run.

e_logger = logging.getLogger('dti')
setup_logger(e_logger)

o_logger = logging.getLogger('dto')
setup_logger(o_logger)

ANNOTATION_KEY = 'imio.transmogrifier.iadocs'

_ = MessageFactory('imio.transmogrifier.iadocs')


def initialize(context):
    """Initializer called when used as a Zope 2 product."""
