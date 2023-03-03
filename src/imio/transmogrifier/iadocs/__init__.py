# -*- coding: utf-8 -*-
"""Init and utils."""
from zope.i18nmessageid import MessageFactory

import logging

# types shortcuts
T_S = {'dmsincomingmail': 'IM', 'dmsincoming_email': 'IE', 'dmsoutgoingmail': 'OM'}

logger = logging.getLogger('dt')
logger.setLevel(logging.INFO)  # needed to be displayed with instance run

e_logger = logging.getLogger('dt-input')
e_logger.setLevel(logging.INFO)

o_logger = logging.getLogger('dt-output')
o_logger.setLevel(logging.INFO)

ANNOTATION_KEY = 'imio.transmogrifier.iadocs'

_ = MessageFactory('imio.transmogrifier.iadocs')


def initialize(context):
    """Initializer called when used as a Zope 2 product."""
