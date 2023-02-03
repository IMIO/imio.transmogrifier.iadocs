# -*- coding: utf-8 -*-

from __future__ import print_function
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs import ANNOTATION_KEY
# from imio.transmogrifier.iadocs.utils import shortcut
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from zope.annotation.interfaces import IAnnotations
from zope.interface import classProvides
from zope.interface import implements

import ipdb
# import sys


class BreakpointSection(object):
    """Stops with ipdb if condition is matched.

    Parameters:
        * condition = M, matching condition.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        condition = options['condition']
        self.condition = Condition(condition, transmogrifier, name, options)
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                # ipdb.set_trace(sys._getframe().f_back)  # Break!
                ipdb.set_trace()  # Break!
            yield item


class ShortLog(object):
    """Logs shortly item."""
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            to_print = u"{}: {}, {}".format(item['_etyp'], item.get('_eid', ''), item.get('title', ''))
            # print(to_print, file=sys.stderr)
            o_logger.info(to_print)
            yield item


class StopSection(object):
    """Stops if condition is matched.

    Parameters:
        * condition = M, matching condition.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        condition = options['condition']
        self.condition = Condition(condition, transmogrifier, name, options)
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                raise Exception('STOP requested')
            yield item
