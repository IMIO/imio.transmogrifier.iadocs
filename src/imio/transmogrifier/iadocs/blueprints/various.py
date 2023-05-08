# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from collective.transmogrifier.utils import Expression
from imio.helpers.transmogrifier import get_obj_from_path  # noqa
from imio.helpers.transmogrifier import key_val as dim
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs import T_S
from imio.transmogrifier.iadocs.utils import course_store
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import print_item  # noqa
from Products.CMFPlone.utils import safe_unicode
from zope.annotation.interfaces import IAnnotations
from zope.interface import classProvides
from zope.interface import implements

import ipdb


class Breakpoint(object):
    """Stops with ipdb if condition is matched.

    Parameters:
        * condition = M, matching condition.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        condition = options.get('condition') or 'python:False'
        self.condition = Condition(condition, transmogrifier, name, options)
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                # ipdb.set_trace(sys._getframe().f_back)  # Break!
                ipdb.set_trace()  # Break!
            yield item


class Count(object):
    """Count items.

    Parameters:
        * group_key = M, item key to group counting.
        * condition = O, matching condition.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        if 'count' not in self.storage:
            self.storage['count'] = {}
        self.storage['count'][name] = {}
        self.group_key = safe_unicode(options['group_key'])

    def __iter__(self):
        counter = self.storage['count'][self.name]
        for item in self.previous:
            if self.condition(item):
                course_store(self)
                if self.group_key:
                    if self.group_key in item:
                        counter.setdefault(item[self.group_key], {'c': 0})['c'] += 1
                    else:
                        counter.setdefault('{}_missing'.format(self.group_key), {'c': 0})['c'] += 1
                else:
                    counter.setdefault('', {'c': 0})['c'] += 1
            yield item
        for group in counter:
            o_logger.info("{} '{}' = {}".format(self.name, group, counter[group]['c']))


class EnhancedCondition(object):
    """Yield item if condition2 is matched, only applied if condition1 is matched.
    Pass storage and item in condition.

    Parameters:
        * condition = M, matching condition.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.condition1 = Condition(options['condition1'], transmogrifier, name, options)
        self.condition2 = Condition(options['condition2'], transmogrifier, name, options)
        self.previous = previous
        self.name = name
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition1(item, storage=self.storage):
                course_store(self)
                if self.condition2(item, storage=self.storage):
                    yield item
            else:
                yield item


class EnhancedInserter(object):
    """Set or append value in key, if condition is matched.

    Parameters:
        * key = M, key value expression
        * value = M, value expression
        * condition = O, matching condition
        * separator = O, separator expression: if specified, the value is appended after the separator
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.key = Expression(options['key'], transmogrifier, name, options)
        self.value = Expression(options['value'], transmogrifier, name, options)
        self.condition = Condition(options.get('condition', 'python:True'), transmogrifier, name, options)
        if options.get('separator'):
            self.separator = Expression(options.get('separator', ''), transmogrifier, name, options)(None)
        else:
            self.separator = None

    def __iter__(self):
        for item in self.previous:
            key = self.key(item)
            if self.condition(item, key=key, storage=self.storage):
                course_store(self)
                value = self.value(item, key=key, storage=self.storage)
                if self.separator and item.get(key):  # with self.separator, we append
                    if value:
                        item[key] += u'{}{}'.format(self.separator, value)
                else:
                    item[key] = value
            yield item


class NeedOther(object):
    """Stops if needed other part or section is not there.

    Parameters:
        * parts = O, needed parts (one letter list).
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        this_part = get_related_parts(name)
        if not is_in_part(self, this_part):
            return
        course_store(self)
        needed_parts = safe_unicode(options.get('parts') or u'')
        for needed_part in needed_parts:
            if not is_in_part(self, needed_part):
                raise Exception("STOPPED because '{}' part needs '{}' part to be included".format(this_part,
                                                                                                  needed_part))

    def __iter__(self):
        for item in self.previous:
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
            to_print = u"{}:{},{},{},{}".format(item['_bpk'], item.get('_eid', ''), dim(item.get('_type', ''), T_S),
                                                item.get('_act', '?'), item.get('_path', '') or item.get('title', ''))
            # print(to_print, file=sys.stderr)
            o_logger.info(to_print)
            yield item


class Stop(object):
    """Stops if condition is matched.

    Parameters:
        * condition = M, matching condition.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.condition = Condition(options['condition'], transmogrifier, name, options)
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                raise Exception('STOP requested')
            yield item
