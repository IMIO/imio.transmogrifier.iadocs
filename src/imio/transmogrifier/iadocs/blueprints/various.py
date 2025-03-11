# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Matcher
from imio.helpers.transmogrifier import Condition
from imio.helpers.transmogrifier import Expression
from imio.helpers.transmogrifier import filter_keys
from imio.helpers.transmogrifier import get_obj_from_path  # noqa
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import course_store
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import print_item  # noqa
from imio.transmogrifier.iadocs.utils import short_log
from imio.transmogrifier.iadocs.utils import time_display
from Products.CMFPlone.utils import safe_unicode
from zope.annotation.interfaces import IAnnotations
from zope.interface import classProvides
from zope.interface import implements

import copy
import ipdb
import re


class Breakpoint(object):
    """Stops with ipdb if condition is matched.

    Parameters:
        * condition = M, matching condition.
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        condition = options.get("condition") or "python:False"
        self.condition = Condition(condition, transmogrifier, name, options)
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item, storage=self.storage):
                # ipdb.set_trace(sys._getframe().f_back)  # Break!
                ipdb.set_trace()  # Break!
            yield item


class Count(object):
    """Count items.

    Parameters:
        * group_key = O, item key to group counting.
        * condition = O, matching condition.
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.condition = Condition(options.get("condition") or "python:True", transmogrifier, name, options)
        if "count" not in self.storage:
            self.storage["count"] = {}
        self.storage["count"][name] = {}
        self.group_key = safe_unicode(options.get("group_key", u""))

    def __iter__(self):
        counter = self.storage["count"][self.name]
        for item in self.previous:
            if self.condition(item):
                course_store(self, item)
                if self.group_key:
                    if self.group_key in item:
                        counter.setdefault(item[self.group_key], {"c": 0})["c"] += 1
                    else:
                        counter.setdefault("{}_missing".format(self.group_key), {"c": 0})["c"] += 1
                else:
                    counter.setdefault("", {"c": 0})["c"] += 1
            yield item
        for group in counter:
            o_logger.info("{} '{}' = {}".format(self.name, group, counter[group]["c"]))


class EnhancedCondition(object):
    """Yield item if condition2 is matched, only applied if condition1 is matched.
    Pass storage and item in condition.

    Parameters:
        * condition1 = M, main matching condition.
        * condition2 = M, matching condition to yield item.
        * get_obj = O, flag to get object from path (default 0)
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.condition1 = Condition(options["condition1"], transmogrifier, name, options)
        self.condition2 = Condition(options["condition2"], transmogrifier, name, options)
        self.get_obj = bool(int(options.get("get_obj") or "0"))
        self.previous = previous
        self.portal = transmogrifier.context
        self.name = name
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition1(item, storage=self.storage):
                course_store(self, item)
                obj = None
                if self.get_obj:
                    obj = get_obj_from_path(self.portal, item)
                if self.condition2(item, storage=self.storage, obj=obj):
                    yield item
            else:
                yield item


class EnhancedInserter(object):
    """Set or append value in key, if condition is matched.

    Parameters:
        * key = M, key value expression
        * value = M, value expression
        * condition = O, matching condition
        * condition2 = O, secondary matching condition (with obj)
        * separator = O, separator expression: if specified, the value is appended after the separator
        * get_obj = O, flag to get object from path (default 0)
        * error = O, error message to display
        * error_value = O, value to set when error occurs
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.key = Expression(options["key"], transmogrifier, name, options)
        self.value = Expression(safe_unicode(options["value"]), transmogrifier, name, options)
        self.condition = Condition(options.get("condition", "python:True"), transmogrifier, name, options)
        self.condition2 = options.get("condition2")
        if self.condition2:
            self.condition2 = Condition(self.condition2, transmogrifier, name, options)
        self.error = Expression(
            options.get("error", 'python:u"error getting value for eid {}".format(' 'item.get("_eid"))'),
            transmogrifier,
            name,
            options,
        )
        self.get_obj = bool(int(options.get("get_obj") or "0"))
        self.error_value = safe_unicode(options.get("error_value") or u"")
        if self.error_value:
            self.error_value = Expression(self.error_value, transmogrifier, name, options)
        if options.get("separator"):
            self.separator = Expression(options.get("separator", ""), transmogrifier, name, options)(None)
        else:
            self.separator = None

    def __iter__(self):
        for item in self.previous:
            key = self.key(item)
            if self.condition(item, key=key, storage=self.storage):
                course_store(self, item)
                obj = None
                if self.get_obj:
                    obj = get_obj_from_path(self.portal, item)
                # if condition2 is defined and not matched, we yield the item
                if self.condition2 is not None and not self.condition2(item, key=key, storage=self.storage, obj=obj):
                    yield item
                    continue
                try:
                    value = self.value(item, key=key, storage=self.storage, obj=obj)
                except Exception as msg:
                    e_logger.error(u"{}: {} ({})".format(self.name, self.error(item), msg))
                    if self.error_value:
                        try:
                            value = self.error_value(item, key=key, storage=self.storage, obj=obj)
                        except Exception as msg:
                            e_logger.error(u"{}: {} ({})".format(self.name, self.error(item), msg))
                            yield item
                            continue
                    else:
                        yield item
                        continue
                if self.separator and item.get(key):  # with self.separator, we append
                    if value:
                        item[key] += u"{}{}".format(self.separator, value)
                else:
                    item[key] = value
            yield item


class EnhancedManipulator(object):
    """Manipulate item if condition is matched: copy, move or discard keys.

    Parameters:
        * keys = O, source keys. Can starts with re:
        * dest = O, destination keys. Can be expression using keys pattern matching
        * delete = O, keys to delete or move.
        * condition = O, matching condition.
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        keys = options.get("keys") or ""
        self.keys = Matcher(*keys.splitlines())
        if keys:
            self.dest = Expression(options["destination"], transmogrifier, name, options)
        self.delete = Matcher(*options.get("delete", "").splitlines())
        self.condition = Condition(options.get("condition", "python:True"), transmogrifier, name, options)
        self.previous = previous
        self.portal = transmogrifier.context
        self.name = name
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                course_store(self, item)
                for key in item.keys():
                    match = self.keys(key)[1]
                    if match:
                        dest = self.dest(item, key=key, match=match)
                        item[dest] = copy.deepcopy(item[key])
                    if self.delete(key)[1]:
                        del item[key]
            yield item


class FilterItem(object):
    """Filter item to keep only some fields. Useful when correcting...

    Parameters:
        * kept_keys = M, keys to keep
        * condition = O, condition to filter (default True)
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.condition = Condition(options.get("condition", "python:True"), transmogrifier, name, options)
        self.kept_keys = safe_unicode(options.get("kept_keys", "")).strip().split()

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.kept_keys and self.condition(item):
                course_store(self, item)
                yield filter_keys(item, self.kept_keys + [fld for fld in item if fld.startswith("_")])
                continue
            yield item


class ItemFieldSplit(object):
    """Splits an item field and yield multiple item.

    Parameters:
        * condition = O, condition to filter (default True)
        * field = M, item field
        * separator = M, separator to split field
        * part_pattern = O, pattern to check accepted part (default none)
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.condition = Condition(options.get("condition") or "python:True", transmogrifier, name, options)
        self.field = safe_unicode(options["field"])
        self.sep = safe_unicode(options["separator"])
        self.pattern = options.get("part_pattern") or u""

    def __iter__(self):
        for item in self.previous:
            # TO BE TESTED
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            if self.sep not in item.get(self.field, ""):
                yield item
                continue
            parts = item[self.field].split(self.sep)
            if self.pattern:
                error = False
                for part in parts:
                    if not re.match(self.pattern, part):
                        yield item
                        error = True
                        break
                if error:
                    continue
            model_item = item.copy()
            for part in parts:
                item = model_item.copy()
                item[self.field] = part
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
        course_store(self, None)
        needed_parts = safe_unicode(options.get("parts") or u"")
        for needed_part in needed_parts:
            if not is_in_part(self, needed_part):
                raise Exception(
                    "STOPPED because '{}' part needs '{}': missing '{}'".format(this_part, needed_parts, needed_part)
                )

    def __iter__(self):
        for item in self.previous:
            yield item


class PrintItem(object):
    """Print item without big fields.

    Parameters:
        * removed_keys = O, keys to remove (default file)
        * condition = O, condition to print (default True)
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.condition = Condition(options.get("condition", "python:True"), transmogrifier, name, options)
        self.removed_keys = safe_unicode(options.get("removed_keys", "file")).strip().split()

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                print_item(item, self.removed_keys)
            yield item


class ReplaceVariables(object):
    """Replace variable in stored dict.

    Parameters:
        * b_condition = O, blueprint condition expression (available: storage)
        * bp_key = M, blueprint key and storage dic name
        * store_key = M, storage key
        * value_key = M, storage value key
        * delimiter = M, delimiter that surrounds variable
        * modify_dict = M, flag to know if the dict must be modified (0 or 1)
        * yield = M, flag to know if a yield must be done (0 or 1)
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.bp_key = safe_unicode(options["bp_key"])
        if not is_in_part(self, self.parts):
            return
        doit = Condition(options.get("b_condition") or "python:True", transmogrifier, name, options)
        self.doit = doit(None, storage=self.storage)
        if not self.doit:
            self.values = None
            return
        self.values = self.storage["data"].get(self.bp_key)
        self.store_key = safe_unicode(options["store_key"])
        self.value_key = safe_unicode(options["value_key"])
        self.delimiter = safe_unicode(options["delimiter"])
        self.modify_dict = safe_unicode(options["modify_dict"])
        self.yld = bool(int(options["yield"]))

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.values:
                yield item
                continue
            value = item[self.value_key]
            while self.delimiter in value:
                pattern = re.escape(self.delimiter) + r'([^' + re.escape(self.delimiter) + r']+)' \
                    + re.escape(self.delimiter)
                matches = re.findall(pattern, value)
                if not matches:
                    break
                for var in matches:
                    if var in self.values:
                        value = value.replace(u"{0}{1}{0}".format(self.delimiter, var),
                                              self.values[var][self.value_key])
                    else:
                        e_logger.error(
                            u'{}: variable "{}" not found in dic from orig value "{}"'.format(self.name, var,
                                                                                              item[self.value_key]))
                        break
            if item[self.value_key] != value:
                if self.modify_dict:
                    self.values[item[self.store_key]][self.value_key] = value
                else:
                    item[self.value_key] = value
            if self.yld:
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
            o_logger.info(
                short_log(item, count=self.storage.get("count", {}).get("commit_count", {}).get("", {}).get("c", 0))
            )
            # to_print = short_log(item)
            # print(to_print, file=sys.stderr)
            yield item


class Stop(object):
    """Stops if condition is matched.

    Parameters:
        * condition = M, matching condition.
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.condition = Condition(options["condition"], transmogrifier, name, options)
        self.previous = previous
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                raise Exception("STOP requested")
            yield item


class TimeDisplay(object):
    """Displays time if condition is matched.

    Parameters:
        * condition = O, matching condition.
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.condition = Condition(options.get("condition", "python:False"), transmogrifier, name, options)
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                time_display(self, item)
            yield item
