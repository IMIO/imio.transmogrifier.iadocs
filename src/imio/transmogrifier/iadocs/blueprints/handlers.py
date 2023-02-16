# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from imio.helpers.transmogrifier import filter_keys
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs.utils import get_part
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import get_values_string
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from Products.CMFPlone.utils import safe_unicode
from zope.annotation import IAnnotations
from zope.interface import classProvides
from zope.interface import implements


class AServiceUpdate(object):
    """Update plonegroup services with external id value.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.all_orgs = self.storage['data']['p_orgs_all']
        self.eid_to_orgs = self.storage['data']['p_eid_to_orgs']
        self.match = self.storage['data']['e_service_match']

    def __iter__(self):
        change = False
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                if item['_eid'] not in self.eid_to_orgs:  # eid not yet in Plone
                    if item['_eid'] in self.match:
                        uid = self.match[item['_eid']]['uid']
                        if not uid or uid not in self.all_orgs:
                            log_error(item, u"Cannot find uid '{}' matched with service".format(uid))
                            continue
                        item['_type'] = 'organization'
                        item['_path'] = self.all_orgs[uid]['p']
                        item['internal_number'] = item['_eid']
                        change = True
                    else:
                        log_error(item, u"Not in matching file")
                        continue
            yield item
        # store services after plonegroup changes
        if change:
            self.storage['data']['p_orgs_all'], self.storage['data']['p_eid_to_orgs'] = get_plonegroup_orgs(self.portal)
            print(self.storage['data']['p_eid_to_orgs'])


class BMailtypesByType(object):
    """Modify mailtypes items following use.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item, storage=self.storage):
                for _etype in self.storage['data']['e_used_mailtype'][item['_eid']]:
                    item['_etype'] = _etype
                    yield item
                continue
            yield item


class StoreInData(object):
    """Store items in a dictionary.

    Parameters:
        * ext_type = M, external type string representing csv
        * store_key = M, storing keys for item. If defined, the item is stored in storage[{ext_type}][{store_key}]
        * store_subkey = O, storing sub keys for item. If defined, the item is stored in
          storage[{ext_type}][{store_key}][{store_subkey}]
        * fieldnames = O, fieldnames to store. All if nothing.
        * condition = O, condition expression
        * yield = O, flag to know if a yield must be done (0 or 1: default 0)
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        # self.store_key = safe_unicode(options['store_key']).split()
        self.store_key = safe_unicode(options['store_key'])
        self.store_subkey = safe_unicode(options.get('store_subkey'))
        self.ext_type = safe_unicode(options['ext_type'])
        self.fieldnames = safe_unicode(options.get('fieldnames') or '').split()
        self.yld = bool(int(options.get('yield') or '0'))
        if self.ext_type not in self.storage['data']:
            self.storage['data'][self.ext_type] = {}

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                # if not self.fieldnames and item['_etyp'] == self.ext_type:
                #     del item['_etyp']
                # key = get_values_string(item, self.store_key)
                key = item.pop(self.store_key)
                if self.store_subkey:
                    subkey = item.get(self.store_subkey)
                    self.storage['data'][self.ext_type].setdefault(key, {})[subkey] = filter_keys(item, self.fieldnames)
                else:
                    self.storage['data'][self.ext_type][key] = filter_keys(item, self.fieldnames)
                if not self.yld:
                    continue
            yield item
