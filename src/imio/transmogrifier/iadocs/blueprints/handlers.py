# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs.utils import get_part
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from zope.annotation import IAnnotations
from zope.interface import classProvides
from zope.interface import implements


class ServiceUpdate(object):
    """Initializes global variables to be used in next sections.

    Parameters:
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
        self.all_orgs = self.storage['data']['p_orgs_all']
        self.eid_to_orgs = self.storage['data']['p_eid_to_orgs']
        self.match = self.storage['data']['e_service_match']

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, 'a'):
                if item['_eid'] not in self.eid_to_orgs:  # eid not yet in Plone
                    if item['_eid'] in self.match:
                        uid = self.match[item['_eid']]['uid']
                        if not uid or uid not in self.all_orgs:
                            log_error(item, u"Cannot find uid '{}' matched with service".format(uid))
                            continue
                        item['_type'] = 'organization'
                        item['_path'] = self.all_orgs[uid]['p']
                        item['internal_number'] = item['_eid']
                    else:
                        log_error(item, u"Not in matching file")
                        continue
            yield item
        # store services after plonegroup changes
        self.storage['data']['p_orgs_all'], self.storage['data']['p_eid_to_orgs'] = get_plonegroup_orgs(self.portal)
