# -*- coding: utf-8 -*-
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import correct_path
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_part
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import get_values_string
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from imio.transmogrifier.iadocs.utils import MAILTYPES
from plone import api
from plone.i18n.normalizer import IIDNormalizer
from Products.CMFPlone.utils import safe_unicode
from z3c.relationfield import RelationValue
from zope.annotation import IAnnotations
from zope.component import getUtility
from zope.interface import classProvides
from zope.interface import implements
from zope.intid import IIntIds

import os


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
                        if item['_eid'] not in self.all_orgs[uid]['eids']:
                            item['_type'] = 'organization'
                            item['_path'] = self.all_orgs[uid]['p']
                            self.all_orgs[uid]['eids'].append(item['_eid'])
                            item['internal_number'] = u','.join(self.all_orgs[uid]['eids'])
                            change = True
                    else:
                        log_error(item, u"Not in matching file")
                        continue
            yield item
        # store services after plonegroup changes
        if change:
            o_logger.info("Part a: some services have been updated.")
            self.storage['data']['p_orgs_all'], self.storage['data']['p_eid_to_orgs'] = get_plonegroup_orgs(self.portal)


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
        self.related_storage = safe_unicode(options['related_storage'])
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item, storage=self.storage):
                for _etype in self.storage['data'][self.related_storage][item['_eid']]:
                    item['_etype'] = _etype
                    yield item
                continue
            yield item


class BMailtypeUpdate(object):
    """Store mailtype if necessary.

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
        self.to_add = {}
        if not is_in_part(self, self.part):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        p_types = self.storage['data']['p_mailtype']
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item, storage=self.storage):
                #  _eid _etype _etitle _enature _esource _c_type _key _title _active
                if not item['_c_type'] and not item['_key']:
                    log_error(item, u'Empty match: we pass _eid {}'.format(item['_eid']), 'warning')
                    continue
                if item['_c_type'] not in p_types or item['_key'] not in p_types[item['_c_type']]:
                    to_add = self.to_add.setdefault(item['_c_type'], {})
                    if item['_key'] not in to_add:
                        to_add[item['_key']] = {'value': item['_key'], 'dtitle': item['_title'],
                                                'active': item['_active']}
                continue
            yield item

        if self.to_add:
            o_logger.info("Part b: adding some mail types")
            for typ in self.to_add:
                values = list(api.portal.get_registry_record(MAILTYPES[typ]))
                for key in self.to_add[typ]:
                    values.append(self.to_add[typ][key])
                api.portal.set_registry_record(MAILTYPES[typ], values)
            self.storage['data']['p_mailtype'] = get_mailtypes(self.portal)


class DOMSenderCreation(object):
    """Create sender held_position if necessary.

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
        self.change = False
        if not is_in_part(self, self.part):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.puid_to_pers = self.storage['data']['p_userid_to_pers']
        self.euid_to_pers = self.storage['data']['p_euid_to_pers']
        self.p_hps = self.storage['data']['p_hps']
        self.p_eid_orgs = self.storage['data']['p_eid_to_orgs']
        self.e_c_s = self.storage['data']['e_contacts_sender']
        self.e_u_m = self.storage['data']['e_user_match']
        self.intids = getUtility(IIntIds)

    def person(self, e_userid, item, pid, title, firstname=None, lastname=None, transitions=('deactivate',)):
        if e_userid not in self.euid_to_pers:
            # we create or update a person
            path = os.path.join(self.storage['plone']['directory_path'], 'personnel-folder/{}'.format(pid))
            pdic = {'_type': 'person', '_path': path, u'internal_number': e_userid, 'use_parent_address': False,
                    u'_transitions': transitions, u'_etyp': u'person_sender', u'_eid': item['_eid'],
                    u'_post_actions': (u'store_internal_person_info',), u'title': title}
            if firstname is not None:
                pdic[u'firstname'] = firstname
            if lastname is not None:
                pdic[u'lastname'] = lastname
            self.change = True
            return [pdic]
        return []

    def hp(self, e_userid, item, transitions=('deactivate',)):
        puid = self.euid_to_pers[e_userid]
        ouid = self.p_eid_orgs[item['_service']]
        if ouid not in self.p_hps[puid]['hps']:
            # we create a hp
            path = os.path.join(self.p_hps[puid]['path'], ouid)
            org = uuidToObject(ouid, unrestricted=True)
            hpdic = {'_type': 'held_position', '_path': path, 'use_parent_address': True,
                     'position': RelationValue(self.intids.getId(org)), 'internal_number': u'',
                     u'_transitions': transitions, u'_etyp': u'hp_sender', u'_eid': item['_eid']}
            self.p_hps[puid]['hps'][ouid] = {'path': path, 'state': 'deactivated'}
            self.change = True
            return [hpdic]
        return []

    def __iter__(self):
        idnormalizer = getUtility(IIDNormalizer)
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                if item['_sender_id'] and item['_sender_id'] in self.e_c_s:  # we have a user id
                    e_userid = self.e_c_s[item['_sender_id']]['_uid']
                    _euidm = self.e_u_m[e_userid]
                    if _euidm['_uid']:  # we have a match
                        if _euidm['_uid'] in self.puid_to_pers:  # we already have a person for this user
                            pid = uuidToObject(self.puid_to_pers[_euidm['_uid']]).id
                            for y in self.person(e_userid, item, pid, u'Existing', transitions=()): yield y
                            for y in self.hp(e_userid, item): yield y
                        else:
                            pid = _euidm['_uid']
                            # TODO take into account prenom nom order
                            parts = _euidm['_fullname'].split()
                            for y in self.person(e_userid, item, pid, u'Matched', firstname=parts[0],
                                                 lastname=' '.join(parts[1:])): yield y
                            for y in self.hp(e_userid, item): yield y
                    else:
                        pid = idnormalizer.normalize(_euidm['_prenom'] and u'{} {}'.format(_euidm['_prenom'],
                                                     _euidm['_nom']) or _euidm['_nom'])
                        for y in self.person(e_userid, item, pid, u'Unmatched', firstname=_euidm['_prenom'],
                                             lastname=_euidm['_nom']): yield y
                        for y in self.hp(e_userid, item): yield y
                else:  # we do not have a _sender_id or _sender_id is not a user id
                    for y in self.person(u'None', item, 'reprise-donnees', u'None', firstname=u'',
                                         lastname=u'Reprise données'): yield y
                    for y in self.hp(u'None', item): yield y
                continue
            yield item
        if self.change:
            o_logger.info("Part d: some internal persons or/and held positions have been added.")


class PostActions(object):
    """Do post actions after creation"""
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)

    def __iter__(self):
        for item in self.previous:
            pa = item.get('_post_actions', [])
            if u'store_internal_person_info' in pa:
                eid = item[u'internal_number']
                uid = self.portal.unrestrictedTraverse(item['_path']).UID()
                self.storage['data']['p_euid_to_pers'][eid] = uid
                if uid not in self.storage['data']['p_hps']:
                    self.storage['data']['p_hps'][uid] = {'path': item['_path'], 'eid': eid, 'hps': {},
                                                          'state': 'deactivated'}
            yield item
