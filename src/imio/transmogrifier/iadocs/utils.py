# -*- coding: utf-8 -*-
from collective.contact.plonegroup.browser.settings import BaseOrganizationServicesVocabulary
from collective.contact.plonegroup.config import get_registry_organizations
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import relative_path
from plone import api


def get_plonegroup_orgs(portal, eid_fld='internal_number'):
    """get plonegroups organisations"""
    all_orgs = {}
    eid_to_orgs = {}
    selected_orgs = get_registry_organizations()
    factory = BaseOrganizationServicesVocabulary()
    factory.valid_states = ('active', 'inactive')  # not only active
    voc = factory(portal)
    for term in voc:
        org = uuidToObject(term.value)
        eid = getattr(org, eid_fld)
        all_orgs[term.value] = {'p': relative_path(portal, '/'.join(org.getPhysicalPath())), 't': org.title,
                                'ft': term.title, 'eid': eid, 'st': api.content.get_state(org),
                                'sl': term.value in selected_orgs}
        if eid:
            eid_to_orgs[eid] = term.value
    return all_orgs, eid_to_orgs
