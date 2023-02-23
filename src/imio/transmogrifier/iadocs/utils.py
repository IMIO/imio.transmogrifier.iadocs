# -*- coding: utf-8 -*-
import os

from collective.classification.tree.utils import iterate_over_tree
from collective.contact.plonegroup.browser.settings import BaseOrganizationServicesVocabulary
from collective.contact.plonegroup.config import get_registry_organizations
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import relative_path
from imio.helpers.vocabularies import get_users_voc
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from plone import api
from Products.CMFPlone.utils import safe_unicode

itf = 'imio.dms.mail.browser.settings.IImioDmsMailConfig'
MAILTYPES = {'te': '{}.mail_types'.format(itf), 'ts': '{}.omail_types'.format(itf),
             'fe': '{}.omail_send_modes'.format(itf)}


def encode_list(lst, encoding):
    """Encode a list following encoding.

    :param lst: lst to transform
    :param encoding: encoding
    :return: new list
    """
    new_list = []
    for content in lst:
        if isinstance(content, unicode):  #
            content = content.encode(encoding)
        new_list.append(content)
    return new_list


def get_categories(portal):
    """Get already defined categories"""
    cats = {}
    for cat in iterate_over_tree(portal.tree):
        if cat.identifier in cats:
            o_logger.error(u"code '{}' '{}' already loaded '{}'".format(cat.identifier, cat.title,
                                                                        cats[cat.identifier]['title']))
        cats[cat.identifier] = {'title': cat.title, 'uid': cat.UID(), 'enabled': cat.enabled}
    return cats


def get_mailtypes(portal):
    """Get mail types and send_mode"""
    mailtypes = {}
    for key, rec in MAILTYPES.items():
        mailtypes[key] = {}
        for dic in api.portal.get_registry_record(rec):
            dico = dict(dic)
            mailtypes[key][dico.pop('value')] = dico
    return mailtypes


def get_part(name):
    return name[0:1]


def get_personnel(portal, eid_fld='internal_number'):
    """Get the personnel persons and held positions"""
    p_userid_to_person = {}  # store plone userid linking person
    e_userid_to_person = {}  # store external userid linking person
    hps = {}  # store person uid with
    brains = portal.portal_catalog.unrestrictedSearchResults(
        portal_type=['held_position'],
        object_provides='imio.dms.mail.interfaces.IPersonnelContact')
    for brain in brains:
        # the userid is stored in mail_type index !!
        hp = brain._unrestrictedGetObject()
        person = hp.get_person()
        org = hp.get_organization()
        if org is None:
            continue
        puid = person.UID()
        ouid = org.UID()
        if person.userid and person.userid not in p_userid_to_person:
            p_userid_to_person[person.userid] = puid
        euid = getattr(person, eid_fld)
        if euid and euid not in e_userid_to_person:
            e_userid_to_person[euid] = puid
        if puid not in hps:
            hps[puid] = {'path': relative_path(portal, '/'.join(person.getPhysicalPath())), 'eid': euid, 'hps': {},
                         'state': api.content.get_state(person)}
        if ouid not in hps[puid]['hps']:
            hps[puid]['hps'][ouid] = {'path': relative_path(portal, brain.getPath()),
                                      'state': api.content.get_state(hp)}
    return p_userid_to_person, e_userid_to_person, hps


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
        value = getattr(org, eid_fld)
        eids = value and value.split(u',') or []
        all_orgs[term.value] = {'p': relative_path(portal, '/'.join(org.getPhysicalPath())), 't': org.title,
                                'ft': term.title, 'eids': eids, 'st': api.content.get_state(org),
                                'sl': term.value in selected_orgs}
        for eid in eids:
            eid_to_orgs[eid] = term.value
    return all_orgs, eid_to_orgs


def get_users(portal):
    """Get users"""
    res = {}
    for term in get_users_voc(False):
        res[term.value] = {'fullname': term.title}
    return res


def get_values_string(item, keys, sep=u':'):
    """Return a string value corresponding to multiple keys

    :param item: yielded item (dict)
    :param keys: item keys
    :param sep: separator
    :return: string
    """
    ret = [item.get(key, u'') for key in keys]  # noqa
    return sep.join(ret)


def full_path(path, filename):
    if not os.path.isabs(filename):
        return os.path.join(path, filename)
    return filename


def is_in_part(section, part):
    """Check if part is one of given."""
    return part in section.storage.get('parts', '')


def log_error(item, msg, level='error', fld='_eid'):
    getattr(e_logger, level)(u'{}: {} {}, {}'.format(item['_bpk'], fld, item[fld], msg))
    item['_error'] = True
