# -*- coding: utf-8 -*-
import os

from collective.contact.plonegroup.browser.settings import BaseOrganizationServicesVocabulary
from collective.contact.plonegroup.config import get_registry_organizations
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import relative_path
from imio.helpers.vocabularies import get_users_voc
from imio.transmogrifier.iadocs import e_logger
from plone import api

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
    getattr(e_logger, level)(u'{}: {} {}, {}'.format(item['_etyp'], fld, item[fld], msg))
    item['_error'] = True
