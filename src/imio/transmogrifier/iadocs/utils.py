# -*- coding: utf-8 -*-
from collective.classification.folder.content.vocabularies import full_title_categories
from collective.classification.tree.utils import iterate_over_tree
from collective.contact.plonegroup.browser.settings import BaseOrganizationServicesVocabulary
from collective.contact.plonegroup.config import get_registry_organizations
from collective.contact.plonegroup.utils import get_organizations
from imio.helpers.cache import get_plone_groups_for_user
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import relative_path
from imio.helpers.vocabularies import get_users_voc
from imio.pyutils.system import dump_var
from imio.pyutils.system import load_var
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from plone import api

import os


itf = 'imio.dms.mail.browser.settings.IImioDmsMailConfig'
MAILTYPES = {'te': '{}.mail_types'.format(itf), 'ts': '{}.omail_types'.format(itf),
             'fe': '{}.omail_send_modes'.format(itf)}


def course_store(section):
    """Stores course in blueprints.  Needs storage and name as section attributes"""
    if section.name in section.storage['course']:
        section.storage['course'][section.name] += 1
    else:
        section.storage['course'][section.name] = 1


def course_print(section):
    """Prints course"""
    for name in section.storage['course']:
        o_logger.info("> {}: {}".format(name, section.storage['course'][name]))


def encode_list(lst, encoding):
    """Encode a list following encoding.

    :param lst: lst to transform
    :param encoding: encoding
    :return: new list
    """
    new_list = []
    for content in lst:
        if isinstance(content, unicode):
            content = content.encode(encoding)
        new_list.append(content)
    return new_list


def get_categories(portal):
    """Get already defined categories"""
    cats = {}
    # cats = {None: {'title': u'', 'uid': portal.tree.UID(), 'enabled': False}}  # the container tree
    for cat in iterate_over_tree(portal.tree):
        if cat.identifier in cats:
            o_logger.error(u"code '{}' '{}' already loaded '{}'".format(cat.identifier, cat.title,
                                                                        cats[cat.identifier]['title']))
        cats[cat.identifier] = {'title': cat.title, 'uid': cat.UID(), 'enabled': cat.enabled, 'obj': cat}
    return cats


def get_folders(section):
    """Get already defined classification folders"""
    portal = section.portal
    folders_uids = {}
    irn_to_folder = {}
    folders_titles = {}
    fuids_file = full_path(section.storage['csvp'], '2_folder_folders_uids.dic')
    irntf_file = full_path(section.storage['csvp'], '2_folder_irn_to_folder.dic')
    ft_file = full_path(section.storage['csvp'], '2_folder_folders_titles.dic')
    if os.path.exists(fuids_file) and os.path.exists(irntf_file) and os.path.exists(ft_file):
        load_var(fuids_file, folders_uids)
        load_var(irntf_file, irn_to_folder)
        load_var(ft_file, folders_titles)
        return folders_uids, irn_to_folder, folders_titles

    crits = {'object_provides': 'collective.classification.folder.content.classification_folder.'
                                'IClassificationFolder',
             'sort_on': 'ClassificationFolderSort'}
    for brain in portal.portal_catalog.unrestrictedSearchResults(**crits):
        folder = brain._unrestrictedGetObject()
        parent = folder.cf_parent()
        full_title = full_title_categories(folder, with_irn=False, with_cat=False)[0]
        irn = folder.internal_reference_no
        if irn:
            # parts = re.split('/', irn)
            parts = irn.split('/')
            irn = parts[0]
            # try:
            #     int(irn)
            # except ValueError:
            #     o_logger.error(u"Invalid irn '{}' for '{}' ({})".format(irn, full_title, folder.absolute_url()))
        folders_uids[brain.UID] = {'title': folder.title, 'path': brain.getPath(), 'full_title': full_title,
                                   'parent': parent and parent.UID() or None, 'irn': folder.internal_reference_no,
                                   'peid': irn}
        if irn not in irn_to_folder:
            irn_to_folder[irn] = {'uid': brain.UID}
        elif not brain.getPath().startswith('{}/'.format(folders_uids[irn_to_folder[irn]['uid']]['path'])):
            # subfolder has been created with parent irn but modified by user who has removed suffix
            o_logger.error(u"Already found folder irn '{}' ({}) on {}".format(irn, brain.getPath(),
                           folders_uids[irn_to_folder[irn]['uid']]['path']))
        if full_title not in folders_titles:
            folders_titles[full_title] = {'uids': []}
        # o_logger.error(u"Already found folder title '{}' ({}) on {}".format(full_title, brain.getPath(),
        #                folders_uids[folders_titles[full_title]['uid']]['path']))
        folders_titles[full_title]['uids'].append(brain.UID)
    dump_var(fuids_file, folders_uids)
    dump_var(irntf_file, irn_to_folder)
    dump_var(ft_file, folders_titles)
    return folders_uids, irn_to_folder, folders_titles


def get_mailtypes(portal):
    """Get mail types and send_mode"""
    mailtypes = {}
    for key, rec in MAILTYPES.items():
        mailtypes[key] = {}
        for dic in api.portal.get_registry_record(rec):
            dico = dict(dic)
            mailtypes[key][dico.pop('value')] = dico
    return mailtypes


def get_related_parts(name):
    if '__' in name:
        return name.split('__')[0]
    return None


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
                                      'state': api.content.get_state(hp),
                                      'puid': brain.UID}
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
            eid_to_orgs[eid] = {'uid': term.value}
    return all_orgs, eid_to_orgs


def get_users(portal):
    """Get users"""
    res = {}
    for term in get_users_voc(False):
        res[term.value] = {'fullname': term.title}
    return res


def get_users_groups(portal, u_dic):
    """Get users groups"""
    res = {}
    org_uids = get_organizations(only_selected=False, the_objects=False, caching=False)
    for userid in u_dic:
        res[userid] = {}
        for groupid in get_plone_groups_for_user(user_id=userid):
            if groupid == 'AuthenticatedUsers':
                continue
            parts = groupid.split('_')
            if len(parts) == 1 or parts[0] not in org_uids:
                group_suffix = ''
            else:
                group_suffix = '_'.join(parts[1:])
            res[userid].setdefault(group_suffix, {})[parts[0]] = {}
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


def full_name(firstname, lastname, fn_first=True):
    if firstname:
        if fn_first:
            return u'{} {}'.format(firstname, lastname)
        else:
            return u'{} {}'.format(lastname, firstname)
    else:
        return lastname


def full_path(path, filename):
    if not os.path.isabs(filename):
        return os.path.join(path, filename)
    return filename


def is_in_part(section, parts):
    """Check if part is one of given."""
    for part in parts or []:
        if part in section.storage.get('parts', ''):
            return True
    return False


def log_error(item, msg, level='error', fld='_eid'):
    getattr(e_logger, level)(u'{}: {} {}, {}'.format(item['_bpk'], fld, item[fld], msg))
    item['_error'] = True
