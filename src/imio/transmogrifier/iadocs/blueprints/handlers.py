# -*- coding: utf-8 -*-
from collective.classification.tree.utils import create_category
from collective.classification.tree.utils import get_parents
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from imio.dms.mail import IM_EDITOR_SERVICE_FUNCTIONS
from imio.dms.mail.utils import create_period_folder
from imio.dms.mail.utils import is_in_user_groups
from imio.dms.mail.utils import separate_fullname
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import clean_value
from imio.helpers.transmogrifier import get_obj_from_path
from imio.pyutils.utils import all_of_dict_values
from imio.pyutils.utils import one_of_dict_values
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import full_name
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
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
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.all_orgs = self.storage['data']['p_orgs_all']
        self.eid_to_orgs = self.storage['data']['p_eid_to_orgs']
        self.match = self.storage['data']['e_service_match']

    def __iter__(self):
        change = False
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
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
        * related_storage = M, related data storage
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.related_storage = safe_unicode(options['related_storage'])
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item, storage=self.storage):
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
        self.parts = get_related_parts(name)
        self.to_add = {}
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        p_types = self.storage['data']['p_mailtype']
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item, storage=self.storage):
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
        self.parts = get_related_parts(name)
        self.change = False
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.puid_to_pers = self.storage['data']['p_userid_to_pers']
        self.euid_to_pers = self.storage['data']['p_euid_to_pers']
        self.p_hps = self.storage['data']['p_hps']
        self.eid_to_orgs = self.storage['data']['p_eid_to_orgs']
        self.e_c_s = self.storage['data']['e_contacts_sender']
        self.e_u_m = self.storage['data']['e_user_match']
        self.intids = getUtility(IIntIds)

    def person(self, e_userid, item, pid, title, firstname=None, lastname=None, transitions=('deactivate',)):
        if e_userid not in self.euid_to_pers:
            # we create or update a person
            path = os.path.join(self.storage['plone']['directory_path'], 'personnel-folder/{}'.format(pid))
            pdic = {'_type': 'person', '_path': path, u'internal_number': e_userid, 'use_parent_address': False,
                    u'_transitions': transitions, u'_bpk': u'person_sender', u'_eid': item['_eid'],
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
        ouid = self.eid_to_orgs[item['_service']]['uid']
        if ouid not in self.p_hps[puid]['hps']:
            # we create a hp
            path = os.path.join(self.p_hps[puid]['path'], ouid)
            org = uuidToObject(ouid, unrestricted=True)
            hpdic = {'_type': 'held_position', '_path': path, 'use_parent_address': True,
                     'position': RelationValue(self.intids.getId(org)), 'internal_number': u'',
                     u'_transitions': transitions, u'_bpk': u'hp_sender', u'_eid': item['_eid']}
            self.p_hps[puid]['hps'][ouid] = {'path': path, 'state': 'deactivated'}
            self.change = True
            return [hpdic]
        return []

    def __iter__(self):
        idnormalizer = getUtility(IIDNormalizer)
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
                if item['_sender_id'] and item['_sender_id'] in self.e_c_s:  # we have a user id
                    e_userid = self.e_c_s[item['_sender_id']]['_uid']
                    _euidm = self.e_u_m[e_userid]
                    if _euidm['_uid']:  # we have a match
                        if _euidm['_uid'] in self.puid_to_pers:  # we already have a person for this user
                            pid = uuidToObject(self.puid_to_pers[_euidm['_uid']]).id
                            for y in self.person(e_userid, item, pid, u'Existing', transitions=()): yield y  # noqa
                            for y in self.hp(e_userid, item): yield y  # noqa
                        else:
                            pid = _euidm['_uid']
                            fn, ln = separate_fullname(None, fn_first=self.storage['plone']['firstname_first'],
                                                       fullname=_euidm['_fullname'])
                            for y in self.person(e_userid, item, pid, u'Matched', firstname=fn,
                                                 lastname=ln): yield y  # noqa
                            for y in self.hp(e_userid, item): yield y  # noqa
                    else:
                        pid = idnormalizer.normalize(full_name(_euidm['_prenom'], _euidm['_nom'],
                                                               fn_first=self.storage['plone']['firstname_first']))
                        for y in self.person(e_userid, item, pid, u'Unmatched', firstname=_euidm['_prenom'],
                                             lastname=_euidm['_nom']): yield y  # noqa
                        for y in self.hp(e_userid, item): yield y  # noqa
                else:  # we do not have a _sender_id or _sender_id is not a user id
                    for y in self.person(u'None', item, 'reprise-donnees', u'None', firstname=u'',
                                         lastname=u'Reprise données'): yield y  # noqa
                    for y in self.hp(u'None', item): yield y  # noqa
                continue
            yield item
        if self.change:
            o_logger.info("Part d: some internal persons or/and held positions have been added.")


class ECategoryUpdate(object):
    """Update category or create a new one.

    Parameters:
        * condition = O, condition expression
        * title_replace_slash = O, replace / by - if 1 (default 1)
        * decimal_import = O, identifier is decimal code if 1 (default 1)
        * yield = O,
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.replace_slash = bool(int(options.get('title_replace_slash') or '1'))
        self.decimal_import = bool(int(options.get('decimal_import') or '1'))
        if not self.decimal_import:
            raise 'Code is only handling decimal import'
        self.p_category = self.storage['data']['p_category']

    def __iter__(self):
        change = False
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
                if item['_pcode']:
                    if item['_pcode'] not in self.p_category:
                        log_error(item, u"The _pcode '{}' is not in the loaded categories. "
                                        u"We pass it".format(item['_pcode']))
                        continue
                    node = self.p_category[item['_pcode']]['obj']
                    # when title is not "defined"
                    if node.title == node.identifier and item['_etitle']:
                        node.title = self.replace_slash and item['_etitle'].replace('/', '-') or item['_etitle']
                        self.p_category[item['_pcode']]['title'] = node.title
                        item['_ptitle'] = node.title
                        change = True
                    # # when pcode is different (new matching) and title is different
                    # new_title = self.replace_slash and item['_etitle'].replace('/', '-') or item['_etitle']
                    # if item['_ecode'] != item['_pcode'] and new_title != node.title:
                    #     o_logger.info(u"The code '{}' with title '{}' will be replaced by '{}'".format(
                    #                   item['_pcode'], node.title, new_title))
                    #     node.title = new_title
                    #     self.p_category[item['_pcode']]['title'] = node.title
                    #     item['_ptitle'] = node.title
                    #     change = True
                else:  # we will create the category
                    parent = self.portal.tree
                    parts = get_parents(item['_ecode'])
                    for part in parts[:-1]:
                        if part not in self.p_category:  # not already in Plone
                            parent = create_category(parent, {'identifier': part, 'title': part, 'enabled': False},
                                                     event=False)
                            self.p_category[parent.identifier] = {'title': parent.title, 'uid': parent.UID(),
                                                                  'enabled': parent.enabled, 'obj': parent}
                            change = True
                        else:
                            parent = self.p_category[part]['obj']
                    if parts[-1] not in self.p_category:
                        # TODO check if _eactive is well boolean
                        node = create_category(parent, {'identifier': parts[-1], 'title': self.replace_slash and
                                               item['_etitle'].replace('/', '-') or item['_etitle'],
                                               'enabled': item['_eactive']}, event=True)
                        self.p_category[node.identifier] = {'title': node.title, 'uid': node.UID(),
                                                            'enabled': node.enabled, 'obj': node}
                        item['_pcode'], item['_ptitle'] = node.identifier, node.title
                        item['_puid'], item['_pactive'] = node.UID(), node.enabled
                        change = True
                if not self.storage['commit']:
                    continue
            yield item
        if change:
            o_logger.info("Part e: some categories have been created or updated.")


def get_contact_name(dic, dic2):
    sender = []
    p_sender = []
    p_name = u' '.join(all_of_dict_values(dic2, ['_lname', '_fname']))
    if not p_name and dic2.get('_name2'):
        p_name = dic2['_name2']
    if p_name:
        p_sender.append(p_name)
    name = u' '.join(all_of_dict_values(dic, ['_lname', '_fname']))
    if not name and dic.get('_name2'):
        name = dic['_name2']
    if name and (not p_name or name != p_name):
        sender.append(name)
    # email
    p_eml = one_of_dict_values(dic2, ['_email1', '_email2', '_email3'])
    if p_eml:
        p_sender.append(p_eml)
    eml = one_of_dict_values(dic, ['_email1', '_email2', '_email3'])
    if eml and (not p_eml or eml != p_eml):
        sender.append(eml)
    if dic.get('_function'):
        sender.append(dic['_function'])
    if dic2.get('_e_nb'):
        p_sender.append(u'NE:{}'.format(dic2['_e_nb']))
    if dic.get('_e_nb') and (not dic2.get('_e_nb') or dic.get('_e_nb') != dic2.get('_e_nb')):
        sender.append(u'NE:{}'.format(dic['_e_nb']))
    # TODO add ctyp
    return sender, p_sender


def get_contact_phone(dic1, dic2):
    phones = []
    phone = one_of_dict_values(dic1, ['_phone1', '_phone2', '_phone3'])
    if phone:
        phones.append(phone)
    else:
        phone = one_of_dict_values(dic2, ['_phone1', '_phone2', '_phone3'])
        if phone:
            phones.append(phone)
    cell = one_of_dict_values(dic1, ['_cell1', '_cell2', '_cell3'])
    if cell:
        phones.append(cell)
    else:
        cell = one_of_dict_values(dic2, ['_cell1', '_cell2', '_cell3'])
        if cell:
            phones.append(cell)
    return phones


def get_contact_info(section, item, label, c_id_fld, free_fld, dest1, dest2):
    """Get contact infos

    :param section: section object
    :param item: yielded dic
    :param label: prefix to add
    :param c_id_fld: field name containing contact id
    :param free_fld: field name containing free contact text
    :param dest1: main list where to add main infos
    :param dest2: secondary list where to add less important infos
    :return: boolean indicating changes
    """
    # e_contact = _uid _ctyp _lname _fname _ptitle _street _pc _city _email1 _email2 _email3 _function _e_nb
    # _cell1 _cell2 _cell3 _web _org _name2 _parent_id _addr_id
    change = False
    sender = []
    p_sender = []
    m_sender = clean_value(item[free_fld], patterns=[r'^["\']+$'])
    if item[c_id_fld]:
        infos = section.storage['data']['e_contact'][item[c_id_fld]]
        parent_infos = {}
        if infos['_parent_id']:
            parent_infos = section.storage['data']['e_contact'].get(infos['_parent_id'], {})
        sender, p_sender = get_contact_name(infos, parent_infos)
        if p_sender:
            change = True
            dest1.append(u'{} parent: {}.'.format(label, u', '.join(p_sender)))
        if sender:
            change = True
            dest1.append(u'{}: {}.'.format(label, u', '.join(sender)))
        # address
        p_address = all_of_dict_values(parent_infos, ['_street', '_pc', '_city'])
        address = all_of_dict_values(infos, ['_street', '_pc', '_city'])
        if address:
            change = True
            dest2.append(u'Adresse {}: {}.'.format(label.lower(), u' '.join(address)))
        elif p_address:  # we add just one address
            change = True
            dest2.append(u'Adresse {} parent: {}.'.format(label.lower(), u' '.join(p_address)))
        else:
            pass  # include _addr_id ? no!
        # phones
        phones = get_contact_phone(infos, parent_infos)
        if phones:
            change = True
            dest2.append(u'Tél {}: {}.'.format(label.lower(), u', '.join(phones)))
    if m_sender:
        lines = m_sender.split('\n')
        change = True
        dest1.append(u'{} libre: {}'.format(label, lines.pop(0)))
        if lines:
            dest2.append(u'{} libre: {}'.format(label, u', '.join(lines)))
    return change


class L1SenderHandling(object):
    """Handles contact"""
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if not self.condition(item):
                yield item
                continue
            desc = 'description' in item and item.get('description').split('\r\n') or []
            d_t = 'data_transfer' in item and item.get('data_transfer').split('\r\n') or []
            if get_contact_info(self, item, u'Expéditeur', '_sender_id', '_sender', desc, d_t):
                item['description'] = u'\r\n'.join(desc)
                item['data_transfer'] = u'\r\n'.join(d_t)
            yield item


class M1AssignedUserHandling(object):
    """Handles assigned user"""
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        if not is_in_part(self, self.parts):
            return
        self.im_paths = self.storage['data']['e_mail_i']
        self.contacts = self.storage['data']['e_contacts_sender']
        self.user_match = self.storage['data']['e_user_match']
        # self.e_user_service = self.storage['data']['e_user_service']
        self.p_user_service = self.storage['data']['p_user_service']
        # calculate once the editor services for each user
        self.p_u_s_editor = {}
        for user in self.p_user_service:
            self.p_u_s_editor[user] = []
            for fct in self.p_user_service[user]:
                if fct in IM_EDITOR_SERVICE_FUNCTIONS:
                    for org in self.p_user_service[user][fct]:
                        if org not in self.p_u_s_editor[user]:
                            self.p_u_s_editor[user].append(org)

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            if item['_contact_id'] not in self.contacts:
                o_logger.warning("eid '%s', contact id not a user '%s'", item['_eid'], item['_contact_id'])
                continue
            e_userid = self.contacts[item['_contact_id']]['_uid']
            p_userid = self.user_match[e_userid]['_uid']
            o_logger.debug("mail %s: euser %s, puser %s", item['_mail_id'], self.user_match[e_userid]['_nom'], p_userid)
            imail = get_obj_from_path(self.portal, path=self.im_paths[item['_mail_id']]['path'])
            if imail is None:
                o_logger.warning("mail %s: path '%s' not found", item['_mail_id'],
                                 self.im_paths[item['_mail_id']]['path'])
                continue
            item = {'_eid': item['_eid'], '_path': self.im_paths[item['_mail_id']]['path'],
                    '_type': imail.portal_type, '_bpk': 'i_assigned_user', '_act': 'U'}
            # store info in data_transfer
            d_t = (imail.data_transfer or u'').split('\r\n')
            r_name = u' '.join(all_of_dict_values(self.user_match[e_userid], ['_nom', '_prenom']))
            r_messages = u', '.join(all_of_dict_values(item, [u'_action', u'_message', u'_response'],
                                                       labels=[u'', u'message', u'réponse']))
            r_infos = u"Destinataire: {}, {}".format(r_name, r_messages and u', {}'.format(r_messages) or u'')
            if r_infos not in d_t:
                d_t.append(r_infos)
                item['data_transfer'] = u'\r\n'.join(d_t)
            # plone user is in the treating_group
            if p_userid and imail.treating_groups and imail.treating_groups in self.p_u_s_editor[p_userid]:
                item['assigned_user'] = p_userid
            elif p_userid:
                # comblain: cannot put user service in copy because most users have more than one service
                pass
            else:
                # comblain: cannot put user service in copy because most users have more than one service
                pass
            if 'data_transfer' in item or 'assigned_user' in item:
                # o_logger.debug(item)
                yield item


class POMSender(object):
    """Set sender held_position.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.change = False
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.puid_to_pers = self.storage['data']['p_userid_to_pers']
        self.euid_to_pers = self.storage['data']['p_euid_to_pers']
        self.p_hps = self.storage['data']['p_hps']
        self.eid_to_orgs = self.storage['data']['p_eid_to_orgs']
        self.e_c_s = self.storage['data']['e_contact']
        self.e_u_m = self.storage['data']['e_user_match']

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            if item['_sender_id']:
                if self.e_c_s[item['_sender_id']]['_uid']:  # we have a user id
                    e_userid = self.e_c_s[item['_sender_id']]['_uid']
                else:  # _sender_id is not a user id
                    e_userid = u'None'
                    # add info in data_transfer
                    desc = 'description' in item and item.get('description').split('\r\n') or []
                    d_t = 'data_transfer' in item and item.get('data_transfer').split('\r\n') or []
                    if get_contact_info(self, item, u'Expéditeur', '_sender_id', '_sender', desc, d_t):
                        item['description'] = u'\r\n'.join(desc)
                        item['data_transfer'] = u'\r\n'.join(d_t)
            else:  # we do not have a _sender_id
                e_userid = u'None'
            pers_uid = self.euid_to_pers[e_userid]
            ouid = self.eid_to_orgs[item['_service_id']]['uid']
            hp_dic = self.p_hps[pers_uid]['hps'][ouid]
            item['sender'] = hp_dic['puid']
            o_logger.debug(u"OM sender info '%s'. Sender '%s', Description '%r', Data '%r'", item['_eid'],
                           item['sender'], item.get('description', u''), item.get('data_transfer', u''))
            yield item


class ParentPathInsert(object):
    """Add parent path key following type"""
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.im_folder = self.portal['incoming-mail']
        self.om_folder = self.portal['outgoing-mail']

    def __iter__(self):
        for item in self.previous:
            ptyp = item.get('_type')
            if not ptyp or '_path' in item or '_parenth' in item:
                yield item
                continue
            if ptyp in ('dmsincomingmail', 'dmsincoming_email'):
                container = create_period_folder(self.im_folder, item['creation_date'])
                item['_parenth'] = u'/incoming-mail/{}'.format(container.id)
            elif ptyp == 'dmsoutgoingmail':
                container = create_period_folder(self.om_folder, item['creation_date'])
                item['_parenth'] = u'/outgoing-mail/{}'.format(container.id)
            yield item


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
                uid = get_obj_from_path(self.portal, item).UID()
                self.storage['data']['p_euid_to_pers'][eid] = uid
                if uid not in self.storage['data']['p_hps']:
                    self.storage['data']['p_hps'][uid] = {'path': item['_path'], 'eid': eid, 'hps': {},
                                                          'state': 'deactivated'}
            yield item


class Q1Recipients(object):
    """Handles om recipients.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.change = False
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.om_paths = self.storage['data']['e_mail_o']
        self.e_c = self.storage['data']['e_contact']

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            omail = get_obj_from_path(self.portal, path=self.om_paths[item['_mail_id']]['path'])
            if omail is None:
                o_logger.warning("mail %s: path '%s' not found", item['_mail_id'],
                                 self.om_paths[item['_mail_id']]['path'])
                continue
            item2 = {'_eid': item['_eid'], '_path': self.om_paths[item['_mail_id']]['path'],
                     '_type': omail.portal_type, '_bpk': 'o_recipients', '_act': 'U'}
            desc = 'description' in item and item.get('description').split('\r\n') or []
            d_t = 'data_transfer' in item and item.get('data_transfer').split('\r\n') or []
            if get_contact_info(self, item, u'Destinataire', '_contact_id', '_comment', desc, d_t):
                item2['description'] = u'\r\n'.join(desc)
                r_messages = u', '.join(all_of_dict_values(item, [u'_action', u'_message', u'_response'],
                                                           labels=[u'', u'message', u'réponse']))
                if r_messages:
                    r_messages = u'{}: {}'.format(u'Complément destinataire', u', {}'.format(r_messages))
                    if r_messages not in d_t:
                        d_t.append(r_messages)
                item2['data_transfer'] = u'\r\n'.join(d_t)
                yield item2
