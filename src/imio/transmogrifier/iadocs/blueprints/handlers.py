# -*- coding: utf-8 -*-
from collective.classification.tree.utils import create_category
from collective.classification.tree.utils import get_parents
from collective.contact.plonegroup.config import get_registry_organizations
from collective.contact.plonegroup.config import set_registry_organizations
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from imio.dms.mail import IM_EDITOR_SERVICE_FUNCTIONS
from imio.dms.mail.utils import create_period_folder
from imio.dms.mail.utils import separate_fullname
from imio.helpers.content import uuidToObject
from imio.helpers.transmogrifier import clean_value
from imio.helpers.transmogrifier import get_obj_from_path
from imio.helpers.transmogrifier import pool_tuples
from imio.pyutils.system import full_path
from imio.pyutils.utils import all_of_dict_values
from imio.pyutils.utils import one_of_dict_values
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.blueprints.various import short_log
from imio.transmogrifier.iadocs.utils import course_store
from imio.transmogrifier.iadocs.utils import full_name
from imio.transmogrifier.iadocs.utils import get_file_content
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from imio.transmogrifier.iadocs.utils import MAILTYPES
from imio.transmogrifier.iadocs.utils import print_item  # noqa
from plone import api
from plone.i18n.normalizer import IIDNormalizer
from Products.CMFPlone.utils import safe_unicode
from z3c.relationfield import RelationValue
from zc.relation.interfaces import ICatalog
from zope.annotation import IAnnotations
from zope.component import getUtility
from zope.interface import classProvides
from zope.interface import implements
from zope.intid import IIntIds

import cPickle
import os
import transaction


class AServiceUpdate(object):
    """Update plonegroup services with external id value.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
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
            if is_in_part(self, self.parts) and self.condition(item, storage=self.storage):
                course_store(self)
                if item['_eid'] in self.eid_to_orgs or not self.match:
                    continue
                if item['_eid'] not in self.match:
                    log_error(item, u"Not in matching file")
                    continue
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
        self.name = name
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
                course_store(self)
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
        self.name = name
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
                course_store(self)
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


class DefaultContactSet(object):
    """Set default contact.

    Parameters:
        * fieldname = M, field to set
        * is_list = M, int for boolean (1 if the field is a list)
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.field = safe_unicode(options['fieldname'])
        self.is_list = bool(int(options.get('is_list') or '1'))
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        intids = getUtility(IIntIds)
        self.def_ctct_iid = intids.getId(self.storage['plone']['def_contact'])

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            course_store(self)
            if self.is_list:
                item[self.field] = [RelationValue(self.def_ctct_iid)]
            else:
                item[self.field] = RelationValue(self.def_ctct_iid)
            yield item


class DOMSenderCreation(object):
    """Create sender held_position if necessary.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
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
                    u'_bpk': u'person_sender', u'_eid': item['_eid'], u'_deactivate': 'deactivate' in transitions,
                    u'creation_date': self.storage['creation_date'],
                    u'modification_date': self.storage['creation_date'],
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
                     u'_bpk': u'hp_sender', u'_eid': item['_eid'], u'_deactivate': 'deactivate' in transitions,
                     u'creation_date': self.storage['creation_date'],
                     u'modification_date': self.storage['creation_date']}
            self.p_hps[puid]['hps'][ouid] = {'path': path, 'state': 'deactivated'}
            self.change = True
            return [hpdic]
        return []

    def __iter__(self):
        idnormalizer = getUtility(IIDNormalizer)
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
                course_store(self)
                if item['_sender_id'] and item['_sender_id'] in self.e_c_s:  # we have a user id
                    e_userid = self.e_c_s[item['_sender_id']]['_user_id']
                    _euidm = self.e_u_m[e_userid]
                    if _euidm['_p_userid']:  # we have a match
                        if _euidm['_p_userid'] in self.puid_to_pers:  # we already have a person for this user
                            pid = uuidToObject(self.puid_to_pers[_euidm['_p_userid']]).id
                            for y in self.person(e_userid, item, pid, u'Existing', transitions=()): yield y  # noqa
                            for y in self.hp(e_userid, item): yield y  # noqa
                        else:
                            pid = _euidm['_p_userid']
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
        * b_condition = O, blueprint condition expression
        * condition = O, condition expression
        * title_replace_slash = O, replace / by - if 1 (default 1)
        * decimal_import = O, identifier is decimal code if 1 (default 1)
        * yield = O,
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        b_condition = Condition(options.get('b_condition') or 'python:True', transmogrifier, name, options)
        self.b_cond = b_condition(None, storage=self.storage)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.replace_slash = bool(int(options.get('title_replace_slash') or '1'))
        self.decimal_import = bool(int(options.get('decimal_import') or '1'))
        if not self.decimal_import:
            raise 'Code is only handling decimal import'
        self.p_category = self.storage['data']['p_category']

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.b_cond and self.condition(item):
                course_store(self)
                # o_logger.info(u"{}, {}".format(item['_ecode'], item['_etitle']))
                if item.get('_pcode'):  # code is already in plone
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
                        item['_act'] = 'U'
                    # # when pcode is different (new matching) and title is different
                    # new_title = self.replace_slash and item['_etitle'].replace('/', '-') or item['_etitle']
                    # if item['_ecode'] != item['_pcode'] and new_title != node.title:
                    #     o_logger.info(u"The code '{}' with title '{}' will be replaced by '{}'".format(
                    #                   item['_pcode'], node.title, new_title))
                    #     node.title = new_title
                    #     self.p_category[item['_pcode']]['title'] = node.title
                    #     item['_ptitle'] = node.title
                    #     item['_act'] = 'U'
                else:  # we will create the category
                    parent = self.portal.tree
                    parts = get_parents(item['_ecode'])
                    for part in parts[:-1]:
                        if part not in self.p_category:  # not already in Plone
                            title = self.storage['data']['e_category'].get(part, {}).get('_etitle', part)
                            # if title != part:
                            #     o_logger.info(u"P:{}, {}".format(item['_ecode'], item['_etitle']))
                            parent = create_category(parent, {'identifier': part, 'title': title, 'enabled': False},
                                                     event=False)
                            self.p_category[parent.identifier] = {'title': parent.title, 'uid': parent.UID(),
                                                                  'enabled': parent.enabled, 'obj': parent}
                            item['_act'] = 'N'
                        else:
                            parent = self.p_category[part]['obj']
                    if parts[-1] not in self.p_category:
                        node = create_category(parent, {'identifier': parts[-1], 'title': self.replace_slash and
                                               item['_etitle'].replace('/', '-') or item['_etitle'],
                                               'enabled': item['_eactive']}, event=True)
                        self.p_category[node.identifier] = {'title': node.title, 'uid': node.UID(),
                                                            'enabled': node.enabled, 'obj': node}
                        item['_pcode'], item['_ptitle'] = node.identifier, node.title
                        item['_puid'], item['_pactive'] = node.UID(), node.enabled
                        item['_act'] = 'N'
                item['_type'] = 'ClassificationCategory'
                item['title'] = item.get('_ptitle', '')
                short_log(item)
                if not self.storage['commit']:
                    continue
            yield item


def get_contact_name(dic, dic2):
    sender = []
    p_sender = []
    p_name = u' '.join(all_of_dict_values(dic2, ['lastname', 'firstname']))
    if not p_name and dic2.get('_name2'):
        p_name = dic2['_name2']
    if p_name:
        p_sender.append(p_name)
    name = u' '.join(all_of_dict_values(dic, ['lastname', 'firstname']))
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
    # e_contact = _user_id _ctyp lastname firstname _ptitle _street _pc _city _email1 _email2 _email3 _function _e_nb
    # _cell1 _cell2 _cell3 _web _org _name2 _parent_id _addr_id
    change = False
    sender = []
    p_sender = []
    m_sender = clean_value(item[free_fld], patterns=[r'^["\']+$'])
    if c_id_fld and item[c_id_fld]:
        infos = section.storage['data']['e_contact'][item[c_id_fld]]
        parent_infos = {}
        if infos['_parent_id']:
            parent_infos = section.storage['data']['e_contact'].get(infos['_parent_id'], {})
        sender, p_sender = get_contact_name(infos, parent_infos)
        if p_sender:
            change = True
            dest1.append(u'{} PARENT: {}.'.format(label, u', '.join(p_sender)))
            dest2.append(u'{} PARENT: {}.'.format(label, u', '.join(p_sender)))
        if sender:
            change = True
            dest1.append(u'{}: {}.'.format(label, u', '.join(sender)))
            dest2.append(u'{}: {}.'.format(label, u', '.join(sender)))
        # address
        p_address = all_of_dict_values(parent_infos, ['_street', '_pc', '_city'])
        address = all_of_dict_values(infos, ['_street', '_pc', '_city'])
        if address:
            change = True
            dest2.append(u'ADRESSE {}: {}.'.format(label, u' '.join(address)))
        elif p_address:  # we add just one address
            change = True
            dest2.append(u'ADRESSE {} PARENT: {}.'.format(label, u' '.join(p_address)))
        else:
            pass  # include _addr_id ? no!
        # phones
        phones = get_contact_phone(infos, parent_infos)
        if phones:
            change = True
            dest2.append(u'TÉL {}: {}.'.format(label, u', '.join(phones)))
    if m_sender:
        lines = m_sender.split('\n')
        change = True
        dest1.append(u'{} LIBRE: {}'.format(label, lines[0]))
        if lines:
            dest2.append(u'{} LIBRE: {}'.format(label, u', '.join(lines)))
    return change


class HContactTypeUpdate(object):
    """Store contact type if necessary.

    Parameters:
        * condition = O, condition expression
        * clean_unused = O, clean unmatched plone values (default 0)
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.new_val = {}
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.clean_unused = bool(int(options.get('clean_unused') or '0'))

    def __iter__(self):
        p_types = self.storage['data']['p_dir_org_types']
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item, storage=self.storage):
                course_store(self)
                if not item['_pid']:
                    log_error(item, u'Empty match: we pass _eid {}'.format(item['_eid']))
                    continue
                elif item['_pid'] not in p_types:
                    p_types[item['_pid']] = {'name': item['_ptitle'], '_used': True}
                else:
                    p_types[item['_pid']]['_used'] = True
                continue
            yield item

        if len(p_types) != self.storage['data']['p_dir_org_types_len']:  # modified
            new_value = []
            for token in p_types.keys():
                if self.clean_unused and not p_types[token].get('_used', False) and token != 'non-defini':
                    del p_types[token]
                else:
                    new_value.append({u'token': token, u'name': p_types[token]['name']})
            o_logger.info("Part h: updating directory organization types with new value: {}".format(new_value))
            self.storage['plone']['directory'].organization_types = new_value


class I1ContactUpdate(object):
    """Add contact fields.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item, storage=self.storage):
                course_store(self)
                # mainly address
                if item['_addr_id']:
                    a_dic = self.storage['data']['e_address'][item['_addr_id']]
                    # _imp_addr _imp_pc _imp_city _imp_country _pc _city _box _street _number _phone _fax _cell
                    # _website _email
                    # has a structured address ?
                    if a_dic['_pc'] and a_dic['_city']:
                        item['zip_code'] = a_dic['_pc']
                        item['city'] = a_dic['_city']
                        if a_dic['_number']:
                            item['number'] = a_dic['_number']
                        if a_dic['_box']:
                            item['number'] = (item['number'] and u'{} bte {}'.format(item['number'], a_dic['_box']) or
                                              a_dic['_box'])
                        if a_dic['_street']:
                            item['street'] = a_dic['_street']
                        elif a_dic['_imp_addr']:
                            item['street'] = clean_value(a_dic['_imp_addr'], osep=', ')
                    # or use only imp
                    elif a_dic['_imp_addr']:
                        item['street'] = clean_value(a_dic['_imp_addr'], osep=', ')
                        if a_dic['_imp_pc']:
                            item['zip_code'] = a_dic['_imp_pc']
                        if a_dic['_imp_city']:
                            item['city'] = a_dic['_imp_city']
                    if a_dic['_imp_country'] and a_dic['_imp_country'].lower() != u'belgique':
                        item['country'] = a_dic['_imp_country']
                    if a_dic['_phone']:
                        item['phone'] = a_dic['_phone']
                    if a_dic['_cell']:
                        item['cell_phone'] = a_dic['_cell']
                    if a_dic['_fax']:
                        item['fax'] = a_dic['_fax']
                    if a_dic['_website']:
                        item['website'] = a_dic['_website']
                    if a_dic['_email']:
                        item['email'] = a_dic['_email']
                else:
                    # _ln _type _user_id _ctyp lastname firstname _ptitle _street _pc _city _phone2 _phone2 _phone3
                    # _email1 _email2 _email3 _function _e_nb _cell1 _cell2 _cell3 _web _org _name2 _parent_id _addr_id
                    if item.get('_street'):
                        item['street'] = clean_value(item['_street'], osep=', ')
                    if item.get('_pc'):
                        item['zip_code'] = item['_pc']
                    if item.get('_city'):
                        item['city'] = item['_city']
                    if item.get('_country') and item['_country'].lower() != u'belgique':
                        item['country'] = item['_country']
                # parent_address
                if item['_parent_id'] and not (item.get('city') and item.get('zip_code') or item.get('street')):
                    item['use_parent_address'] = True
                else:
                    item['use_parent_address'] = False
                # other
                if not item.get('phone'):
                    item['phone'] = one_of_dict_values(item, ['_phone1', '_phone2', '_phone3'])
                if not item.get('cell_phone'):
                    item['cell_phone'] = one_of_dict_values(item, ['_cell1', '_cell2', '_cell3'])
                if not item.get('email'):
                    item['email'] = one_of_dict_values(item, ['_email1', '_email2', '_email3'])
                if not item.get('_website'):
                    item['website'] = item['_web']
            yield item


class L1RecipientGroupsSet(object):
    """Handles recipient_groups.

    Parameters:
        * condition = O, condition expression
        * global_recipient_service = O, title of the global recipient group
        * global_recipient_tg_exceptions = O, list of eid (service) for which the global recipient is not set
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.grs_title = safe_unicode(options.get('global_recipient_service', ''))
        self.grs_uid = None
        found = [uid for uid in self.storage['data']['p_orgs_all']
                 if self.storage['data']['p_orgs_all'][uid]['ft'] == self.grs_title]
        if found:
            self.grs_uid = found[0]
        self.gr_tg_exc = safe_unicode(options.get('global_recipient_tg_exceptions', '')).strip().split()
        self.gr_tg_exc = [tup[0] for tup in pool_tuples(self.gr_tg_exc, 2, 'global_recipient_tg_exceptions option')]

    def __iter__(self):
        for item in self.previous:
            if not self.condition(item):
                yield item
                continue
            course_store(self)
            # create service if necessary
            if self.grs_uid is None and self.grs_title:
                path = u'{}/plonegroup-organization/reprise-de-donnees'.format(self.storage['plone']['directory_path'])
                item0 = {'_eid': item['_eid'], '_path': path, '_type': u'organization',
                         '_bpk': 'global_recipient_service', '_act': 'N', u'title': self.grs_title}
                yield item0
                obj = get_obj_from_path(self.portal, path=path)
                self.grs_uid = obj.UID()
                selected_orgs = get_registry_organizations()
                selected_orgs.append(self.grs_uid)
                set_registry_organizations(selected_orgs)
                self.storage['data']['p_orgs_all'], self.storage['data']['p_eid_to_orgs'] = get_plonegroup_orgs(
                    self.portal)
            if item['_service_id'] not in self.gr_tg_exc and self.grs_uid not in item.get('recipient_groups', []):
                item['recipient_groups'] = [self.grs_uid]
            yield item


class L1SenderAsTextSet(object):
    """Handles contact"""
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        # do we use contact id field ?
        self.contact_id_key = '_sender_id'
        if os.path.exists(full_path(self.storage['csvp'],
                          transmogrifier.get('h__contact_type_match_read', {}).get('filename', '_not_found_'))):
            self.contact_id_key = ''

    def __iter__(self):
        for item in self.previous:
            if not self.condition(item):
                yield item
                continue
            course_store(self)
            desc = 'description' in item and item.get('description').split('\r\n') or []
            d_t = 'data_transfer' in item and item.get('data_transfer').split('\r\n') or []
            if get_contact_info(self, item, u'EXPÉDITEUR', self.contact_id_key, '_sender', desc, d_t):
                item['description'] = u'\r\n'.join(desc)
                item['data_transfer'] = u'\r\n'.join(d_t)
            yield item


class M1AssignedUserHandling(object):
    """Handles assigned user.

    Parameters:
        * store_key = M, storage main key to find mail path
        * condition = O, condition expression
    """
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
        store_key = safe_unicode(options['store_key'])
        self.im_paths = self.storage['data'][store_key]
        self.contacts = self.storage['data']['e_contacts_sender']  # user only
        self.user_match = self.storage['data']['e_user_match']
        # self.e_user_service = self.storage['data']['e_user_service']
        self.p_user_service = self.storage['data']['p_user_service']  # plone user service
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
            course_store(self)
            if item['_contact_id'] not in self.contacts:
                o_logger.warning("eid '%s', contact id not a user '%s'", item['_eid'], item['_contact_id'])
                continue
            e_userid = self.contacts[item['_contact_id']]['_user_id']
            p_userid = self.user_match[e_userid]['_p_userid']
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
            r_infos = u"DESTINATAIRE: {}, {}".format(r_name, r_messages and u', {}'.format(r_messages) or u'')
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


class POMSenderSet(object):
    """Set sender held_position.

    Parameters:
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
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
            course_store(self)
            if item['_sender_id']:
                if self.e_c_s[item['_sender_id']]['_user_id']:  # we have a user id
                    e_userid = self.e_c_s[item['_sender_id']]['_user_id']
                else:  # _sender_id is not a user id
                    e_userid = u'None'
                    # add info in data_transfer
                    desc = 'description' in item and item.get('description').split('\r\n') or []
                    d_t = 'data_transfer' in item and item.get('data_transfer').split('\r\n') or []
                    if get_contact_info(self, item, u'EXPÉDITEUR', '_sender_id', '_sender', desc, d_t):
                        item['description'] = u'\r\n'.join(desc)
                        item['data_transfer'] = u'\r\n'.join(d_t)
            else:  # we do not have a _sender_id
                e_userid = u'None'
            pers_userid = self.euid_to_pers[e_userid]
            ouid = self.eid_to_orgs[item['_service_id']]['uid']
            hp_dic = self.p_hps[pers_userid]['hps'][ouid]
            item['sender'] = hp_dic['puid']
            o_logger.debug(u"OM sender info '%s'. Sender '%s', Description '%r', Data '%r'", item['_eid'],
                           item['sender'], item.get('description', u''), item.get('data_transfer', u''))
            yield item


class ParentPathInsert(object):
    """Add parent path key following type.

    Parameters:
        * bp_key = O, blueprint key (here used to get parent path from pickle
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.bp_key = safe_unicode(options.get('bp_key', None))
        self.im_folder = self.portal['incoming-mail']
        self.om_folder = self.portal['outgoing-mail']

    def __iter__(self):
        for item in self.previous:
            ptyp = item.get('_type')
            if not ptyp or '_path' in item or '_parenth' in item:
                yield item
                continue
            course_store(self)
            if ptyp in ('dmsincomingmail', 'dmsincoming_email'):
                container = create_period_folder(self.im_folder, item['creation_date'])
                item['_parenth'] = u'/incoming-mail/{}'.format(container.id)
            elif ptyp == 'dmsoutgoingmail':
                container = create_period_folder(self.om_folder, item['creation_date'])
                item['_parenth'] = u'/outgoing-mail/{}'.format(container.id)
            elif ptyp == 'person':
                item['_parenth'] = u'/contacts'
            elif ptyp == 'organization':
                item['_parenth'] = u'/contacts'
                if item['_parent_id']:
                    if item['_parent_id'] not in self.storage['data'][self.bp_key]:
                        log_error(item, u"Parent id not found '{}'".format(item['_parent_id']))
                    else:
                        item['_parenth'] = self.storage['data'][self.bp_key][item['_parent_id']]['path']
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
                course_store(self)
                eid = item[u'internal_number']
                uid = get_obj_from_path(self.portal, item).UID()
                self.storage['data']['p_euid_to_pers'][eid] = uid
                if uid not in self.storage['data']['p_hps']:
                    self.storage['data']['p_hps'][uid] = {'path': item['_path'], 'eid': eid, 'hps': {},
                                                          'state': 'deactivated'}
            if self.storage['commit'] and self.storage['commit_nb'] and \
                    self.storage['count']['commit_count']['']['c'] % self.storage['commit_nb'] == 0:
                transaction.commit()
                o_logger.info(u"Commit in '{}' at {}".format(item['_bpk'],
                                                             self.storage['count']['commit_count']['']['c']))
                for filename, store_key, condition in self.storage['lastsection']['pkl_dump']:
                    if filename and condition(None, storage=self.storage):
                        o_logger.info(u"Dumping '{}'".format(filename))
                        with open(filename, 'wb') as fh:
                            cPickle.dump(self.storage['data'][store_key], fh, -1)
            yield item


class Q1RecipientsAsTextUpdate(object):
    """Handles om recipients.

    Parameters:
        * condition = O, condition expression
        * store_key = M, storage main key to find mail path
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        store_key = safe_unicode(options['store_key'])
        self.om_paths = self.storage['data'][store_key]
        self.e_c = self.storage['data']['e_contact']
        # do we use contact id field ?
        self.contact_id_key = '_contact_id'
        if os.path.exists(full_path(self.storage['csvp'],
                          transmogrifier.get('h__contact_type_match_read', {}).get('filename', '_not_found_'))):
            self.contact_id_key = ''

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            course_store(self)
            omail = get_obj_from_path(self.portal, path=self.om_paths[item['_mail_id']]['path'])
            if omail is None:
                o_logger.warning("mail %s: path '%s' not found", item['_mail_id'],
                                 self.om_paths[item['_mail_id']]['path'])
                continue
            item2 = {'_eid': item['_eid'], '_path': self.om_paths[item['_mail_id']]['path'],
                     '_type': omail.portal_type, '_bpk': 'o_recipients', '_act': 'U'}
            desc = (omail.description or u'').split('\r\n')
            d_t = (omail.data_transfer or u'').split('\r\n')
            if get_contact_info(self, item, u'DESTINATAIRE', self.contact_id_key, '_comment', desc, d_t):
                item2['description'] = u'\r\n'.join(desc)
                r_messages = u', '.join(all_of_dict_values(item, [u'_action', u'_message', u'_response'],
                                                           labels=[u'', u'message', u'réponse']))
                if r_messages:
                    r_messages = u'{}: {}'.format(u'COMPLÉMENT DESTINATAIRE', u', {}'.format(r_messages))
                    if r_messages not in d_t:
                        d_t.append(r_messages)
                item2['data_transfer'] = u'\r\n'.join(d_t)
                yield item2


class S1ClassificationFoldersUpdate(object):
    """Handles classification folders assignments.

    Parameters:
        * condition = O, condition expression
        * store_key = M, storage main key to find mail path
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        store_key = safe_unicode(options['store_key'])
        self.paths = self.storage['data'][store_key]

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            course_store(self)
            mail_path = self.paths[item['_eid']]['path']
            mail = get_obj_from_path(self.portal, path=mail_path)
            item2 = {'_eid': item['_eid'], '_folder_id': item['_folder_id'], '_bpk': u'classification_folders',
                     '_path': mail_path, '_type': mail.portal_type, '_act': 'U'}
            change = False
            folder_id = item['_folder_id']
            if folder_id in self.storage['data']['p_irn_to_folder']:
                cf = mail.classification_folders or []
                folder_uid = self.storage['data']['p_irn_to_folder'][folder_id]['uid']
                if folder_uid not in cf:
                    cf.append(folder_uid)
                    item2['classification_folders'] = cf
                    change = True
            else:
                log_error(item, u"Cannot find folder_id in plone '{}' => put in in description".format(folder_id))
                desc = mail.description and mail.description.split(u'\r\n') or []
                folder_tit = u"DOSSIER: {}".format(self.storage['data']['e_folder'][folder_id]['_title'])
                if folder_tit not in desc:
                    desc.append(folder_tit)
                    item2['description'] = u'\r\n'.join(desc)
                    change = True
            if change:
                yield item2


class T1DmsfileCreation(object):
    """Create file.

    Parameters:
        * bp_key = M, blueprint key
        * store_key = M, storage main key to find mail path
        * condition = O, condition expression
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.bp_key = safe_unicode(options['bp_key'])
        store_key = safe_unicode(options['store_key'])
        self.paths = self.storage['data'][store_key]
        self.files = {}
        self.ext = {}

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            course_store(self)
            order = item['_order'] is not None and int(item['_order']) or None
            # self.ext.setdefault(item['_ext'].lower(), {'c': 0})['c'] += 1
            if item['_mail_id'] not in self.files:
                if self.bp_key == u'e_dmsfile_i':
                    typ = 'dmsmainfile'
                elif self.bp_key == u'e_dmsfile_o':
                    typ = 'dmsommainfile'
                else:
                    typ = 'dmsappendixfile'
                self.files[item['_mail_id']] = {'lo': order, 'ids': [item['_eid']]}
                # if item['_desc'] != u'Fichier scanné':
                #     e_logger.warn(u"eid:{}, mid:{}: not fichier scanné".format(item['_eid'], item['_mail_id']))
            else:
                typ = 'dmsappendixfile'
                # if order is None and self.files[item['_mail_id']]['lo'] is None:
                #     pass
                #     e_logger.warn(u"eid:{}, mid:{}: order is None and previous too".format(item['_eid'],
                #                                                                            item['_mail_id']))
                # elif order is None:
                #     e_logger.warn(u"eid:{}, mid:{}: order is None while previous not".format(item['_eid'],
                #                                                                              item['_mail_id']))
                # elif self.files[item['_mail_id']]['lo'] is None:
                #     e_logger.warn(u"eid:{}, mid:{}: order not None while previous is None".format(item['_eid'],
                #                                                                                   item['_mail_id']))
                # elif order != self.files[item['_mail_id']]['lo']+1:
                #     e_logger.warn(u"eid:{}, mid:{}: order discontinuity {} <> {}".format(
                #         item['_eid'], item['_mail_id'], order, self.files[item['_mail_id']]['lo']))
                # elif order != len(self.files[item['_mail_id']]['ids'])+1:
                #     e_logger.warn(u"eid:{}, mid:{}: order discordance {} <> {}".format(
                #         item['_eid'], item['_mail_id'], order, len(self.files[item['_mail_id']]['ids'])))
                self.files[item['_mail_id']]['lo'] = order
                self.files[item['_mail_id']]['ids'].append(item['_eid'])
            item2 = {'_eid': item['_eid'], '_parenth': self.paths[item['_mail_id']]['path'],
                     '_type': typ, '_bpk': self.bp_key, 'label': item['_desc'], '_id': item['_eid'],
                     'title': item['_desc'],
                     'creation_date': item['creation_date'], 'modification_date': item['creation_date']}
            # get file content
            new_ext, file_content = get_file_content(self, item)
            if file_content is None:
                e_logger.error(u"Cannot open filename '{}'".format(new_ext))
            else:
                filename = item['_filename']
                (basename, ext) = os.path.splitext(filename)
                if not ext:
                    filename = u'{}{}'.format(filename, new_ext)
                item2['file'] = {'data': file_content, 'filename': filename}
                item2['title'] = filename
            yield item2

        # o_logger.info(self.ext)


class X1ReplyToUpdate(object):
    """Handles mail links (reply_to field).

    Parameters:
        * condition = O, condition expression
        * store_key = M, storage main key to find mail path
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        self.intids = getUtility(IIntIds)
        self.catalog = getUtility(ICatalog)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        store_key = safe_unicode(options['store_key'])
        self.paths = self.storage['data'][store_key]

    def __iter__(self):
        for item in self.previous:
            if not is_in_part(self, self.parts) or not self.condition(item):
                yield item
                continue
            course_store(self)
            source_path = self.paths[item['_eid']]['path']  # source is the om
            target_path = self.paths[item['_target_id']]['path']  # target is the im
            source = get_obj_from_path(self.portal, path=source_path)
            target = get_obj_from_path(self.portal, path=target_path)
            source_id = self.intids.getId(source)
            target_id = self.intids.getId(target)
            reply_to = source.reply_to or []
            reply_to_ids = [rv.to_id for rv in reply_to]  # existing targets
            reply_to_ids_len = len(reply_to_ids)
            reply_to_ids.append(source_id)  # to avoid self relation in catalog search
            back_rels_ids = [rv.from_id for rv in self.catalog.findRelations({'from_attribute': 'reply_to',
                                                                              'to_id': source_id})]
            reply_to_ids.extend(back_rels_ids)  # to avoid "looping" relations causing problems
            # imail itself
            if target_id not in reply_to_ids:
                reply_to.append(RelationValue(target_id))
                reply_to_ids.append(target_id)
            # get directly imail linked mails
            if target.reply_to:
                ids_to_add = [rv.to_id for rv in target.reply_to if rv.to_id not in reply_to_ids]
                for _id in ids_to_add:
                    reply_to.append(RelationValue(_id))
                    reply_to_ids.append(_id)
            # we get backrefs too
            for ref in self.catalog.findRelations({'to_id': target_id, 'from_attribute': 'reply_to'}):
                if ref.from_id not in reply_to_ids:
                    reply_to.append(RelationValue(ref.from_id))
                    reply_to_ids.append(ref.from_id)
            if reply_to_ids_len == len(reply_to):
                continue
            item2 = {'_eid': item['_eid'], '_target_id': item['_target_id'], '_bpk': 'reply_to',
                     '_path': source_path, '_type': source.portal_type, '_act': 'U', 'reply_to': reply_to}
            yield item2


class WorkflowHistoryUpdate(object):
    """Update workflow history.

    Parameters:
        * owner = M, owner id
        * condition = O, condition expression
    """

    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.owner_id = options['owner']
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                course_store(self)
                obj = get_obj_from_path(self.portal, item)
                for wkf in obj.workflow_history or {}:
                    change = False
                    wfh = []
                    for status in obj.workflow_history[wkf]:
                        if status['actor'] != self.owner_id:
                            status['actor'] = self.owner_id
                            change = True
                        wfh.append(status)
                    if change:
                        obj.workflow_history[wkf] = tuple(wfh)
            yield item
