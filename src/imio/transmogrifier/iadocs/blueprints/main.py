# -*- coding: utf-8 -*-
from collections import OrderedDict
from collective.classification.tree.utils import create_category
from collective.contact.plonegroup.config import get_registry_organizations
from collective.contact.plonegroup.config import set_registry_organizations
from collective.documentviewer.settings import GlobalSettings
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from copy import copy
from datetime import date
from datetime import datetime
from DateTime.DateTime import DateTime
from imio.dms.mail import ARCHIVE_SITE
from imio.dms.mail.browser.settings import IImioDmsMailConfig
from imio.helpers.security import generate_password
from imio.helpers.transmogrifier import clean_value
from imio.helpers.transmogrifier import Condition
from imio.helpers.transmogrifier import Expression
from imio.helpers.transmogrifier import filter_keys
from imio.helpers.transmogrifier import get_correct_path
from imio.helpers.transmogrifier import get_main_path
from imio.helpers.transmogrifier import get_obj_from_path
from imio.helpers.transmogrifier import pool_tuples
from imio.helpers.transmogrifier import relative_path
from imio.helpers.transmogrifier import split_text
from imio.helpers.transmogrifier import str_to_bool
from imio.helpers.transmogrifier import str_to_date
from imio.pyutils.system import full_path
from imio.pyutils.system import read_recursive_dir
from imio.pyutils.system import stop
from imio.pyutils.utils import setup_logger
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import course_print
from imio.transmogrifier.iadocs.utils import course_store
from imio.transmogrifier.iadocs.utils import get_categories
from imio.transmogrifier.iadocs.utils import get_folders
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_org_level
from imio.transmogrifier.iadocs.utils import get_personnel
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import get_related_parts
from imio.transmogrifier.iadocs.utils import get_users
from imio.transmogrifier.iadocs.utils import get_users_groups
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from imio.transmogrifier.iadocs.utils import print_item  # noqa
from plone import api
from plone.dexterity.fti import DexterityFTIModificationDescription
from plone.dexterity.fti import ftiModified
from plone.i18n.normalizer import IIDNormalizer
from Products.CMFPlone.utils import safe_unicode
from Products.cron4plone.browser.configlets.cron_configuration import ICronConfiguration
from zope.annotation import IAnnotations
from zope.component import getUtility
from zope.component import queryUtility
from zope.interface import classProvides
from zope.interface import implements
from zope.lifecycleevent import ObjectModifiedEvent
from zope.schema.interfaces import IVocabularyFactory

import cPickle
import csv
import json
import logging
import os


class Initialization(object):
    """Initializes global variables to be used in next sections.

    Parameters:
        * basepath = O, absolute directory. If empty, buildout dir will be used.
        * subpath = O, if given, it will be appended to basepath.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        workingpath = get_main_path(safe_unicode(options.get('basepath') or ''),
                                    safe_unicode(options.get('subpath') or ''))
        csvpath = safe_unicode(options.get('csvpath') or '')
        if not csvpath:
            csvpath = workingpath
        filespath = safe_unicode(options.get('filespath') or '')
        if not filespath:
            filespath = workingpath
        inb_types = safe_unicode(transmogrifier['config'].get('internal_number_behavior_types') or '').split()
        dtb_types = safe_unicode(transmogrifier['config'].get('data_transfer_behavior_types') or '').split()
        creator = transmogrifier['config'].get('creator')
        if creator and not api.portal.get_tool('acl_users').getUserById(creator):
            api.user.create(u'csv@imio.be', creator, generate_password())
        if bool(int(transmogrifier['config'].get('debug') or '0')):
            setup_logger(o_logger, level=logging.DEBUG)
        # get site name
        site_name = self.portal.getPhysicalPath()[-1]
        # setting logs
        efh = logging.FileHandler(os.path.join(workingpath, '{}_dt_input_errors.log'.format(site_name)), mode='w')
        efh.setFormatter(logging.Formatter('(%(levelname).1s) %(message)s'))
        efh.setLevel(logging.INFO)
        e_logger.addHandler(efh)
        ofh = logging.FileHandler(os.path.join(workingpath, '{}_dt.log'.format(site_name)), mode='w')
        ofh.setFormatter(logging.Formatter('(%(levelname).1s) %(message)s'))
        ofh.setLevel(logging.INFO)
        o_logger.addHandler(ofh)
        run_options = json.loads(transmogrifier.context.REQUEST.get('_transmo_options_') or '{}')
        start_msg = u"STARTING '{}' parts at {}".format(run_options['parts'], datetime.now().strftime('%Y%m%d-%H%M'))
        if run_options['commit']:
            ecfh = logging.FileHandler(os.path.join(workingpath, '{}_dt_input_errors_commit.log'.format(site_name)),
                                       mode='a')
            ecfh.setFormatter(logging.Formatter('(%(levelname).1s) %(message)s'))
            ecfh.setLevel(logging.INFO)
            e_logger.addHandler(ecfh)
            ocfh = logging.FileHandler(os.path.join(workingpath, '{}_dt_commit.log'.format(site_name)),
                                       mode='a')
            ocfh.setFormatter(logging.Formatter('(%(levelname).1s) %(message)s'))
            ocfh.setLevel(logging.INFO)
            o_logger.addHandler(ocfh)
            o_logger.info(start_msg)

        # check package installation and configuration
        if inb_types:
            if not self.portal.portal_quickinstaller.isProductInstalled('collective.behavior.internalnumber'):
                self.portal.portal_setup.runAllImportStepsFromProfile(
                    'profile-collective.behavior.internalnumber:default', dependency_strategy='new')
                o_logger.info('Installed collective.behavior.internalnumber')
            reg_key = 'collective.behavior.internalnumber.browser.settings.IInternalNumberConfig.portal_type_config'
            inptc = list(api.portal.get_registry_record(reg_key))
            for typ in inb_types:
                if not [dic for dic in inptc if dic['portal_type'] == typ]:
                    inptc.append({'portal_type': typ, 'uniqueness': True, 'default_expression': None,
                                  'default_number': 1})
                    api.portal.set_registry_record(reg_key, inptc)
                    o_logger.info('Added internalnumber config for type {}'.format(typ))
                fti = getattr(self.portal.portal_types, typ)
                if 'collective.behavior.internalnumber.behavior.IInternalNumberBehavior' not in fti.behaviors:
                    old_bav = tuple(fti.behaviors)
                    fti.behaviors = tuple(list(fti.behaviors) +
                                          ['collective.behavior.internalnumber.behavior.IInternalNumberBehavior'])
                    ftiModified(fti, ObjectModifiedEvent(fti,
                                                         DexterityFTIModificationDescription('behaviors', old_bav)))
                    o_logger.info('Added internalnumber behavior on type {}'.format(typ))
        if dtb_types:
            fields_conf = {'dmsincomingmail': 'imail', 'dmsoutgoingmail': 'omail'}
            for typ in dtb_types:
                fti = getattr(self.portal.portal_types, typ)
                if 'imio.dms.mail.content.behaviors.IDmsMailDataTransfer' not in fti.behaviors:
                    old_bav = tuple(fti.behaviors)
                    fti.behaviors = tuple(list(fti.behaviors) +
                                          ['imio.dms.mail.content.behaviors.IDmsMailDataTransfer'])
                    ftiModified(fti, ObjectModifiedEvent(fti,
                                                         DexterityFTIModificationDescription('behaviors', old_bav)))
                    o_logger.info('Added datatransfer behavior on type {}'.format(typ))
                if typ in fields_conf:
                    reg = 'imio.dms.mail.browser.settings.IImioDmsMailConfig.{}_fields'.format(fields_conf[typ])
                    rec = api.portal.get_registry_record(reg)
                    if not [dic for dic in rec if dic['field_name'] == 'IDmsMailDataTransfer.data_transfer']:
                        lst = list(rec)
                        lst.append({"field_name": u'IDmsMailDataTransfer.data_transfer', "read_tal_condition": None,
                                    "write_tal_condition": None})
                        api.portal.set_registry_record(reg, lst)
                        o_logger.info('Added data_transfer field in type {}'.format(typ))
        if ARCHIVE_SITE:
            cssr = self.portal.portal_css
            if not cssr.getResource('imiodmsmail_archives.css').getEnabled():
                cssr.updateStylesheet('imiodmsmail_archives.css', enabled=True)
                cssr.cookResources()
            cron_configlet = queryUtility(ICronConfiguration, 'cron4plone_config')
            if cron_configlet.cronjobs:
                cron_configlet.cronjobs = []

        # set global variables in annotation
        self.storage = IAnnotations(transmogrifier).setdefault(ANNOTATION_KEY, {})
        self.storage['o_logger'] = o_logger
        self.storage['wp'] = workingpath
        self.storage['csvp'] = csvpath
        self.storage['filesp'] = filespath
        self.storage['creation_date'] = str_to_date(transmogrifier['config'], 'default_creation_date', log_error,
                                                    fmt='%Y%m%d-%H%M', as_date=False)
        self.storage['csv'] = {}
        self.storage['data'] = {}
        self.storage['course'] = OrderedDict()
        course_store(self, None)
        self.storage['parts'] = run_options['parts']
        self.storage['commit'] = run_options['commit']
        self.storage['commit_nb'] = run_options['commit_nb']
        self.storage['batch_nb'] = run_options['batch_nb']
        self.storage['plone'] = {}
        self.storage['lastsection'] = {'pkl_dump': []}
        # store storage on transmogrifier, so it can be used with standard condition
        transmogrifier.storage = self.storage
        if is_in_part(self, 'tuv'):
            gsettings = GlobalSettings(self.portal)
            gsettings.auto_convert = False

        # store fullname order
        start = api.portal.get_registry_record('omail_fullname_used_form', IImioDmsMailConfig, default='firstname')
        self.storage['plone']['firstname_first'] = (start == 'firstname')
        # find directory
        brains = api.content.find(portal_type='directory')
        if brains:
            self.storage['plone']['directory'] = brains[0].getObject()
            self.storage['plone']['directory_path'] = relative_path(self.portal, brains[0].getPath())
        else:
            raise Exception("{}: Directory not found !".format(name))
        # store directory configuration
        for typ in ['types', 'levels']:
            key = 'p_dir_org_{}'.format(typ)
            self.storage['data'][key] = OrderedDict(
                [(safe_unicode(t['token']), {'name': safe_unicode(t['name'])}) for t in
                 getattr(self.storage['plone']['directory'], 'organization_%s' % typ)])
            if not len(self.storage['data'][key]):
                self.storage['data'][key] = OrderedDict([(u'non-defini', u'Non défini')])
            self.storage['data']['{}_len'.format(key)] = len(self.storage['data'][key])
        # create default contact
        for param, service in (('default_contact', False), ('generic_service', True)):
            params = next(csv.reader([transmogrifier['config'].get(param).strip()], delimiter=' ',
                                     quotechar='"', skipinitialspace=True))
            obj = get_obj_from_path(self.portal, path=params[0])
            if obj is None:
                obj = api.content.create(self.portal.unrestrictedTraverse('/'.join(params[0].split('/')[0:-1])),
                                         'organization', id=params[0].split('/')[-1], title=params[1].decode('utf8'))
            if service:
                selected_orgs = get_registry_organizations()
                if obj.UID() not in selected_orgs:
                    selected_orgs.append(obj.UID())
                    set_registry_organizations(selected_orgs)
            else:
                self.storage['plone']['def_contact'] = obj
        # store services
        self.storage['data']['p_orgs_all'], self.storage['data']['p_eid_to_orgs'] = get_plonegroup_orgs(self.portal)
        # store mailtypes
        self.storage['data']['p_mailtype'] = get_mailtypes(self.portal)
        # store users
        self.storage['data']['p_user'] = get_users(self.portal)
        # store personnel
        (self.storage['data']['p_userid_to_pers'], self.storage['data']['p_euid_to_pers'],
         self.storage['data']['p_hps']) = get_personnel(self.portal)
        # store categories
        self.storage['data']['p_category'] = get_categories(self.portal)
        if ('f' in self.storage['parts'] and
                'f__to_correct_only' in transmogrifier['transmogrifier']['pipeline'].split('\n')):
            self.storage['data']['p_categories'] = \
                getUtility(IVocabularyFactory, 'collective.classification.vocabularies:fulltree')(self.portal)
        # create default classification category
        def_cat = transmogrifier['config'].get('default_category') or u''
        if def_cat:
            code, title = next(csv.reader([transmogrifier['config'].get('default_category').strip()], delimiter=' ',
                                          quotechar='"', skipinitialspace=True))
            if code not in self.storage['data']['p_category']:
                node = create_category(self.portal.tree, {'identifier': safe_unicode(code),
                                                          'title': safe_unicode(title), 'enabled': False},
                                       event=True)
                o_logger.info('Created default category {}'.format(code))
                self.storage['data']['p_category'][node.identifier] = {'title': node.title, 'uid': node.UID(),
                                                                       'enabled': node.enabled, 'obj': node}
            self.storage['plone']['def_category'] = self.storage['data']['p_category'][code]['uid']
        # store classification folders
        if 'f' in self.storage['parts'] or 's' in self.storage['parts']:
            (self.storage['data']['p_folder_uid'], self.storage['data']['p_irn_to_folder'],
             self.storage['data']['p_folder_full_title']) = get_folders(self)
        # store already imported mails
        self.storage['data']['p_mail_ids'] = {}  # TODO will replace pkl file ?
        # store user groups
        self.storage['data']['p_user_service'] = get_users_groups(self.portal, self.storage['data']['p_user'])
        # adapt im counter
        counter = api.portal.get_registry_record('collective.dms.mailcontent.browser.settings.IDmsMailConfig.'
                                                 'incomingmail_number')
        if counter > 1:
            brains = self.portal.portal_catalog.unrestrictedSearchResults(portal_type=['dmsincoming_email',
                                                                                       'dmsincomingmail'])
            if not len(brains):
                api.portal.set_registry_record('collective.dms.mailcontent.browser.settings.IDmsMailConfig.'
                                               'incomingmail_number', 1)
        # adapt om counter
        counter = api.portal.get_registry_record('collective.dms.mailcontent.browser.settings.IDmsMailConfig.'
                                                 'outgoingmail_number')
        if counter > 2:
            brains = self.portal.portal_catalog.unrestrictedSearchResults(portal_type=['dmsoutgoingmail'])
            if len(brains) <= 1:
                api.portal.set_registry_record('collective.dms.mailcontent.browser.settings.IDmsMailConfig.'
                                               'outgoingmail_number', 2)
        # adapt folder counter
        counter = api.portal.get_registry_record('collective.classification.folder.browser.settings.'
                                                 'IClassificationConfig.folder_number')

        # deactivate versioning
        pr_tool = api.portal.get_tool('portal_repository')
        self.storage['plone']['pr_vct'] = tuple(pr_tool._versionable_content_types)
        pr_tool._versionable_content_types[:] = ()

    def __iter__(self):
        for item in self.previous:
            yield item


class AddDataInItem(object):
    """Add item keys from a stored dictionary.

    Parameters:
        * b_condition = O, blueprint condition expression
        * bp_key = M, blueprint key
        * related_storage = M, storage to get data
        * store_key = M, store key in data. The data is gotten from storage[{related_storage}][{store_key}]
        * store_subkey = O, storing sub key. If defined, the item is gotten from
          storage[{related_storage}][{store_key}][{store_subkey}]
        * fieldnames = O, fieldnames to add to item. All if nothing.
        * add_store_keys = O, flag to know if the store keys must be added in item (0 or 1: default 1)
        * prefix = O, prefix to add to fieldnames (to avoid collision with existing keys)
        * condition = O, condition expression
        * marker = 0, marker name added in '_mar_ker' key if given
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.related_storage = safe_unicode(options['related_storage'])
        if not is_in_part(self, self.parts):
            return
        b_condition = Condition(options.get('b_condition') or 'python:True', transmogrifier, name, options)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        if not b_condition(None, storage=self.storage):
            self.related_storage = None
            return
        self.store_key = safe_unicode(options['store_key'])
        self.store_subkey = safe_unicode(options.get('store_subkey'))
        self.bp_key = safe_unicode(options['bp_key'])
        self.fieldnames = safe_unicode(options.get('fieldnames') or '').split()
        self.add_store_keys = bool(int(options.get('add_store_keys') or '1'))
        self.prefix = safe_unicode(options.get('prefix', u''))
        self.marker = safe_unicode(options.get('marker', u''))

    def __iter__(self):
        ddic = self.storage['data'].get(self.related_storage, {})
        for item in self.previous:
            if is_in_part(self, self.parts) and self.related_storage is not None and self.condition(item):
                course_store(self, item)
                store_keys = {}
                vdic = ddic.get(item[self.store_key], {})
                if self.add_store_keys:
                    store_keys[u'{}{}'.format(self.prefix, self.store_key)] = vdic and item[self.store_key] or u''
                if self.store_subkey:
                    vdic = vdic.get(item[self.store_subkey], {})
                    if self.add_store_keys:
                        store_keys[u'{}{}'.format(self.prefix, self.store_subkey)] = (vdic and item[self.store_subkey]
                                                                                      or u'')
                update_dic = {u'{}{}'.format(self.prefix, key): vdic.get(key, u'') for key in self.fieldnames}
                if self.marker:
                    item['_mar_ker'] = vdic and self.marker or u''
                item.update(update_dic)
                item.update(store_keys)
            yield item


class CommonInputChecks(object):
    """Checks input values of the corresponding external type.

    Parameters:
        * bp_key = M, blueprint key corresponding to csv
        * condition = O, condition expression
        * csv_key = O, csv key (default to bp_key)
        * strip_chars = O, list of triplets (fieldname chars type) on which a strip must be done
          (type: l=lstrip, s=strip, r=rstrip)
        * clean_value = 0, list of quintets (fieldname isep_expr strip patterns osep_expr)
          for which field multilines content will be cleaned
        * replace_newline = O, list of pairs (fieldname value) where newline will be replaced by value
        * invalids = O, list of pairs (fieldname values) for which field content will be replaced with None
          if it is equal to a value. values are | separated
        * split_text = O, list of septets (field length remainder_field remainder_position isep_expr osep_expr prefix)
          for which fieldname is split at length and remainder is put in remainder field
        * booleans = O, list of fields to transform in booleans
        * dates = O, list of triplets (fieldname format as_date) to transform in date
        * evals = O, list of fields that will be evaluated
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.bp_key = safe_unicode(options['bp_key'])
        self.csv_key = safe_unicode(options.get('csv_key', self.bp_key))
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            return
        fieldnames = self.storage['csv'].get(self.csv_key, {}).get('fd', [])
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.strips = safe_unicode(options.get('strip_chars', '')).strip().split()
        self.strips = [tup for tup in pool_tuples(self.strips, 3, 'strip_chars option') if tup[0] in fieldnames]
        self.cleans = next(csv.reader([options.get('clean_value', '').strip()], delimiter=' ', quotechar='"',
                                      skipinitialspace=True))
        self.cleans = [cell.decode('utf8') for cell in self.cleans]
        self.cleans = [(tup[0], Expression(tup[1], transmogrifier, name, options)(None), tup[2],
                        Expression(tup[3], transmogrifier, name, options)(None),
                        Expression(tup[4], transmogrifier, name, options)(None))
                       for tup in pool_tuples(self.cleans, 5, 'clean_value option') if tup[0] in fieldnames]
        self.repl_nl = next(csv.reader([options.get('replace_newline', '').strip()], delimiter=' ', quotechar='"',
                                       skipinitialspace=True))
        self.repl_nl = [cell.decode('utf8') for cell in self.repl_nl]
        self.repl_nl = [tup for tup in pool_tuples(self.repl_nl, 2, 'replace_newline option') if tup[0] in fieldnames]
        self.invalids = next(csv.reader([options.get('invalids', '').strip()], delimiter=' ', quotechar='"',
                                        skipinitialspace=True))
        self.invalids = [cell.decode('utf8') for cell in self.invalids]
        self.invalids = [tup for tup in pool_tuples(self.invalids, 2, 'invalids option') if tup[0] in fieldnames]
        self.splits = next(csv.reader([options.get('split_text', '').strip()], delimiter=' ', quotechar='"',
                                      skipinitialspace=True))
        self.splits = [cell.decode('utf8') for cell in self.splits]
        self.splits = [(tup[0], int(tup[1]), tup[2], int(tup[3]),
                        Expression(tup[4], transmogrifier, name, options)(None),
                        Expression(tup[5], transmogrifier, name, options)(None), tup[6]) for tup in
                       pool_tuples(self.splits, 7, 'splits option') if tup[0] in fieldnames]
        self.booleans = [key for key in safe_unicode(options.get('booleans', '')).split() if key in fieldnames]
        self.dates = next(csv.reader([options.get('dates', '').strip()], delimiter=' ', quotechar='"',
                                     skipinitialspace=True))
        self.dates = [cell.decode('utf8') for cell in self.dates]
        self.dates = [tup for tup in pool_tuples(self.dates, 3, 'dates option') if tup[0] in fieldnames]
        self.evals = [key for key in safe_unicode(options.get('evals', '')).split() if key in fieldnames]

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
                course_store(self, item)
                # strip chars
                for fld, chars, _typ in self.strips:
                    if not item[fld]:
                        continue
                    if _typ == 's':
                        item[fld] = item[fld].strip(chars)
                    elif _typ == 'l':
                        item[fld] = item[fld].lstrip(chars)
                    elif _typ == 'r':
                        item[fld] = item[fld].rstrip(chars)
                # clean multiline value
                for fld, isep, strip, patterns, osep in self.cleans:
                    item[fld] = clean_value(item[fld], isep, strip, patterns, osep)
                # replace newline by given value on specified fields
                for fld, val in self.repl_nl:
                    if u'\n' in (item[fld] or u''):
                        item[fld] = val.join([part.strip() for part in item[fld].split(u'\n') if part.strip()])
                # replace invalid values on specified fields
                for fld, values in self.invalids:
                    for value in values.split(u'|'):
                        if item[fld] == value:
                            item[fld] = None
                            break
                # split long value
                for fld, length, dest_fld, dest_pos, isep, osep, prefix in self.splits:
                    part1, part2 = split_text(item[fld], length)
                    if part1 != item[fld] or isep in item[fld]:
                        item[fld] = part1.replace(isep, ' ')
                        if part2:
                            remainder = dest_fld in item and item[dest_fld] and item[dest_fld].split(osep) or []
                            remainder.insert(dest_pos, u"{}{}".format(prefix, part2.replace(isep, osep)))
                            item[dest_fld] = osep.join(remainder)
                # to bool
                for fld in self.booleans:
                    item[fld] = str_to_bool(item, fld, log_error)
                # to dates
                for fld, fmt, as_date in self.dates:
                    item[fld] = str_to_date(item, fld, log_error, fmt=fmt, as_date=bool(int(as_date)),
                                            min_val=bool(int(as_date)) and date(1900, 1, 1) or datetime(1900, 1, 1))
                # evals
                for fld in self.evals:
                    item[fld] = eval(item[fld])
            yield item


class DependencySorter(object):
    """Handles dependencies.

    Parameters:
        * bp_key = M, blueprint key, input dictionary key
        * store_key = M, storing key for item. If defined, the item is stored in storage[{bp_key}][{store_key}]
        * condition = O, condition expression
        * parent_relation = M, parent_relation dictionary (key: {'_parent_id': 'xx'}
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.bp_key = options['bp_key']
        self.store_key = safe_unicode(options['store_key'])
        self.parent_relation = self.storage['data'].get(options['parent_relation'])

    def __iter__(self):
        for item in self.previous:
            yield item
        if self.parent_relation is None:
            return
        orig_dic = self.storage['data'].get(self.bp_key, {})
        for main_key in orig_dic:
            item = orig_dic[main_key]
            course_store(self, item)
            if not self.condition(item):
                continue
            if main_key in self.parent_relation and self.parent_relation[main_key]['_parent_id'] not in orig_dic:
                temp_item = copy(item)
                temp_item.update({'_bpk': self.bp_key, '_eid': main_key})  # to be used in log_error
                log_error(temp_item, u"Parent '{}' not found: please correct csv".format(
                    self.parent_relation[main_key]['_parent_id']))
            item['_level'] = get_org_level(self.parent_relation, main_key)


class FilesList(object):
    """Files list.

    Parameters:
        * bp_key = M, blueprint key
        * condition = O, condition expression
        * exclude_patterns = O, folder patterns list to exclude
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
        self.files = self.storage['data'].setdefault(self.bp_key, {})
        exclude_patterns = options.get('exclude_patterns', '').strip()
        excludes = exclude_patterns and Expression(exclude_patterns, transmogrifier, name, options)(None) or []
        if self.condition({}, storage=self.storage):
            course_store(self, None)
            for fp in sorted(read_recursive_dir(self.storage['filesp'], u'', exclude_patterns=excludes)):
                filename = os.path.basename(fp)
                basename, ext = os.path.splitext(filename)
                if basename not in self.files:
                    self.files[basename] = {'f': [(ext, os.path.dirname(fp))]}
                elif (ext, os.path.dirname(fp)) not in self.files[basename]['f']:
                    self.files[basename]['f'].append((ext, os.path.dirname(fp)))
            o_logger.info("Stored {} files info".format(len(self.files)))

    def __iter__(self):
        for item in self.previous:
            yield item


class InsertPath(object):
    """Adds _path if not yet defined, to create new items.

    Parameters:
        * bp_key = M, blueprint key representing csv
        * id_keys = M, fieldnames to use to create id.
        * csv_key = O, csv key (default to bp_key)
        * condition = O, condition expression
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.portal = transmogrifier.context
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        self.bp_key = safe_unicode(options['bp_key'])
        self.csv_key = safe_unicode(options.get('csv_key', self.bp_key))
        if not is_in_part(self, self.parts):
            return
        self.eids = self.storage['data'].setdefault(self.bp_key, {})
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        fieldnames = self.storage['csv'].get(self.csv_key, {}).get('fd', [])
        self.id_keys = [key for key in safe_unicode(options.get('id_keys', '')).split() if not fieldnames or
                        key in fieldnames]
        self.roe = bool(int(options.get('raise_on_error', '1')))

    def __iter__(self):
        idnormalizer = getUtility(IIDNormalizer)
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
                course_store(self, item)
                if '_path' in item:  # _path is already set
                    if '_act' not in item:
                        item['_act'] = 'N'
                    if item['_eid'] in self.eids:
                        item['_act'] = 'U'
                        if self.eids[item['_eid']].get('path') and item['_path'] != self.eids[item['_eid']]['path']:
                            log_error(item, u"2 differents path: new '{} <> existing '{}'".format(item['_path'],
                                      self.eids[item['_eid']]['path']))
                    self.eids.setdefault(item['_eid'], {})['path'] = item['_path']
                    yield item
                    continue
                if 'title' in item and item['title']:
                    title = item['title']
                else:
                    title = u'-'.join([item[key] for key in self.id_keys if item[key]])
                if not title:
                    log_error(item, u'cannot get an id from id keys {}'.format(self.id_keys), level='critical')
                    if self.roe:
                        raise Exception(u'No title ! See log...')
                    continue
                if item['_eid'] in self.eids and self.eids[item['_eid']].get('path'):  # already created
                    item['_path'] = self.eids[item['_eid']]['path']
                    item['_act'] = 'U'
                else:
                    if '_id' in item:
                        new_id = item['_id']
                    else:
                        new_id = idnormalizer.normalize(title)
                    item['_path'] = '/'.join([item['_parenth'], new_id])
                    item['_path'] = get_correct_path(self.portal, item['_path'])
                    item['_act'] = 'N'
                    self.eids.setdefault(item['_eid'], {})['path'] = item['_path']
            yield item


class LastSection(object):
    """Last section to do things at the end of each item process or global process.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.portal = transmogrifier.context

    def __iter__(self):
        for item in self.previous:
            yield item
        # end of process
        course_store(self, None)
        # dump pkl
        for filename, store_key, condition in self.storage['lastsection']['pkl_dump']:
            if filename and condition(None, storage=self.storage):
                o_logger.info(u"Dumping '{}'".format(filename))
                with open(filename, 'wb') as fh:
                    cPickle.dump(self.storage['data'][store_key], fh, -1)
        # activate dv auto convert
        if is_in_part(self, 'tuv'):
            gsettings = GlobalSettings(self.portal)
            gsettings.auto_convert = True
        # reactivate versioning
        if not ARCHIVE_SITE:
            pr_tool = api.portal.get_tool('portal_repository')
            pr_tool._versionable_content_types[:] = ()
            pr_tool._versionable_content_types.extend(self.storage['plone']['pr_vct'])
        course_print(self)
        # import ipdb; ipdb.set_trace()


class PickleData(object):
    """Pickle data.

    Parameters:
        * filename = M, filename to load and dump
        * store_key = M, store key in data dicts
        * d_condition = O, dump condition expression (default: False)
        * update = O, int for boolean to update (1) or set (0) storage. (default 0)
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.parts = get_related_parts(name)
        if not is_in_part(self, self.parts):
            self.filename = None
            return
        # import cPickle; fh = open('dt_csv_dir/2_e_mail_ass_user.pkl', 'rb')
        # dic = cPickle.load(fh); fh.close()
        self.filename = safe_unicode(options['filename'])
        if not self.filename:
            return
        self.store_key = safe_unicode(options['store_key'])
        self.filename = full_path(self.storage['csvp'], self.filename)
        self.storage['data'].setdefault(self.store_key, {})
        update = bool(int(options.get('update') or '0'))
        if os.path.exists(self.filename):
            o_logger.info(u"Loading '{}'".format(self.filename))
            with open(self.filename, 'rb') as fh:
                if update:
                    self.storage['data'][self.store_key].update(cPickle.load(fh))
                else:
                    self.storage['data'][self.store_key] = cPickle.load(fh)
        self.d_condition = Condition(options.get('d_condition') or 'python:False', transmogrifier, name, options)
        self.storage['lastsection']['pkl_dump'].append((self.filename, self.store_key, self.d_condition))

    def __iter__(self):
        for item in self.previous:
            yield item


class ReadFromData(object):
    """Read items from a dictionary.

    Parameters:
        * bp_key = M, blueprint key for item
        * store_key = M, storing keys for item. The item is read from storage[{bp_key}][{store_key}]
        * store_subkey = O, storing sub keys for item. If defined, the item is read from
          storage[{bp_key}][{store_key}][{store_subkey}]
        * fieldnames = O, fieldnames to get. All if nothing.
        * condition = O, condition expression to yield item
        * sort_value = O, python expression for sort method return (default: 'k')
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
        # self.store_key = safe_unicode(options['store_key']).split()
        self.store_key = safe_unicode(options['store_key'])
        self.store_subkey = safe_unicode(options.get('store_subkey'))
        self.bp_key = safe_unicode(options['bp_key'])
        self.fieldnames = safe_unicode(options.get('fieldnames') or '').split()
        # Using a lambda in expression is not working when a local var is used, even if given in context
        self.sort_value = Expression(options.get('sort_value') or 'k', transmogrifier, name, options)
        if self.bp_key not in self.storage['data']:
            self.storage['data'][self.bp_key] = {}

    def __iter__(self):
        for item in self.previous:
            yield item
        if not is_in_part(self, self.parts):
            return
        o_logger.info(u"Reading data from '{}'".format(self.bp_key))
        data = self.storage['data'][self.bp_key]

        def sort_method(k):
            return self.sort_value(None, k=k, data=data)

        for key in sorted(data, key=sort_method):
            if self.store_subkey:
                for skey in sorted(data[key].keys()):
                    course_store(self, item)
                    item = {'_bpk': self.bp_key, self.store_key: key, self.store_subkey: skey}
                    item.update(filter_keys(data[key][skey], self.fieldnames))
                    if self.condition(item):
                        yield item
            else:
                item = {'_bpk': self.bp_key, self.store_key: key}
                item.update(filter_keys(data[key], self.fieldnames))
                course_store(self, item)
                if self.condition(item):
                    yield item


class SetOwner(object):
    """Sets ownership on created object.

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
        self.owner = api.portal.get_tool('acl_users').getUserById(options['owner'])
        if not self.owner:
            stop("Section name '{}': owner not found '{}' !".format(name, options['owner']), o_logger)
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            if self.condition(item):
                course_store(self, item)
                obj = get_obj_from_path(self.portal, item)
                creators = list(obj.creators)
                # change creator metadata
                # if 'admin' in creators:
                #     creators.remove('admin')
                if self.owner_id not in creators:
                    creators.insert(0, self.owner_id)
                obj.setCreators(creators)
                # change owner with acl_users user !! (otherwise getOwner() fails)
                obj.changeOwnership(self.owner)
                # change Owner role
                # owners = obj.users_with_local_role('Owner')
                # if 'admin' in owners:
                #     obj.manage_delLocalRoles(['admin'])
                # if self.owner_id not in owners:
                #     roles = list(obj.get_local_roles_for_userid(self.owner_id))
                #     roles.append('Owner')
                #     obj.manage_setLocalRoles(self.owner_id, roles)
            yield item


class SetState(object):
    """Sets state in workflow history.

    Parameters:
        * workflow_id = M, workflow id
        * state_id = M, state id
        * action_id = M, action id
        * replace = O, state id
        * date_key = O, date key. Or don't change
        * actor = 0, actor
        * condition = O, condition expression
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
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
        self.workflow_id = options['workflow_id']
        self.state_id = options.get('state_id')
        self.action_id = options.get('action_id')
        self.replace = options.get('replace')
        self.actor = options.get('actor') or api.user.get_current().getId()  # username or userid ?
        self.date_key = options.get('date_key')
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.roe = bool(int(options.get('raise_on_error', '1')))

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item):
                course_store(self, item)
                try:
                    obj = self.portal.unrestrictedTraverse(safe_unicode(item['_path'][1:]).encode('utf8'))
                except (AttributeError, KeyError):
                    log_error(item, "The corresponding object '{}' cannot be found".format(item['_path']))
                    continue
                if self.workflow_id not in obj.workflow_history:
                    log_error(item, "Cannot find '{}' wkf id in workflow_history: {}".format(self.workflow_id,
                                                                                             obj.workflow_history))
                if len(obj.workflow_history[self.workflow_id]) > 1:
                    log_error(item, "workflow_history len > 1: {}".format(obj.workflow_history))
                wfh = []
                old_wfh = obj.workflow_history.get(self.workflow_id)
                old_wfh_len = len(old_wfh)
                change = False
                for i, status in enumerate(old_wfh, 1):
                    if self.replace:
                        # only modify last wf action
                        if i == old_wfh_len and status['review_state'] == self.replace:
                            if self.state_id:
                                status['review_state'] = self.state_id
                            if self.action_id:
                                status['action'] = self.action_id
                            if self.date_key and item.get(self.date_key):
                                status['time'] = DateTime(item[self.date_key])
                            if self.actor:
                                status['actor'] = self.actor
                            change = True
                    wfh.append(status)
                if not self.replace:
                    status = {'action': self.action_id, 'actor': self.actor, 'comments': '',
                              'review_state': self.state_id, 'time': DateTime(item[self.date_key])}
                    wfh.append(status)
                    change = True
                if change:
                    obj.workflow_history[self.workflow_id] = tuple(wfh)
            yield item


class StoreInData(object):
    """Store items in a dictionary.

    Parameters:
        * bp_key = M, blueprint key used as main storage key
        * store_key = M, storing keys for item. If defined, the item is stored in storage[{bp_key}][{store_key}]
        * store_subkey = O, storing sub keys for item. If defined, the item is stored in
          storage[{bp_key}][{store_key}][{store_subkey}]
        * fieldnames = O, fieldnames to store. All if nothing.
        * condition = O, condition expression
        * check_key_uniqueness = O, flag (0 or 1: default 1)
        * check_subkey_uniqueness = O, flag (0 or 1: default 1)
        * yield = O, flag to know if a yield must be done (0 or 1: default 0)
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
        # self.store_key = safe_unicode(options['store_key']).split()
        self.store_key = safe_unicode(options['store_key'])
        self.store_subkey = safe_unicode(options.get('store_subkey'))
        self.bp_key = safe_unicode(options['bp_key'])
        self.fieldnames = safe_unicode(options.get('fieldnames') or '').split()
        self.cku = bool(int(options.get('check_key_uniqueness') or '1'))
        self.csku = bool(int(options.get('check_subkey_uniqueness') or '1'))
        self.yld = bool(int(options.get('yield') or '0'))
        if self.bp_key not in self.storage['data']:
            self.storage['data'][self.bp_key] = {}

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.parts) and self.condition(item, storage=self.storage):
                course_store(self, item)
                # if not self.fieldnames and item['_bpk'] == self.bp_key:
                #     del item['_bpk']
                # key = get_values_string(item, self.store_key)
                key = item[self.store_key]
                if self.store_subkey:
                    subkey = item.get(self.store_subkey)
                    if self.csku and subkey in self.storage['data'][self.bp_key].get(key, {}):
                        log_error(item, u"Subkey '{}' of key '{}' already in '{}' data dict".format(subkey, key,
                                                                                                    self.bp_key))
                    self.storage['data'][self.bp_key].setdefault(key, {}).setdefault(subkey, {}).update(
                        filter_keys(item, self.fieldnames))
                else:
                    if self.cku and key in self.storage['data'][self.bp_key]:
                        log_error(item, u"Key '{}' already in '{}' data dict".format(key, self.bp_key))
                    self.storage['data'][self.bp_key].setdefault(key, {}).update(filter_keys(item, self.fieldnames))
                if not self.yld:
                    continue
            yield item
