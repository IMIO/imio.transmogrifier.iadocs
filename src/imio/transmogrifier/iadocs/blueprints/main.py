# -*- coding: utf-8 -*-
from collections import OrderedDict
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from DateTime.DateTime import DateTime
from imio.helpers.transmogrifier import correct_path
from imio.helpers.transmogrifier import filter_keys
from imio.helpers.transmogrifier import get_main_path
from imio.helpers.transmogrifier import pool_tuples
from imio.helpers.transmogrifier import relative_path
from imio.helpers.transmogrifier import str_to_bool
from imio.helpers.transmogrifier import str_to_date
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import full_path
from imio.transmogrifier.iadocs.utils import get_categories
from imio.transmogrifier.iadocs.utils import get_folders
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_part
from imio.transmogrifier.iadocs.utils import get_personnel
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import get_users
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from plone import api
from plone.dexterity.fti import DexterityFTIModificationDescription
from plone.dexterity.fti import ftiModified
from plone.i18n.normalizer import IIDNormalizer
from Products.CMFPlone.utils import safe_unicode
from zope.annotation import IAnnotations
from zope.component import getUtility
from zope.interface import classProvides
from zope.interface import implements
from zope.lifecycleevent import ObjectModifiedEvent

import csv
import json
import logging
import os
import pickle


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
        self.part = get_part(name)
        self.related_storage = safe_unicode(options['related_storage'])
        if not is_in_part(self, self.part):
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
            if is_in_part(self, self.part) and self.related_storage is not None and self.condition(item):
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
        self.portal = transmogrifier.context
        workingpath = get_main_path(safe_unicode(options.get('basepath') or ''),
                                    safe_unicode(options.get('subpath') or ''))
        csvpath = safe_unicode(options.get('csvpath') or '')
        if not csvpath:
            csvpath = workingpath
        in_types = safe_unicode(transmogrifier['config'].get('internal_number_types') or '').split()

        # setting logs
        efh = logging.FileHandler(os.path.join(workingpath, 'dt_input_errors.log'), mode='w')
        efh.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        efh.setLevel(logging.INFO)
        e_logger.addHandler(efh)
        ofh = logging.FileHandler(os.path.join(workingpath, 'dt_shortlog.log'), mode='w')
        ofh.setFormatter(logging.Formatter('%(message)s'))
        ofh.setLevel(logging.INFO)
        o_logger.addHandler(ofh)
        run_options = json.loads(transmogrifier.context.REQUEST.get('_transmo_options_') or '{}')
        if run_options['commit']:
            ecfh = logging.FileHandler(os.path.join(workingpath, 'dt_input_errors_commit.log'), mode='a')
            ecfh.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
            ecfh.setLevel(logging.INFO)
            e_logger.addHandler(ecfh)
            ocfh = logging.FileHandler(os.path.join(workingpath, 'dt_shortlog_commit.log'), mode='a')
            ocfh.setFormatter(logging.Formatter('%(message)s'))
            ocfh.setLevel(logging.INFO)
            o_logger.addHandler(ocfh)

        # check package installation and configuration
        if in_types:
            if not self.portal.portal_quickinstaller.isProductInstalled('collective.behavior.internalnumber'):
                self.portal.portal_setup.runAllImportStepsFromProfile(
                    'profile-collective.behavior.internalnumber:default', dependency_strategy='new')
                o_logger.info('Installed collective.behavior.internalnumber')
            reg_key = 'collective.behavior.internalnumber.browser.settings.IInternalNumberConfig.portal_type_config'
            inptc = list(api.portal.get_registry_record(reg_key))
            for typ in in_types:
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

        # set global variables in annotation
        self.storage = IAnnotations(transmogrifier).setdefault(ANNOTATION_KEY, {})
        self.storage['wp'] = workingpath
        self.storage['csvp'] = csvpath
        self.storage['csv'] = {}
        self.storage['data'] = {}
        self.storage['parts'] = run_options['parts']
        self.storage['commit'] = run_options['commit']
        self.storage['commit_nb'] = run_options['commit_nb']
        self.storage['batch_nb'] = run_options['batch_nb']
        self.storage['plone'] = {}
        # store parts on transmogrifier, so it can be used with standard condition
        transmogrifier.parts = self.storage['parts']

        # find directory
        brains = api.content.find(portal_type='directory')
        if brains:
            self.storage['plone']['directory'] = brains[0].getObject()
            self.storage['plone']['directory_path'] = relative_path(self.portal, brains[0].getPath())
        else:
            raise Exception("{}: Directory not found !".format(name))
        # store directory configuration
        dir_org_config = {}
        dir_org_config_len = {}
        for typ in ['types', 'levels']:
            dir_org_config[typ] = OrderedDict([(safe_unicode(t['name']), safe_unicode(t['token'])) for t in
                                               getattr(self.storage['plone']['directory'], 'organization_%s' % typ)])
            if not len(dir_org_config[typ]):
                dir_org_config[typ] = OrderedDict([(u'Non défini', u'non-defini')])
            dir_org_config_len[typ] = len(dir_org_config[typ])
        # self.storage['data']['dir_org_config'] = dir_org_config
        # self.storage['data']['dir_org_config_len'] = dir_org_config_len

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
        # store classification folders
        (self.storage['data']['p_folder_uid'], self.storage['data']['p_irn_to_folder'],
         self.storage['data']['p_folder_full_title']) = get_folders(self)
        # store already imported mails
        self.storage['data']['p_mail_ids'] = {}

        # deactivate versioning
        pr_tool = api.portal.get_tool('portal_repository')
        self.storage['plone']['pr_vct'] = tuple(pr_tool._versionable_content_types)
        pr_tool._versionable_content_types[:] = ()


    def __iter__(self):
        for item in self.previous:
            yield item


class CommonInputChecks(object):
    """Checks input values of the corresponding external type.

    Parameters:
        * bp_key = M, blueprint key corresponding to csv
        * condition = O, condition expression
        * booleans = O, list of fields to transform in booleans
        * invalids = O, list of pairs (fieldname values) for which field content will be replaced with None
          if it is equal to a value. values are | separated
        * hyphen_newline = O, list of fields where newline will be replaced by hyphen
        * dates = O, list of triplets (fieldname format as_date) to transform in date
        * strip_chars = O, list of pairs (fieldname chars) on which a strip must be done
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.bp_key = safe_unicode(options['bp_key'])
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            return
        fieldnames = self.storage['csv'].get(self.bp_key, {}).get('fd', [])
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.invalids = next(csv.reader([options.get('invalids', '').strip()], delimiter=' ', quotechar='"',
                                        skipinitialspace=True))
        self.invalids = [cell.decode('utf8') for cell in self.invalids]
        self.invalids = [tup for tup in pool_tuples(self.invalids, 2, 'invalids option') if tup[0] in fieldnames]
        self.hyphens = [key for key in safe_unicode(options.get('hyphen_newline', '')).split() if key in fieldnames]
        self.booleans = [key for key in safe_unicode(options.get('booleans', '')).split() if key in fieldnames]
        self.dates = safe_unicode(options.get('dates', '')).strip().split()
        self.dates = [tup for tup in pool_tuples(self.dates, 3, 'dates option') if tup[0] in fieldnames]
        self.strips = safe_unicode(options.get('strip_chars', '')).strip().split()
        self.strips = [tup for tup in pool_tuples(self.strips, 2, 'strip_chars option') if tup[0] in fieldnames]

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                # strip chars
                for fld, chars in self.strips:
                    if not item[fld]:
                        continue
                    item[fld] = item[fld].strip(chars)
                # replace newline by hyphen on specified fields
                for fld in self.hyphens:
                    if '\n' in (item[fld] or ''):
                        item[fld] = ' - '.join([part.strip() for part in item[fld].split('\n') if part.strip()])
                # replace invalid values on specified fields
                for fld, values in self.invalids:
                    for value in values.split(u'|'):
                        if item[fld] == value:
                            item[fld] = None
                            break
                # to bool
                for fld in self.booleans:
                    item[fld] = str_to_bool(item, fld, log_error)
                # to dates
                for fld, fmt, as_date in self.dates:
                    item[fld] = str_to_date(item, fld, log_error, fmt=fmt, as_date=bool(int(as_date)))
            yield item


class InsertPath(object):
    """Adds _path if not yet defined, to create new items.

    Parameters:
        * bp_key = M, blueprint key representing csv
        * id_keys = M, fieldnames to use to create id.
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
        self.part = get_part(name)
        self.bp_key = safe_unicode(options['bp_key'])
        if not is_in_part(self, self.part):
            return
        self.eids = self.storage['data'].setdefault(self.bp_key, {})
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        fieldnames = self.storage['csv'].get(self.bp_key, {}).get('fd', [])
        self.id_keys = [key for key in safe_unicode(options.get('id_keys', '')).split() if key in fieldnames]
        self.roe = bool(int(options.get('raise_on_error', '1')))

    def __iter__(self):
        idnormalizer = getUtility(IIDNormalizer)
        for item in self.previous:
            if '_path' in item:  # _path is already set
                yield item
                continue
            if is_in_part(self, self.part) and self.condition(item):
                title = u'-'.join([item[key] for key in self.id_keys if item[key]])
                if not title:
                    log_error(item, u'cannot get an id from id keys {}'.format(self.id_keys), level='critical')
                    if self.roe:
                        raise Exception(u'No title ! See log...')
                    continue
                new_id = idnormalizer.normalize(title)
                item['_path'] = '/'.join([item['_parenth'], new_id])
                item['_path'] = correct_path(self.portal, item['_path'])
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
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.portal = transmogrifier.context

    def __iter__(self):
        for item in self.previous:
            yield item
        # end of process
        # deactivate versioning
        pr_tool = api.portal.get_tool('portal_repository')
        pr_tool._versionable_content_types[:] = ()
        pr_tool._versionable_content_types.extend(self.storage['plone']['pr_vct'])
        # import ipdb; ipdb.set_trace()


class PickleData(object):
    """Pickle data.

    Parameters:
        * filename = M, filename to load and dump
        * store_key = M, store key in data dicts
        * d_condition = O, dump condition expression (default: False)
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.name = name
        self.transmogrifier = transmogrifier
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            self.filename = None
            return
        self.filename = safe_unicode(options['filename'])
        if not self.filename:
            return
        self.store_key = safe_unicode(options['store_key'])
        self.filename = full_path(self.storage['csvp'], self.filename)
        self.storage['data'].setdefault(self.store_key, {})
        if os.path.exists(self.filename):
            o_logger.info(u"Loading '{}'".format(self.filename))
            with open(self.filename, 'rb') as fh:
                self.storage['data'][self.store_key] = pickle.load(fh)
        self.d_condition = Condition(options.get('d_condition') or 'python:False', transmogrifier, name, options)

    def __iter__(self):
        for item in self.previous:
            yield item
        if self.filename and self.d_condition(None, storage=self.storage):
            o_logger.info(u"Dumping '{}'".format(self.filename))
            with open(self.filename, 'wb') as fh:
                pickle.dump(self.storage['data'][self.store_key], fh)


class SetState(object):
    """Sets state in workflow history.

    Parameters:
        * workflow_id = M, workflow id
        * state_id = M, state id
        * date_key = O, date key. Or don't change
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
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            return
        self.workflow_id = options['workflow_id']
        self.state_id = options['state_id']
        self.date_key = options.get('date_key')
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.roe = bool(int(options.get('raise_on_error', '1')))

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                try:
                    obj = self.portal.unrestrictedTraverse(safe_unicode(item['_path'][1:]).encode('utf8'))
                except AttributeError:
                    log_error(item, "The corresponding object '{}' cannot be found".format(item['_path']))
                    continue
                if self.workflow_id not in obj.workflow_history:
                    log_error(item, "Cannot find '{}' wkf id in workflow_history: {}".format(self.workflow_id,
                                                                                             obj.workflow_history))
                if len(obj.workflow_history[self.workflow_id]) > 1:
                    log_error(item, "workflow_history len > 1: {}".format(obj.workflow_history))
                wfh = []
                change = False
                for status in obj.workflow_history.get(self.workflow_id):
                    # replace old state by new one
                    if status['review_state'] == 'created':
                        status['review_state'] = self.state_id
                    if self.date_key and item.get(self.date_key):
                        status['time'] = DateTime(item[self.date_key])
                    # actor ?
                    wfh.append(status)
                    change = True
                if change:
                    obj.workflow_history[self.workflow_id] = tuple(wfh)
            yield item


class StoreInData(object):
    """Store items in a dictionary.

    Parameters:
        * bp_key = M, blueprint key representing csv
        * store_key = M, storing keys for item. If defined, the item is stored in storage[{bp_key}][{store_key}]
        * store_subkey = O, storing sub keys for item. If defined, the item is stored in
          storage[{bp_key}][{store_key}][{store_subkey}]
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
        self.bp_key = safe_unicode(options['bp_key'])
        self.fieldnames = safe_unicode(options.get('fieldnames') or '').split()
        self.yld = bool(int(options.get('yield') or '0'))
        if self.bp_key not in self.storage['data']:
            self.storage['data'][self.bp_key] = {}

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                # if not self.fieldnames and item['_bpk'] == self.bp_key:
                #     del item['_bpk']
                # key = get_values_string(item, self.store_key)
                key = item[self.store_key]
                if self.store_subkey:
                    subkey = item.get(self.store_subkey)
                    self.storage['data'][self.bp_key].setdefault(key, {})[subkey] = filter_keys(item, self.fieldnames)
                else:
                    self.storage['data'][self.bp_key][key] = filter_keys(item, self.fieldnames)
                if not self.yld:
                    continue
            yield item
