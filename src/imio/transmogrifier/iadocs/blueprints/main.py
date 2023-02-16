# -*- coding: utf-8 -*-
from collections import OrderedDict
from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from collective.transmogrifier.utils import Condition
from imio.helpers.transmogrifier import get_main_path
from imio.helpers.transmogrifier import relative_path
from imio.helpers.transmogrifier import text_to_bool
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from imio.transmogrifier.iadocs.utils import get_mailtypes
from imio.transmogrifier.iadocs.utils import get_part
from imio.transmogrifier.iadocs.utils import get_plonegroup_orgs
from imio.transmogrifier.iadocs.utils import is_in_part
from imio.transmogrifier.iadocs.utils import log_error
from plone import api
from plone.dexterity.fti import DexterityFTIModificationDescription
from plone.dexterity.fti import ftiModified
from Products.CMFPlone.utils import safe_unicode
from zope.annotation import IAnnotations
from zope.interface import classProvides
from zope.interface import implements
from zope.lifecycleevent import ObjectModifiedEvent

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
        if run_options.get('commit'):
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

    def __iter__(self):
        for item in self.previous:
            yield item


class CommonInputChecks(object):
    """Checks input values of the corresponding external type.

    Parameters:
        * ext_type = M, external type string corresponding to csv
        * condition = O, condition expression
        * booleans = O, list of fields to transform in booleans
        * hyphen_newline = O, list of fields where newline will be replaced by hyphen
        * raise_on_error = O, raises exception if 1. Default 1. Can be set to 0.
    """
    classProvides(ISectionBlueprint)
    implements(ISection)

    def __init__(self, transmogrifier, name, options, previous):
        self.previous = previous
        self.storage = IAnnotations(transmogrifier).get(ANNOTATION_KEY)
        self.ext_type = safe_unicode(options['ext_type'])
        self.part = get_part(name)
        if not is_in_part(self, self.part):
            return
        fieldnames = self.storage['csv'].get(self.ext_type, {}).get('fd', [])
        self.condition = Condition(options.get('condition') or 'python:True', transmogrifier, name, options)
        self.hyphens = [key for key in safe_unicode(options.get('hyphen_newline', '')).split() if key in fieldnames]
        self.booleans = [key for key in safe_unicode(options.get('booleans', '')).split() if key in fieldnames]

    def __iter__(self):
        for item in self.previous:
            if is_in_part(self, self.part) and self.condition(item):
                # replace newline by hyphen on specified fields
                for fld in self.hyphens:
                    if '\n' in item[fld]:
                        item[fld] = ' - '.join([part.strip() for part in item[fld].split('\n') if part.strip()])
                # to bool from int
                for fld in self.booleans:
                    item[fld] = text_to_bool(item, fld, log_error)
            yield item
