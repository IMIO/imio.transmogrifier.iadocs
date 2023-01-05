# -*- coding: utf-8 -*-
from collections import OrderedDict

from collective.transmogrifier.interfaces import ISection
from collective.transmogrifier.interfaces import ISectionBlueprint
from imio.helpers.transmogrifier import get_main_path
from imio.helpers.transmogrifier import relative_path
from imio.transmogrifier.iadocs import ANNOTATION_KEY
from imio.transmogrifier.iadocs import e_logger
from imio.transmogrifier.iadocs import o_logger
from plone import api
from Products.CMFPlone.utils import safe_unicode
from zope.annotation import IAnnotations
from zope.interface import classProvides
from zope.interface import implements

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
        self.workingpath = get_main_path(safe_unicode(options.get('basepath', '')),
                                         safe_unicode(options.get('subpath', '')))
        self.portal = transmogrifier.context
        efh = logging.FileHandler(os.path.join(self.workingpath, 'dt_input_errors.log'), mode='w')
        efh.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        efh.setLevel(logging.INFO)
        e_logger.addHandler(efh)
        ofh = logging.FileHandler(os.path.join(self.workingpath, 'dt_shortlog.log'), mode='w')
        ofh.setFormatter(logging.Formatter('%(message)s'))
        ofh.setLevel(logging.INFO)
        o_logger.addHandler(ofh)
        run_options = json.loads(transmogrifier.context.REQUEST.get('_transmo_options_', '{}'))
        if run_options.get('commit'):
            ecfh = logging.FileHandler(os.path.join(self.workingpath, 'dt_input_errors_commit.log'), mode='a')
            ecfh.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
            ecfh.setLevel(logging.INFO)
            e_logger.addHandler(ecfh)
            ocfh = logging.FileHandler(os.path.join(self.workingpath, 'dt_shortlog_commit.log'), mode='a')
            ocfh.setFormatter(logging.Formatter('%(message)s'))
            ocfh.setLevel(logging.INFO)
            o_logger.addHandler(ocfh)

        # set working path in portal annotation to retrieve log files
        annot = IAnnotations(self.portal).setdefault(ANNOTATION_KEY, {})
        annot['wp'] = self.workingpath
        # set global variables in annotation
        self.storage = IAnnotations(transmogrifier).setdefault(ANNOTATION_KEY, {})
        self.storage['wp'] = self.workingpath
        # find directory
        brains = api.content.find(portal_type='directory')
        if brains:
            self.storage['directory'] = brains[0].getObject()
            self.storage['directory_path'] = relative_path(self.portal, brains[0].getPath())
        else:
            raise Exception("{}: Directory not found !".format(name))
        # store directory configuration
        dir_org_config = {}
        dir_org_config_len = {}
        for typ in ['types', 'levels']:
            dir_org_config[typ] = OrderedDict([(safe_unicode(t['name']), safe_unicode(t['token'])) for t in
                                               getattr(self.storage['directory'], 'organization_%s' % typ)])
            if not len(dir_org_config[typ]):
                dir_org_config[typ] = OrderedDict([(u'Non défini', u'non-defini')])
            dir_org_config_len[typ] = len(dir_org_config[typ])
        self.storage['dir_org_config'] = dir_org_config
        self.storage['dir_org_config_len'] = dir_org_config_len

    def __iter__(self):
        for item in self.previous:
            yield item
