#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil about module."""

import inspect as _inspect
import os as _os
import pkg_resources as _pkg_resources
import pickle as _pickle

################################################################################

class _About(object):
    u"""Class for info about package."""
    
    def __init__(self):
        self._data = dict()
    
    def __delattr__(self, keys):
        raise TypeError("{} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __delitem__(self, keys):
        raise TypeError("{} object does not support item deletion".format(
            self.__class__.__name__))
    
    def __getitem__(self, key):
        if len(self._data) == 0: self.load()
        return self._data[key]
        
    def __repr__(self):
        if len(self._data) == 0: self.load()
        return '{}({})'.format(self.__class__.__name__, repr(self._data)[1:-1])
    
    def __setattr__(self, key, value):
        if hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[key] = value
        
    def __setitem__(self, keys, value):
        raise TypeError("{} object does not support item assignment".format(
            self.__class__.__name__))
    
    def load(self):
        u"""Load info about package."""
        
        about_file = _os.path.join(u'data', u'about.p')
        about_path = _pkg_resources.resource_filename(u'gactutil', about_file)
        
        try:
            with open(about_path, 'r') as fh:
                about_info = _pickle.load(fh)
        except (IOError, OSError, _pickle.PickleError):
            raise RuntimeError("failed to read package 'about' file: {!r}".format(
                about_path))
        
        self._data.clear()
        self._data.update(about_info)
        
        return self._data
    
    def setup(self, setup_info):
        u"""Setup info about package."""
        
        try: # Validate caller.
            parentframe = (_inspect.stack())[1][0]
            assert parentframe.f_globals['__file__'] == 'setup.py'
            assert parentframe.f_globals['__name__'] == '__main__'
        except (AssertionError, KeyError):
            raise RuntimeError("GACTutil about info should only be setup during "
                "GACTutil package setup")
        
        # Set keys common to setup info and about info.
        common_keys = (u'author_email', u'name', u'version')
        
        # Set common info from setup info.
        about_info = dict()
        for k in common_keys:
            about_info[k] = setup_info[k]
        
        # Ensure data directory exists.
        data_dir = _os.path.join(u'gactutil', u'data')
        if not _os.path.isdir(data_dir):
            _os.makedirs(data_dir)
        
        # Write info about package.
        about_path = _os.path.join(data_dir, u'about.p')
        try:
            with open(about_path, 'w') as fh:
                _pickle.dump(about_info, fh)
        except (IOError, OSError, _pickle.PickleError):
            raise RuntimeError("failed to setup package 'about' file: {!r}".format(
                about_path))

################################################################################

about = _About()

################################################################################
