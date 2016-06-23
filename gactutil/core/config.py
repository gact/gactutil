#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil module."""

import codecs as _codecs
import collections as _cxn
import inspect as _inspect
import os as _os
import pkg_resources as _pkg_resources
import platform as _platform

from gactutil.core import _standard_newlines
from gactutil.core.deep import DeepDict as _DeepDict
from gactutil.core.frozen import FrozenDeepDict as _FrozenDeepDict
import gactutil.core.uniyaml as _uniyaml

################################################################################

def _postload_standard_newline(x):
    
    if not isinstance(x, basestring):
        raise TypeError("invalid newline type: {!r}".format(type(x).__name__))
    
    # If not standard newline, check if escaped newline.
    if x not in _standard_newlines:
        unescaped = _codecs.decode(x, 'string_escape')
        if unescaped not in _standard_newlines:
            raise ValueError("invalid/unsupported newline: {!r}".format(x))
        x = unescaped
    
    try: # Convert newline to bytestring.
        x = x.encode('ascii')
    except UnicodeEncodeError:
        pass
    
    return x

def _predump_standard_newline(x):
    
    if not isinstance(x, basestring):
        raise TypeError("invalid newline type: {!r}".format(type(x).__name__))
    
    # If not standard newline, check if escaped newline.
    if x not in _standard_newlines:
        unescaped = _codecs.decode(x, 'string_escape')
        if unescaped not in _standard_newlines:
            raise ValueError("invalid/unsupported newline: {!r}".format(x))
        x = unescaped
    
    # Escape newline before dumping to config file.
    x = _codecs.encode(x, 'string_escape')
    
    return x

def _validate_command(x):
    u""""Validate command as string or sequence of strings."""
    if isinstance(x, basestring):
        return x
    if isinstance(x, _cxn.Sequence) and all( isinstance(w, basestring) for w in x ):
        return tuple(x)
    raise TypeError("command must be a string or sequence of strings: {!r}".format(x))

def _validate_unicode(x):
    if not isinstance(x, unicode):
        raise TypeError("expected object of type unicode, not {!r}".format(
            type(x).__name__))
    return x

################################################################################

_ConfigAtom = _cxn.namedtuple('ConfigAtom', ['default', 'postload', 'predump'])

class _Config(object):
    u"""Class for package configuration."""
    
    _spec = _FrozenDeepDict({
        
        u'default': {
            u'newline': _ConfigAtom(_os.linesep.encode('string_escape'),
                _postload_standard_newline, _predump_standard_newline)
        },
        
        u'tools': {
            u'bwa': _ConfigAtom(u'bwa', _validate_command, _validate_command),
            u'picard': _ConfigAtom((u'java', u'-jar', u'picard.jar'),
                _validate_command, _validate_command)
        }
    })
    
    @property
    def dirpath(self):
        u"""Config directory path."""
        return self._dirpath
    
    @property
    def filename(self):
        u"""Config filename."""
        return self._filename
    
    @property
    def filepath(self):
        u"""Config filepath."""
        return self._filepath
    
    @classmethod
    def _validate_config_info(cls, config_info):
        
        config_info = _DeepDict(config_info)
        config_spec = cls._spec
        
        for keys, value in config_info.leafitems():
            
            try:
                value_spec = config_spec[keys]
            except (KeyError, TypeError):
                raise KeyError("invalid config keys: {!r}".format(keys))
            
            try: # validate value, even if default
                config_info[keys] = value_spec.postload(value)
            except (AssertionError, TypeError, ValueError):
                raise ValueError("cannot set {!r} - invalid value: {!r}".format(
                    keys, value))
        
        return config_info
    
    def __init__(self):
        
        # Set config filename.
        self._filename = u'config.yaml'
        
        # Set platform-dependent config directory path.
        platform_system = _platform.system()
        if platform_system in ('Linux', 'Darwin'):
            home = _os.path.expanduser(u'~')
            if platform_system == 'Linux':
                self._dirpath = _os.path.join(home, u'.config', u'gactutil')
            elif platform_system == 'Darwin':
                self._dirpath = _os.path.join(home, u'Library',
                u'Application Support', u'GACTutil')
        elif platform_system == 'Windows':
            appdata = _os.getenv('APPDATA')
            self._dirpath = _os.path.join(appdata, u'GACTutil')
            if appdata is None or not _os.path.isdir(appdata):
                raise RuntimeError("valid %APPDATA% not found")
        else:
            raise RuntimeError("unrecognised platform: {!r}".format(platform_system))
        
        # Set config filepath.
        self._filepath = _os.path.join(self._dirpath, self._filename)
    
    def __delattr__(self, keys):
        raise TypeError("{} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __delitem__(self, keys):
        raise TypeError("{} object does not support item deletion".format(
            self.__class__.__name__))
    
    def __getitem__(self, keys):
        
        try:
            config_info = self.load()
            item = config_info[keys]
        except (KeyError, RuntimeError, TypeError):
            try:
                item = _Config._spec[keys].default
            except (KeyError, TypeError):
                raise KeyError("invalid config keys: {!r}".format(keys))
        
        if isinstance(item, _cxn.Mapping):
            item = _FrozenDeepDict(item)
        
        return item
        
    def __repr__(self):
        config_info = dict( self.load() )
        return '{}({})'.format(self.__class__.__name__, repr(config_info)[1:-1])
    
    def __setattr__(self, name, value):
        if hasattr(self, '_filepath'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[name] = value
        
    def __setitem__(self, keys, value):
        raise TypeError("{} object does not support item assignment".format(
            self.__class__.__name__))
    
    def load(self):
        u"""Load package config info."""
        
        config_info = _DeepDict()
        
        if _os.path.isfile(self._filepath):
            
            try:
                with open(self._filepath, 'r') as fh:
                    config_info = _uniyaml.uniload(fh)
            except (IOError, OSError, _uniyaml.YAMLError):
                raise RuntimeError("failed to read package config file: {!r}".format(
                    self._filepath))
            
            config_info = _Config._validate_config_info(config_info)
        
        return(config_info)
    
    def setup(self):
        u"""Setup package config file."""
        
        try: # Validate caller.
            parentframe = (_inspect.stack())[1][0]
            assert parentframe.f_globals['__file__'] == 'setup.py'
            assert parentframe.f_globals['__name__'] == '__main__'
        except (AssertionError, KeyError):
            raise RuntimeError("cGACTutil config info should only be setup during "
                "GACTutil package setup")
        
        config_info = self.load()
        config_spec = self._spec
        
        # Set any config info not already in config file.
        for keys, value_spec in config_spec.leafitems():
            try:
                value = config_info[keys]
            except KeyError:
                value = value_spec.default
            config_info[keys] = config_spec[keys].predump(value)
        config_info = dict( config_info )
        
        # Ensure config directory exists.
        if not _os.path.isdir(self._dirpath):
            _os.makedirs(self._dirpath)
        
        try: # Write package config file.
            s = _uniyaml.unidump(config_info)
            b = s.encode('utf_8')
            with open(self._filepath, 'w') as fh:
                fh.write(b)
        except (IOError, OSError, _uniyaml.YAMLError):
            raise RuntimeError("failed to setup package config file: {!r}".format(
                self._filepath))

################################################################################

config = _Config()

################################################################################
