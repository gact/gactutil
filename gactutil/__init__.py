#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""GACTutil module."""

from binascii import hexlify
from contextlib import contextmanager
from gzip import GzipFile
import inspect
import io
import os
from os.path import expanduser
from os.path import expandvars
from os.path import realpath
from os.path import relpath
from pkg_resources import resource_filename
from platform import system
from shutil import rmtree
import sys
from tempfile import mkdtemp

from yaml import dump
from yaml import load
from yaml import safe_dump
from yaml import safe_load
from yaml import YAMLError

################################################################################

_info = {
    
    # Data types that are considered valid as keys or valuse in a mapping object.
    'mapping-types': (None, bool, int, float, str),
    
    # Config settings filename.
    'settings-file': 'settings.yaml'
}

################################################################################

def _read_about():
    """Read information about this package."""
    about_file = os.path.join('data', 'about.yaml')
    about_path = resource_filename('gactutil', about_file)
    with open(about_path, 'r') as fh:
        about_info = load(fh)
    return about_info

def _read_command_info():
    """Read package command info."""
    cmd_file = os.path.join('data', 'gaction.yaml')
    cmd_path = resource_filename('gactutil', cmd_file)
    with open(cmd_path, 'r') as fh:
        cmd_info = load(fh)
    return cmd_info

def _read_setting(key):
    """Read a single package setting."""
    info = _read_settings()
    return info[key]

def _read_settings():
    """Read package settings file."""
    about = _read_about()
    settings_file = _info['settings-file']
    settings_path = os.path.join(about['config_dir'], settings_file)
    
    if os.path.isfile(settings_path):
        try:
            with open(settings_path) as fh:
                config_info = safe_load(fh)
        except (IOError, YAMLError):
            raise RuntimeError("failed to read package settings file ~ {!r}".format(settings_file))
    else:
        config_info = dict()
    return(config_info)

def _setup_about(setup_info):
    """Setup info about package.
    
    Outputs a package data file in YAML format with information about package.
    
    NB: this function should only be called during package setup.
    """
    
    # Validate caller.
    caller_file, caller_func = [ (inspect.stack()[1])[i] for i in (1, 3) ]
    if caller_file != 'setup.py' or caller_func != '<module>':
        raise RuntimeError("{!r} should only be called during GACTutil "
            "package setup".format(inspect.stack()[0][3]))
    
    # Set keys common to setup info and about info.
    common_keys = ('name', 'version')
    
    # Set common info from setup info.
    about_info = { k: setup_info[k] for k in common_keys }
    
    # Set config directory path for this platform.
    platform_system = system()
    if platform_system in ('Linux', 'Darwin'):
        home = expanduser('~')
        if platform_system == 'Linux':
            about_info['config_dir'] = os.path.join(home, '.config', 'gactutil')
        elif platform_system == 'Darwin':
            about_info['config_dir'] = os.path.join(home, 'Library', 
            'Application Support', 'GACTutil')
    elif platform_system == 'Windows':
        appdata = os.getenv('APPDATA')
        about_info['config_dir'] = os.path.join(appdata, 'GACTutil')
        if appdata is None or not os.path.isdir(appdata):
            raise RuntimeError("valid %APPDATA% not found")
    else:
        raise RuntimeError("unrecognised platform ~ {!r}".format(platform_system))
    
    # Ensure data directory exists.
    data_dir = os.path.join('gactutil', 'data')
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
    
    # Write info about package.
    about_file = os.path.join(data_dir, 'about.yaml')
    with open(about_file, 'w') as fh:
        dump(about_info, fh, default_flow_style=False)

def _truncate_string(s, length=16):
    """Truncate a string to the given length."""
    
    if not isinstance(s, basestring):
        raise TypeError("truncation object must be of type string")
    if not isinstance(length, int):
        raise TypeError("truncation length must be of type int")
    if length < 0:
        raise ValueError("invalid truncation length ~ {!r}".format(length))
    
    return s if len(s) <= length else '{}...'.format(s[:(length-3)])

def _validate_mapping(mapping):
    """Validate a mapping object."""
    for k in mapping:
        
        if not isinstance(k, _info['mapping-types']):
            raise TypeError("mapping key {!r} has invalid type ~ {!r}".format(str(k), type(k).__name__))
        
        try:
            x = mapping[k]
        except KeyError:
            raise TypeError("mapping has invalid type ~ {!r}".format(type(mapping).__name__))
        
        if not isinstance(x, _info['mapping-types']):
            raise TypeError("mapping value {!r} has invalid type ~ {!r}".format(str(x), type(x).__name__))

def _write_setting(key, value):
    """Write a single package setting."""
    info = _read_settings_file()
    info[key] = value    
    _write_settings_file(info, path)

def _write_settings(config_info):
    """Write package settings file."""
    about = _read_about()
    settings_file = _info['settings-file']
    settings_path = os.path.join(about['config_dir'], settings_file)
    try:
        with open(settings_path, 'w') as fh:
            safe_dump(config_info, fh, default_flow_style=False)
    except (IOError, YAMLError):
        raise RuntimeError("failed to write package settings file ~ {!r}".format(settings_file))

################################################################################

def prise(filepath, mode='r'):
    """Open a (possibly compressed) text file."""
    
    text_modes = ('r', 'w', 'a', 'rU')
    gzip_modes = ('r', 'w', 'a', 'rb', 'wb', 'ab')
    modes = text_modes + gzip_modes
    valid_modes = [ m for i, m in enumerate(modes) 
        if m not in modes[:i] ]
    gzip_magic = '1f8b' 
    
    if mode not in valid_modes:
        raise ValueError("invalid file mode ~ {!r}".format(mode))
    
    # Assume no GZIP compression/decompression.
    gzipping = False
    
    if mode.startswith('r'):
        
        with io.open(filepath, 'rb') as fh:
            sample = fh.peek()

        magic = hexlify(sample[:2])
        method = ord(sample[2:3])
        
        if magic == gzip_magic:
            if method == 8:
                gzipping = True
            else:
                raise ValueError("input compressed with unknown GZIP method")
            
    elif filepath.endswith('.gz'):
        gzipping = True
    
    if gzipping:

        if mode not in gzip_modes:
            raise ValueError("file mode {!r} should not be used for GZIP-compressed content".format(mode))
        fh = GzipFile(filepath, mode=mode)

    else:

        if mode not in text_modes:
            raise ValueError("file mode {!r} should not be used for plain text".format(mode))
        fh = open(filepath, mode=mode)
    
    return fh

def resolve_path(path, start=None):
    """Resolve the specified path.
    
    By default, the specified path is modified by expanding the home directory 
    and any environment variables, resolving symbolic links, and returning the 
    resulting absolute path. If a `start` path is specified, the resolved path 
    is given relative to `start`.
    
    Args:
        path (str): A system path.
        start (str): Optional starting point for the resolved path.
    
    Returns:
        str: Resolved system path.
    """
    resolved_path = realpath( expandvars( expanduser(path) ) )
    if start is not None:
        start = realpath( expandvars( expanduser(start) ) )
        resolved_path = relpath(resolved_path, start)
    return resolved_path

def resolve_paths(paths, start=None):
    """Resolve the specified paths.
    
    By default, the specified paths are modified by expanding the home directory 
    and any environment variables, resolving symbolic links, and returning the 
    resulting absolute path for each input path. If a `start` path is specified, 
    the resolved paths are given relative to `start`.
    
    Args:
        paths (list): System paths.
        start (str): Optional starting point for the resolved paths.
    
    Returns:
        dict: Mapping of input paths to their resolved form.
    """
    resolved_paths = dict()
    for path in paths:
        resolved_paths[path] = resolve_path(path, start=start)
    return resolved_paths

@contextmanager
def TemporaryDirectory(suffix='', prefix='tmp', name=None, dir=None, 
    delete=True):
    """Create temporary directory."""
    
    # If a temp directory name was specified, ensure it exists..
    if name is not None:
        
        # Verify temp directory name is a valid pathname component.
        if os.path.split(name)[0] != '':
            raise ValueError("temp directory name must be a valid pathname component")
        
        # Set temp directory name.
        twd = name
        
        # Prepend directory if specified.
        if dir is not None:
            os.path.join(dir, twd)
        
        # Resolve path of temp directory.
        twd = resolve_path(twd)
        
        # Ensure temp directory exists.
        if not os.path.exists(twd):
            os.makedirs(twd)
    
    # ..otherwise, create temp directory in usual way.
    else:
        twd = mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    
    try:
        yield twd
    finally:
        if delete:
            try:
                rmtree(twd)
            except OSError:
                warn("failed to remove temp directory ~ {!r}".format(twd), 
                    RuntimeWarning)

################################################################################
