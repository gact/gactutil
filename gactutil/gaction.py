#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil command-line interface."""

from __future__ import absolute_import
from argparse import ArgumentParser
from collections import deque
from collections import Mapping
from collections import Iterable
from collections import MutableMapping
from collections import namedtuple
from collections import OrderedDict
from copy import deepcopy
from datetime import date
from datetime import datetime
from functools import partial
from imp import load_source
from importlib import import_module
from inspect import getargspec
from inspect import getmembers
from inspect import getsource
from inspect import isfunction
from inspect import stack
from io import BytesIO
import os
from pandas import DataFrame
from pandas import read_csv
import pickle
from pkg_resources import resource_filename
import re
import sys
from textwrap import dedent
from types import IntType
from types import NoneType

from gactutil import _read_about
from gactutil import _tokenise_source
from gactutil import fsdecode
from gactutil import fsencode
from gactutil import TextReader
from gactutil import TextWriter
from gactutil.core.yaml import unidump
from gactutil.core.yaml import uniload
from gactutil.core.yaml import unidump_scalar
from gactutil.core.yaml import uniload_scalar
from gactutil.core.yaml import YAMLError

################################################################################

class _GactfuncSpec(object):
    u"""Class for specification of gactfunc info."""
    
    @property
    def module(self):
        return self._data[u'module']
    
    @property
    def function(self):
        return self._data[u'function']
    
    @property
    def ap_spec(self):
        return self._data[u'ap_spec']
    
    def __init__(self, module, function, ap_spec):
        self._data = OrderedDict([
            (u'module', module),
            (u'function', function),
            (u'ap_spec', ap_spec)
        ])
    
    def __setattr__(self, key, value):
        if hasattr(self, '_data'):
            raise TypeError("{!r} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[key] = value

# Named tuple for specification of gactfunc parameter/return types.
_GFTS = namedtuple('GFTS', [
    'is_compound',    # Composite data type.
    'is_ductile',     # Convertible to a single-line string, and vice versa.
    'is_fileable'     # Can be loaded from and dumped to file.
])

_info = {

    # True values from PyYAML-3.11 <http://pyyaml.org/browser/pyyaml> [Accessed: 5 Apr 2016].
    u'true_values': (u'yes', u'Yes', u'YES', u'true', u'True', u'TRUE', u'on', u'On', u'ON'),
    
    # False values from PyYAML-3.11 <http://pyyaml.org/browser/pyyaml> [Accessed: 5 Apr 2016].
    u'false_values': (u'no', u'No', u'NO', u'false', u'False', u'FALSE', u'off', u'Off', u'OFF'),
    
    # Null values from PyYAML-3.11 <http://pyyaml.org/browser/pyyaml> [Accessed: 5 Apr 2016].
    u'na_values': (u'null', u'Null', u'NULL'),
    
    u'reserved_params': frozenset([
        u'help',              # argparse help
        u'version',           # argparse version
        u'gactfunc_function', # gactfunc function name
        u'gactfunc_module',   # gactfunc module name
        u'retfile'            # return-value option name
    ]),
    
    # Input/output patterns.
    u'iop': {
        
        # Input parameter patterns.
        u'input': {
            
            u'single': {
                u'regex': re.compile(u'^infile$'),
                u'metavar': u'FILE',
                u'flag': u'-i'
            },
            
            u'listed': {
                u'regex': re.compile(u'^infiles$'),
                u'metavar': u'FILES',
                u'flag': u'-i'
            },
            
            u'indexed': {
                u'regex': re.compile(u'^infile(?P<index>[1-9]+|U)$'),
                u'metavar': u'FILE\g<index>',
                u'flag': u'-\g<index>'
            },
            
            u'directory': {
                u'regex': re.compile(u'^indir$'),
                u'metavar': u'DIR',
                u'flag': u'-i'
            },
            
            u'prefix': {
                u'regex': re.compile(u'^inprefix$'),
                u'metavar': u'PREFIX',
                u'flag': u'-i'
            }
        },
        
        # Output parameter patterns.
        u'output': {
            u'single': {
                u'regex': re.compile(u'^outfile$'),
                u'metavar': u'FILE',
                u'flag': u'-o'
            },
            
            u'listed': {
                u'regex': re.compile(u'^outfiles$'),
                u'metavar': u'FILES',
                u'flag': u'-o'
            },
            
            u'indexed': {
                u'regex': re.compile(u'^outfile(?P<index>[1-9]+|U)$'),
                u'metavar': u'FILE\g<index>',
                u'flag': u'--\g<index>'
            },
            
            u'directory': {
                u'regex': re.compile(u'^outdir$'),
                u'metavar': u'DIR',
                u'flag': u'-o'
            },
            
            u'prefix': {
                u'regex': re.compile(u'^outprefix$'),
                u'metavar': u'PREFIX',
                u'flag': u'-o'
            },
            
            u'returned': {
                u'regex': None,
                u'metavar': u'FILE',
                u'flag': u'-o'
            }
        }
    },
    
    # Short-form parameters: mappings of Python function parameters to
    # short-form command-line flags. These make it possible for common
    # parameters to take a short form on the command line. If a gactfunc
    # uses a short-form parameter, this is automatically converted to
    # the corresponding flag by '_setup_commands'.
    u'short_params': {
        
        u'directory': {
            u'flag': u'-d',
            u'type': unicode
        },
        
        u'threads': {
            u'default': 1,
            u'flag': u'-t',
            u'required': False,
            u'type': int
        }
    },
    
    # Gactfunc docstring headers.
    u'docstring_headers': {
        
        u'known': (u'Args', u'Arguments', u'Attributes', u'Example', u'Examples',
                  u'Keyword Args', u'Keyword Arguments', u'Methods', u'Note',
                  u'Notes', u'Other Parameters', u'Parameters', u'Return',
                  u'Returns', u'Raises', u'References', u'See Also', u'Warning',
                  u'Warnings', u'Warns', u'Yield', u'Yields'),
        
        u'supported': (u'Args', u'Arguments', u'Note', u'Notes', u'Parameters',
                      u'Return', u'Returns', u'References', u'See Also'),
        
        u'alias_mapping': { u'Arguments': u'Args', u'Parameters': u'Args',
                           u'Return': u'Returns' }
    },
    
    u'regex': {
        u'gactfunc': re.compile(u'^(?:[A-Z0-9]+)(?:_(?:[A-Z0-9]+))*$', re.IGNORECASE),
        u'docstring_header': re.compile(u'^(\w+):\s*$'),
        u'docstring_param': re.compile(u'^([*]{0,2}\w+)\s*(?:\((\w+)\))?:\s+(.+)$'),
        u'docstring_return': re.compile(u'^(?:(\w+):\s+)?(.+)$'),
        u'docstring_default': re.compile(u'[[(]default:\s+(.+?)\s*[])]', re.IGNORECASE)
    }
}

################################################################################

def _DataFrame_from_file(f):
    u"""Get Pandas DataFrame from file."""
    
    try:
        with TextReader(f) as reader:
            x = read_csv(reader, sep=',', header=0, mangle_dupe_cols=False,
                skipinitialspace=True, true_values=_info[u'true_values'],
                false_values=_info[u'false_values'], keep_default_na=False,
                na_values=_info[u'na_values'])
    except (IOError, OSError):
        raise RuntimeError("failed to get DataFrame from file ~ {!r}".format(f))
    
    return x

def _DataFrame_to_file(x, f):
    u"""Output Pandas DataFrame to file."""
    try:
        with TextWriter(f) as writer:
            x.to_csv(writer, sep=',', na_rep=_info[u'na_values'][0], index=False)
    except (IOError, OSError):
        raise ValueError("failed to output DataFrame to file ~ {!r}".format(x))

def _dict_from_file(f):
    u"""Get dictionary from file."""
    
    try:
        with TextReader(f) as reader:
            x = uniload(reader)
        assert type(x) == dict
    except (AssertionError, IOError, YAMLError):
        raise ValueError("failed to read valid dict from file ~ {!r}".format(f))
    
    return x

def _dict_from_line(s):
    u"""Get dictionary from input string."""
    
    s = fsdecode(s)
    
    if not ( s.startswith(u'{') and s.endswith(u'}') ):
        s = u'{' + s + u'}'
    
    try:
        x = uniload(s)
        assert type(x) == dict
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert string to valid dict ~ {!r}".format(s))
    
    return x

def _dict_to_file(x, f):
    u"""Output dictionary to file."""
    
    if type(x) != dict:
        raise TypeError("argument must be of type dict, not {!r}".format(
            type(x).__name__))
    
    try:
        with TextWriter(f) as writer:
            unidump(x, writer, default_flow_style=False, width=sys.maxint)
    except (IOError, YAMLError):
        raise ValueError("failed to output dict to file ~ {!r}".format(x))

def _dict_to_line(x):
    u"""Convert dictionary to a single-line unicode string."""
    
    if type(x) != dict:
        raise TypeError("argument must be of type dict, not {!r}".format(
            type(x).__name__))
    
    try:
        s = unidump(x, default_flow_style=True, width=sys.maxint)
        s = s.rstrip(u'\n')
        assert u'\n' not in s
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert dict to unicode ~ {!r}".format(x))
    
    return s

def _list_from_file(f):
    u"""Get list from file."""
    
    with TextReader(f) as reader:
        
        document_ended = False
        last_nonempty_line = 0
        x = list()
        
        # Enumerate lines of file with 1-offset index.
        for i, line in enumerate(reader, start=1):
            
            # Strip YAML comments and flanking whitespace from this line.
            line = ystrip(line)
            
            # Skip lines after explicit document end.
            if document_ended:
                if line != u'':
                    raise RuntimeError("list elements found after document end")
                continue
            
            # If line is not empty, update 1-offset index of last nonempty line.
            if line != u'':
                last_nonempty_line = i
            
            # Check for explicit document separator.
            if line == u'---':
                raise RuntimeError("expected a single document in list stream")
            
            # Check for explicit document end.
            if line == u'...':
                document_ended = True
                continue
            
            # Load element from line.
            if line.startswith(u'{') and line.endswith(u'}'):
                element = _dict_from_line(line)
            elif line.startswith(u'[') and line.endswith(u']'):
                element = _list_from_line(line)
            else:
                element = _scalar_from_line(line)
            
            # Append element to list.
            x.append(element)
        
        # Pop trailing empty lines.
        while len(x) > last_nonempty_line:
            x.pop()
    
    return x

def _list_from_line(s):
    u"""Get list from string."""
    
    s = fsdecode(s)
    
    if not ( s.startswith(u'[') and s.endswith(u']') ):
        s = u'[' + s + u']'
    
    try:
        x = uniload(s)
        assert type(x) == list
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert string to valid list ~ {!r}".format(s))
    
    return x

def _list_to_file(x, f):
    u"""Output list to file."""
    
    if type(x) != list:
        raise TypeError("argument must be of type list, not {!r}".format(
            type(x).__name__))
    
    with TextWriter(f) as writer:
        
        for element in x:
            
            try:
                # Convert element to a single-line.
                if isinstance(element, dict):
                    line = _dict_to_line(element)
                elif isinstance(element, list):
                    line = _list_to_line(element)
                else:
                    line = _scalar_to_line(element)
                
                # Write line to output file.
                writer.write( u'{}{}'.format(line.rstrip(u'\n'), u'\n') )
                
            except (IOError, ValueError):
                raise ValueError("failed to output list to file ~ {!r}".format(x))

def _list_to_line(x):
    u"""Convert list to a single-line unicode string."""
    
    if type(x) != list:
        raise TypeError("argument must be of type list, not {!r}".format(
            type(x).__name__))
    
    try:
        s = unidump(x, default_flow_style=True, width=sys.maxint)
        s = s.rstrip(u'\n')
        assert u'\n' not in s
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert list to unicode ~ {!r}".format(x))
    
    return s

def _scalar_from_file(f, scalar_type=None):
    u"""Get scalar from file."""
    with TextReader(f) as fh:
        s = fh.read()
    return _scalar_from_line(s, scalar_type=scalar_type)

def _scalar_from_line(s, scalar_type=None):
    u"""Get scalar from string."""
    
    s = fsdecode(s)
    
    x = uniload_scalar(s)
    
    if scalar_type is not None and type(x) != scalar_type:
        raise ValueError("failed to convert string to valid {} ~ {!r}".format(
            scalar_type.__name__, s))
    
    return x

def _scalar_to_file(x, f):
    u"""Output scalar to file."""
    s = _scalar_to_line(x)
    with TextWriter(f) as fh:
        fh.write(u'{}\n'.format(s))

def _scalar_to_line(x):
    u"""Convert scalar to a single-line unicode string."""
    return unidump_scalar(x)

class _Chaperon(object):
    
    # Supported gactfunc parameter/return types. These must be suitable for use
    # both as Python function arguments and as command-line arguments, whether
    # loaded from a file or converted from a simple string. NB: types should be
    # checked in order (e.g. bool before int, datetime before date).
    supported_types = OrderedDict([
        #                 COMPOUND   DUCTILE  FILEABLE
        (NoneType,  _GFTS(   False,     True,     True)),
        (bool,      _GFTS(   False,     True,     True)),
        (unicode,   _GFTS(   False,     True,     True)),
        (float,     _GFTS(   False,     True,     True)),
        (int,       _GFTS(   False,     True,     True)),
        (datetime,  _GFTS(   False,     True,     True)),
        (date,      _GFTS(   False,     True,     True)),
        (dict,      _GFTS(    True,     True,     True)),
        (list,      _GFTS(    True,     True,     True)),
        (DataFrame, _GFTS(    True,    False,     True))
    ])
    
    # Mapping of each supported type name to its corresponding type object.
    _name2type = OrderedDict([
        (u'NoneType',  NoneType),
        (u'bool',      bool),
        (u'unicode',   unicode),
        (u'float',     float),
        (u'int',       int),
        (u'datetime',  datetime),
        (u'date',      date),
        (u'dict',      dict),
        (u'list',      list),
        (u'DataFrame', DataFrame)
    ])
    
    # Mapping of each supported type to its corresponding file-loading function.
    _from_file = OrderedDict([
        (NoneType,  partial(_scalar_from_file, scalar_type=NoneType)),
        (bool,      partial(_scalar_from_file, scalar_type=bool)),
        (unicode,   partial(_scalar_from_file, scalar_type=unicode)),
        (float,     partial(_scalar_from_file, scalar_type=float)),
        (int,       partial(_scalar_from_file, scalar_type=int)),
        (datetime,  partial(_scalar_from_file, scalar_type=datetime)),
        (date,      partial(_scalar_from_file, scalar_type=date)),
        (dict,      _dict_from_file),
        (list,      _list_from_file),
        (DataFrame, _DataFrame_from_file)
    ])
    
    # Mapping of each supported type to its corresponding line-loading function.
    _from_line = OrderedDict([
        (NoneType,  partial(_scalar_from_line, scalar_type=NoneType)),
        (bool,      partial(_scalar_from_line, scalar_type=bool)),
        (unicode,   partial(_scalar_from_line, scalar_type=unicode)),
        (float,     partial(_scalar_from_line, scalar_type=float)),
        (int,       partial(_scalar_from_line, scalar_type=int)),
        (datetime,  partial(_scalar_from_line, scalar_type=datetime)),
        (date,      partial(_scalar_from_line, scalar_type=date)),
        (dict,      _dict_from_line),
        (list,      _list_from_line)
    ])
    
    # Mapping of each supported type to its corresponding file-dumping function.
    _to_file = OrderedDict([
        (NoneType,  _scalar_to_file),
        (bool,      _scalar_to_file),
        (unicode,   _scalar_to_file),
        (float,     _scalar_to_file),
        (int,       _scalar_to_file),
        (datetime,  _scalar_to_file),
        (date,      _scalar_to_file),
        (dict,      _dict_to_file),
        (list,      _list_to_file),
        (DataFrame, _DataFrame_to_file)
    ])
    
    # Mapping of each supported type to its corresponding line-dumping function.
    _to_line = OrderedDict([
        (NoneType,  _scalar_to_line),
        (bool,      _scalar_to_line),
        (unicode,   _scalar_to_line),
        (float,     _scalar_to_line),
        (int,       _scalar_to_line),
        (datetime,  _scalar_to_line),
        (date,      _scalar_to_line),
        (dict,      _dict_to_line),
        (list,      _list_to_line)
    ])
    
    @staticmethod
    def _validate_ductile(x):
        u"""Validate ductile object type."""
        
        object_type = type(x)
        
        try:
            ductile = _Chaperon.supported_types[object_type].is_ductile
        except KeyError:
            raise TypeError("unknown gactfunc parameter/return type ~ {!r}".format(
                object_type.__name__))
        
        if object_type == unicode:
            
            if any( line_break in x for line_break in (u'\n', u'\r', u'\r\n') ):
                raise ValueError("unicode string is not ductile ~ {!r}".format(x))
            
        elif object_type == dict:
            
            for key, value in x.items():
                _validate_ductile(key)
                _validate_ductile(value)
            
        elif object_type == list:
            
            for element in x:
                _validate_ductile(element)
            
        elif not ductile:
            raise TypeError("{} is not ductile ~ {!r}".format(
                object_type.__name__, x))
    
    @classmethod
    def from_file(cls, filepath, object_type):
        u"""Get chaperon object from file."""
        try:
            x = _Chaperon._from_file[object_type](filepath)
        except KeyError:
            raise TypeError("unsupported type ~ {!r}".format(type(x).__name__))
        return _Chaperon(x)
        
    @classmethod
    def from_line(cls, string, object_type):
        u"""Get chaperon object from single-line string."""
        try:
            x = _Chaperon._from_line[object_type](string)
        except KeyError:
            raise TypeError("unsupported type ~ {!r}".format(type(x).__name__))
        return _Chaperon(x)
    
    @property
    def value(self):
        u"""Get chaperoned object."""
        return self._obj
    
    def __init__(self, x):
        if type(x) not in _Chaperon.supported_types:
            raise TypeError("unsupported type ~ {!r}".format(type(x).__name__))
        self._obj = x
        
    def __repr__(self):
        return '_Chaperon({})'.format( repr(self._obj) )
        
    def __str__(self):
        return fsencode( self._to_line[type(self._obj)](self._obj) )
    
    def __unicode__(self):
        return self._to_line[type(self._obj)](self._obj)
        
    def to_file(self, filepath):
        u"""Output chaperoned object to file."""
        self._to_file[type(self._obj)](self._obj, filepath)

class gactfunc(object):
    u"""A gactfunc wrapper class."""
    
    @property
    def ap_spec(self):
        try:
            return deepcopy(self._data[u'ap_spec'])
        except KeyError:
            self._update_ap_spec()
            return deepcopy(self._data[u'ap_spec'])
    
    @property
    def commands(self):
        return self._data[u'commands']
    
    @property
    def description(self):
        return self._data[u'description']
    
    @property
    def function(self):
        return self._data[u'function']
    
    @property
    def iop(self):
        return deepcopy(self._data[u'iop'])
    
    @property
    def param_spec(self):
        return deepcopy(self._data[u'param_spec'])
    
    @property
    def params(self):
        return self._data[u'param_spec'].keys()
    
    @property
    def return_spec(self):
        return deepcopy(self._data[u'return_spec'])
    
    @property
    def summary(self):
        return self._data[u'summary']
    
    @staticmethod
    def _parse_function_docstring(function):
        u"""Parse gactfunc docstring.
        
        This function parses a gactfunc docstring and returns an ordered
        dictionary mapping headers to documentation. A gactfunc docstring
        must be in Google-style format. The keys of the returned dictionary
        will correspond to the docstring headers (e.g. 'Args'), in addition
        to two special headers: 'Summary', which will contain the docstring
        summary line; and 'Description', which will contain the docstring
        description, if present.
        """
        
        # Get function name.
        func_name = function.__name__
        
        # Check gactfunc is indeed a function.
        if not isfunction(function):
            return TypeError("object is not a function ~ {!r}".format(func_name))
        
        # Get function docstring.
        docstring = function.__doc__
        
        # Set default parsed docstring.
        doc_info = None
        
        # Parse docstring if present.
        if docstring is not None and docstring.strip() != u'':
            
            # Init raw docstring.
            raw_info = OrderedDict()
            
            # Split docstring into lines.
            lines = deque( docstring.split(u'\n'))
            
            # Set summary from first non-blank line.
            line = lines.popleft().strip()
            if line == u'':
                line = lines.popleft().strip()
                if line == u'':
                    raise ValueError("{} docstring summary is a blank line".format(func_name))
            raw_info[u'Summary'] = [line]
            
            # Check summary followed by a blank line.
            if len(lines) > 0:
                line = lines.popleft().strip()
                if line != u'':
                    raise ValueError("{} docstring summary is not followed by a blank line".format(func_name))
            
            # Get list of remaining lines, with common indentation removed.
            lines = deque( (dedent( u'\n'.join(lines) ) ).split(u'\n') )
            
            # Init docstring description.
            raw_info[u'Description'] = list()
            
            # Docstring description includes everything before the first header.
            h = u'Description'
            
            # Group content by docstring section.
            while len(lines) > 0:
                
                # Get first of remaining lines.
                line = lines.popleft()
                
                # Try to match line to a docstring header.
                m = _info[u'regex'][u'docstring_header'].match(line)
                
                # If matches, set header of new section..
                if m is not None:
                    
                    # Set current header.
                    h = m.group(1)
                    
                    # Map header to alias, if relevant.
                    if h in _info[u'docstring_headers'][u'alias_mapping']:
                        h = _info[u'docstring_headers'][u'alias_mapping'][h]
                    
                    # Check header is known.
                    if h not in _info[u'docstring_headers'][u'known']:
                        raise ValueError("unknown docstring header ~ {!r}".format(h))
                    
                    # Check header is supported.
                    if h not in _info[u'docstring_headers'][u'supported']:
                        raise ValueError("unsupported docstring header ~ {!r}".format(h))
                    
                    # Check for duplicate headers.
                    if h in raw_info:
                        raise ValueError("duplicate docstring header ~ {!r}".format(h))
                    
                    raw_info[h] = list()
                    
                # ..otherwise append line to current section.
                else:
                    raw_info[h].append(line)
            
            # Remove docstring description, if empty.
            if len(raw_info[u'Description']) == 0:
                del raw_info[u'Description']
            
            # Init parsed docstring.
            doc_info = OrderedDict()
            
            # Process each docstring section.
            for h in raw_info:
                
                # Get docstring section as unindented lines.
                raw_info[h] = ( dedent( '\n'.join(raw_info[h]) ) ).split('\n')
                
                if h == u'Args':
                    
                    # Init parsed parameter info.
                    param_info = OrderedDict()
                    
                    param_name = None
                    
                    # Group content by parameter.
                    for line in raw_info[h]:
                        
                        line = line.strip()
                        
                        # Skip blank lines.
                        if line != u'':
                            
                            # Try to match line to expected pattern of parameter.
                            m = _info[u'regex'][u'docstring_param'].match(line)
                            
                            # If this is a parameter definition line, get parameter info..
                            if m is not None:
                                
                                param_name, type_name, param_desc = m.groups()
                                
                                # Check parameter does not denote unenumerated arguments.
                                if param_name.startswith(u'*'):
                                    raise RuntimeError("{} docstring must not specify unenumerated arguments".format(
                                        func_name))
                                
                                # Check parameter type specified.
                                if type_name is None:
                                    raise ValueError("{} docstring must specify a type for parameter {!r}".format(
                                        func_name, param_name))
                                
                                try: # Get type of parameter.
                                    param_type = _Chaperon._name2type[type_name]
                                except KeyError:
                                    raise ValueError("{} docstring specifies unknown type {!r} for parameter {!r}".format(
                                        func_name, type_name, param_name))
                                
                                # Check parameter type is not NoneType.
                                if param_type is NoneType:
                                    raise ValueError("{} docstring specifies 'NoneType' for parameter {!r}".format(
                                        func_name, param_name))
                                
                                # Check parameter type is supported.
                                if param_type not in _Chaperon.supported_types:
                                    raise ValueError("{} docstring specifies unsupported type {!r} for parameter {!r}".format(
                                        func_name, type_name, param_name))
                                
                                # Check for duplicate parameters.
                                if param_name in param_info:
                                    raise ValueError("{} docstring contains duplicate parameter ~ {!r}".format(
                                        func_name, param_name))
                                
                                param_info[param_name] = {
                                    u'type': param_type,
                                    u'description': param_desc
                                }
                            
                            # ..otherwise if parameter defined, treat this as
                            # a continuation of the parameter description..
                            elif param_name is not None:
                                
                                param_info[param_name][u'description'] = u'{} {}'.format(
                                    param_info[param_name][u'description'], line)
                            
                            # ..otherwise this is not a valid docstring parameter.
                            else:
                                raise ValueError("failed to parse docstring for function ~ {!r}".format(
                                    func_name))
                    
                    # Validate docstring default info.
                    for param_name in param_info:
                        
                        # Try to match default definition pattern in parameter description.
                        defaults = _info[u'regex'][u'docstring_default'].findall(
                            param_info[param_name][u'description'])
                        
                        # If a default definition matched, keep
                        # string representation of default value..
                        if len(defaults) == 1:
                            param_info[param_name][u'docstring_default'] = defaults[0]
                        # ..otherwise the description has ambiguous default info.
                        elif len(defaults) > 1:
                            raise ValueError("{} docstring has multiple defaults for parameter {!r}".format(
                                func_name, param_name))
                    
                    # Set parsed parameter info for docstring.
                    doc_info[h] = param_info
                    
                elif h == u'Returns':
                    
                    description = list()
                    param_type = None
                    
                    # Process each line of return value section.
                    for line in raw_info[h]:
                        
                        line = line.strip()
                        
                        # Skip blank lines.
                        if line != u'':
                            
                            # Try to match line to expected pattern of return value.
                            m = _info[u'regex'][u'docstring_return'].match(line)
                            
                            # If return value type info is present,
                            # get type info and initial description..
                            if m is not None:
                                
                                type_name = m.group(1)
                                description.append( m.group(2) )
                                
                                # Check parameter type specified.
                                if type_name is None:
                                    raise ValueError("{} docstring must specify a type for return value".format(
                                        func_name))
                                
                                try: # Get type of parameter.
                                    param_type = _Chaperon._name2type[type_name]
                                except KeyError:
                                    raise ValueError("{} docstring specifies unknown type {!r} for return value".format(
                                        func_name, type_name))
                                
                                # Check return value type is not NoneType.
                                if param_type is NoneType:
                                    raise ValueError("{} docstring specifies 'NoneType' for return value".format(
                                        func_name))
                                
                                # Check return value type is supported.
                                if param_type not in _Chaperon.supported_types:
                                    raise ValueError("{} docstring specifies unsupported type {!r} for return value".format(
                                        func_name, type_name))
                            
                            # ..otherwise if return value type already
                            # identified, append line to description..
                            elif param_type is not None:
                                
                                description.append(line)
                                
                            # ..otherwise this is not a valid docstring return value.
                            else:
                                raise ValueError("failed to parse docstring for function ~ {!r}".format(
                                    func_name))
                    
                    # Set parsed return value info for docstring.
                    doc_info[h] = {
                        u'type': param_type,
                        u'description': u' '.join(description)
                    }
                    
                else:
                    
                    # Strip leading/trailing blank lines.
                    lines = raw_info[h]
                    for i in (0, -1):
                        while len(lines) > 0 and lines[i].strip() == u'':
                            lines.pop(i)
                    doc_info[h] = u'\n'.join(lines)
        
        return doc_info
    
    @staticmethod
    def _parse_function_name(function):
        u"""Parse gactfunc name."""
        
        # Get function name.
        func_name = function.__name__
        
        # Check gactfunc is indeed a function.
        if not isfunction(function):
            return TypeError("object is not a function ~ {!r}".format(func_name))
        
        # Try to match function name to expected gactfunc pattern.
        m = _info[u'regex'][u'gactfunc'].match(func_name)
        
        try: # Split gactfunc name into commands.
            assert m is not None
            commands = tuple( func_name.split(u'_') )
            assert len(commands) >= 2
            assert len(set(commands)) == len(commands)
        except AssertionError:
            raise ValueError("function {!r} does not follow gactfunc naming convention".format(func_name))
        
        return commands

    @staticmethod
    def _validate_param_type(x, expected_type=None):
        u"""Validate parameter object type."""
        
        object_type = type(x)
        
        if expected_type is not None:
            
            if not isinstance(expected_type, type):
                raise TypeError("argument 'expected_type' is not a type object ~ {!r}".format(
                    expected_type))
            
            if object_type != expected_type:
                raise TypeError("parameter type ({}) differs from that expected ({})".format(
                    object_type, type_name))
        
        if object_type == unicode:
        
            _validate_ductile(x)
        
        elif object_type == dict:
        
            for key, value in x.items():
                _validate_ductile(key)
                _validate_ductile(value)
        
        elif object_type == list:
        
            for element in x:
                _validate_ductile(element)
        
        elif not _Chaperon.supported_types[object_type].is_ductile:
            raise TypeError("{} is not a valid parameter object ~ {!r}".format(
                object_type.__name__, x))
        
    @staticmethod
    def _validate_return_type(x, expected_type=None):
        u"""Validate return value type."""
        
        object_type = type(x)
        
        if expected_type is not None:
            
            if not isinstance(expected_type, type):
                raise TypeError("argument 'expected_type' is not a type object ~ {!r}".format(
                    expected_type))
            
            if object_type != expected_type:
                raise TypeError("parameter type ({}) differs from that expected ({})".format(
                    object_type, type_name))
        
        if object_type == unicode:
            
            _validate_ductile(x)
            
        elif object_type == dict:
            
            for key, value in x.items():
                _validate_ductile(key)
                _validate_ductile(value)
            
        elif object_type == list:
            
            try:
                for element in x:
                    _validate_ductile(element)
            except (TypeError, ValueError):
                for element in x:
                    _validate_delimitable(element)
            
        elif object_type not in _Chaperon.supported_types:
            raise TypeError("{} is not a valid return value object ~ {!r}".format(
                object_type, x))
        
    def __init__(self, function):
        u"""Init gactfunc wrapper from wrapped function."""
        
        # Init gactfunc data.
        self._data = dict()
        
        # Get function name.
        func_name = function.__name__
        
        # Get commands from function name.
        self._data[u'commands'] = self._parse_function_name(function)
        
        # Get function argspec.
        arg_spec = getargspec(function)
        
        # Check that there are no unenumerated arguments.
        if arg_spec.varargs is not None or arg_spec.keywords is not None:
            raise ValueError("{} cannot have unenumerated arguments".format(
                self.__class__.__name__))
        
        # Get enumerated parameter names.
        param_names = arg_spec.args
        
        # Check for reserved parameter names.
        res_params = [ p for p in param_names if p in _info[u'reserved_params'] ]
        if len(res_params) > 0:
            raise ValueError("{} {!r} uses reserved parameter names ~ {!r}".format(
                self.__class__.__name__, func_name, res_params))
        
        # Map formal keyword parameters to their defaults.
        if arg_spec.defaults is not None:
            i = len(arg_spec.defaults)
            spec_def_info = { k: x for k, x in
                zip(param_names[-i:], arg_spec.defaults) }
        else:
            spec_def_info = None
        
        # Set docstring info from gactfunc docstring.
        doc_info = self._parse_function_docstring(function)
        
        # Check that gactfunc has been documented.
        if doc_info is None:
            raise ValueError("{} {!r} is not documented".format(
                self.__class__.__name__, func_name))
        
        self._data[u'summary'] = doc_info[u'Summary']
        
        if u'Description' in doc_info:
            self._data[u'description'] = doc_info[u'Description']
        else:
            self._data[u'description'] = None
        
        # If parameters documented, validate them..
        if u'Args' in doc_info:
            
            # Set gactfunc parameter info from parsed docstring.
            self._data[u'param_spec'] = doc_info[u'Args']
            
            # Get set of documented parameters.
            doc_param_set = set(self._data[u'param_spec'])
            
            # Get set of parameters specified in function definition.
            spec_param_set = set(param_names)
            
            # Check for parameters in docstring but not in function definition.
            undef_params = list(doc_param_set - spec_param_set)
            if len(undef_params) > 0:
                raise ValueError("{} {!r} parameters documented but not defined ~ {!r}".format(
                    self.__class__.__name__, func_name, undef_params))
            
            # Check for parameters in function definition but not in docstring.
            undoc_params = list(spec_param_set - doc_param_set)
            if len(undoc_params) > 0:
                raise ValueError("{} {!r} parameters defined but not documented ~ {!r}".format(
                    self.__class__.__name__, func_name, undoc_params))
            
            # Validate any formal keyword parameters.
            if spec_def_info is not None:
                
                for param_name, default in spec_def_info.items():
                    
                    self._data[u'param_spec'][param_name][u'default'] = default
                    
                    # Skip unspecified defaults as we cannot validate them.
                    if default is None:
                        continue
                    
                    # Get parameter type.
                    type_name = self._data[u'param_spec'][param_name][u'type']
                    
                    # Check that the defined default value is of the
                    # type specified in the function documentation.
                    try:
                        gactfunc._validate_param_type(default, type_name)
                    except (TypeError, ValueError):
                        raise TypeError("{} definition has default type mismatch for parameter {!r}".format(
                            func_name, param_name))
                    
                    # Skip undocumented defaults.
                    if u'docstring_default' not in self._data[u'param_spec'][param_name]:
                        continue
                    
                    # Get string representation of docstring default.
                    docstring_default = self._data[u'param_spec'][param_name][u'docstring_default']
                    
                    try: # Coerce documented default from string.
                        coerced_default = _Chaperon._from_line[type_name](docstring_default)
                    except (KeyError, TypeError, ValueError):
                        raise TypeError("{} docstring has default type mismatch for parameter {!r}".format(
                            func_name, param_name))
                    
                    # Check that documented default matches actual default.
                    if coerced_default != default:
                        raise ValueError("{} has default value mismatch for parameter {!r}".format(
                            func_name, param_name))
        
        # ..otherwise, check that no parameters were defined.
        else:
            if len(param_names) > 0:
                raise ValueError("{} parameters defined but not documented ~ {!r}".format(
                    func_name, param_names))
            self._data[u'param_spec'] = None
            
        # Init input/output parameter set info.
        self._data[u'iop'] = { channel: None for channel in _info[u'iop'] }
        
        # Check if function contains explicit return.
        explicit_return = any( token == 'return' for token in
            reversed( _tokenise_source( getsource(function) ) ) )
        
        # If gactfunc has explicit return, check that it is
        # documented, then set return spec and IO pattern.
        if explicit_return:
            if not u'Returns' in doc_info:
                raise ValueError("{} return value defined but not documented".format(
                    func_name))
            self._data[u'return_spec'] = doc_info[u'Returns']
            self._data[u'iop'][u'output'] = { u'type': u'returned' }
        
        # ..otherwise, check that no return value was documented.
        else:
            if u'Returns' in doc_info:
                raise ValueError("{} return value documented but not defined".format(
                    func_name))
            self._data[u'return_spec'] = None
            
        # Get info on gactfunc input/output (IO) patterns.
        for channel in _info[u'iop']:
            
            # Check for each IO pattern, store info on matching pattern.
            for iop in _info[u'iop'][channel]:
                
                # Get info on this IO pattern.
                regex, metavar, flag = [ _info[u'iop'][channel][iop][k]
                    for k in (u'regex', u'metavar', u'flag') ]
                
                # Skip return-value IO pattern, already done.
                if iop == u'returned':
                    continue
                
                # Try to match parameter names to those expected for this parameter set.
                matches = [ regex.match(param_name) for param_name in param_names ]
                
                # Get mapping of params to matches for this parameter set.
                param2match = { p: m for p, m in zip(param_names, matches)
                    if m is not None }
                
                # If no parameters matched, skip to next parameter set.
                if len(param2match) == 0:
                    continue
                
                # Store matching IO pattern, checking for any conflicts.
                if self._data[u'iop'][channel] is not None:
                    raise ValueError("{} has conflicting {} IO patterns ~ {!r}".format(
                        func_name, channel, (self._data[u'iop'][channel][u'type'], iop)))
                self._data[u'iop'][channel] = { u'type': iop }
                
                # Store parameter info for each parameter in this set.
                for param_name in param2match:
                    
                    # If these are indexed input/output files, store
                    # parameter name by index, preferably as an integer..
                    if iop == u'indexed':
                        i = param2match[param_name].group(u'index')
                        try:
                            i = int(i)
                        except ValueError:
                            pass
                        self._data[u'iop'][channel].setdefault(u'params', dict())
                        self._data[u'iop'][channel][u'params'][i] = param_name
                        
                    # ..otherwise store set of parameter names.
                    else:
                        self._data[u'iop'][channel].setdefault(u'params', set())
                        self._data[u'iop'][channel][u'params'].add(param_name)
                        
                    # Check parameter type is as expected.
                    param_type = self._data[u'param_spec'][param_name][u'type']
                    if param_type != unicode:
                        raise TypeError("{} {} parameter must be of type unicode, not {!r} ~ {!r}".format(
                            func_name, channel, type_name, param_name))
                    
                if iop == u'indexed':
                    
                    # Check indexed parameters are as expected:
                    # * numbered indices start at 1, increment by 1
                    # * unindexed parameter not present without indexed parameters
                    indices = self._data[u'iop'][channel][u'params'].keys()
                    numbers = sorted( i for i in indices if i != u'U' )
                    if numbers[0] != 1 or any( j - i != 1
                        for i, j in zip(numbers[:-1], numbers[1:]) ):
                        raise ValueError("sparse indices in {} parameters of {}".format(
                            channel, func_name))
                    if u'U' in indices and len(indices) == 1:
                        raise ValueError("{} defines unindexed {1} parameter but not indexed {1} parameters".format(
                            func_name, channel))
                    
                    # Check required indexed parameters are as expected:
                    # * numbered indices start at 1, increment by 1
                    # * unindexed parameter not present without indexed parameters
                    indices = [ i for i, p in
                        self._data[u'iop'][channel][u'params'].items()
                        if u'default' not in self._data[u'param_spec'][p] ]
                    numbers = sorted( i for i in indices if i != u'U' )
                    if numbers[0] != 1 or any( j - i != 1
                        for i, j in zip(numbers[:-1], numbers[1:]) ):
                        raise ValueError("sparse indices in required {} parameters of {}".format(
                            channel, func_name))
                    if u'U' in indices and len(indices) == 1:
                        raise ValueError("{} requires unindexed {1} parameter but not indexed {1} parameters".format(
                            func_name, channel))
        
        self.__name__ = function.__name__
        self._data[u'function'] = function

    def __call__(self, *args, **kwargs):
        u"""Call gactfunc wrapper."""
        return self.function(*args, **kwargs)

    def _update_ap_spec(self):
        
        # Set argparser spec info from deep copy of gactfunc info.
        ap_spec = OrderedDict([
            (u'commands',       deepcopy(self._data[u'commands'])),
            (u'summary',         deepcopy(self._data[u'summary'])),
            (u'description', deepcopy(self._data[u'description'])),
            (u'params',       deepcopy(self._data[u'param_spec'])),
            (u'iop',                 deepcopy(self._data[u'iop']))
        ])
        
        # Init input/output parameter mappings.
        param2channel = dict()
        param2iop = dict()
        
        # If gactfunc has explicit return value,
        # create a command-line parameter for it.
        if self._data[u'return_spec'] is not None:
            
            ap_spec.setdefault(u'params', OrderedDict())
            
            # Set special parameter name for return value.
            param_name = u'retfile'
            
            # Set parameter info for return-value option.
            ap_spec[u'params'][param_name] = {
                u'default': u'-',
                u'description': self._data[u'return_spec'][u'description'],
                u'flag': _info[u'iop'][u'output'][u'returned'][u'flag'],
                u'metavar': _info[u'iop'][u'output'][u'returned'][u'metavar'],
                u'type': self._data[u'return_spec'][u'type']
            }
            
            param2channel[u'retfile'] = u'output'
            param2iop[u'retfile'] = u'returned'
            
            # Update argparser spec with return-value option.
            ap_spec[u'iop'][u'output'][u'params'] = set([u'retfile'])
        
        # Get info for IO parameters.
        for channel in ap_spec[u'iop']:
            
            # Skip if no relevant parameters.
            if ap_spec[u'iop'][channel] is None:
                continue
            
            # Get IO pattern type.
            iop = ap_spec[u'iop'][channel][u'type']
            
            # Skip return-value IO pattern, already done.
            if iop == u'returned':
                continue
            
            # Get info on this IO pattern.
            regex, metavar, flag = [ _info[u'iop'][channel][iop][k]
                for k in (u'regex', u'metavar', u'flag') ]
            
            # Get parameter names.
            if iop == u'indexed':
                param_names = ap_spec[u'iop'][channel][u'params'].values()
            else:
                param_names = list(ap_spec[u'iop'][channel][u'params'])
            
            # Update parameter info.
            for param_name in param_names:
                
                ap_spec[u'params'][param_name].update({
                    u'metavar': regex.sub(metavar, param_name),
                    u'flag': regex.sub(flag, param_name)
                })
                
                param2channel[param_name] = channel
                param2iop[param_name] = iop
                
        # Init flag set to check for conflicting option strings.
        flag2param = dict()
        
        # Prepare parameters for argument parser.
        for param_name in ap_spec[u'params']:
            
            # Get info for this parameter.
            param_info = ap_spec[u'params'][param_name]
            
            # Set parameter name to be used in argument parser.
            param_info[u'dest'] = param_name
            
            # If parameter has a default value, set as option or switch..
            if u'default' in param_info:
                
                param_info[u'required'] = False
                
                # If default value is False, assign to switches..
                if param_info[u'type'] == bool and param_info[u'default'] is False:
                    param_info[u'group'] = u'switch'
                # ..otherwise assign to optionals.
                else:
                    param_info[u'group'] = u'optional'
                
            # ..otherwise, assign to positional parameters.
            else:
                param_info[u'group'] = u'positional'
            
            # If this for input/output, change to IO parameter..
            if param_name in param2channel:
                
                channel = param2channel[param_name]
                
                # Input/output parameters are treated as optionals. If
                # parameter was positional, set default value, using
                # standard input or output where appropriate.
                if param_info[u'group'] == u'positional':
                    
                    iop = param2iop[param_name]
                    
                    if ( iop == u'indexed' and
                        param_name == ap_spec[u'iop'][channel][u'params'][1] ):
                        param_info[u'required'] = False
                        param_info[u'default'] = u'-'
                    elif iop == u'listed':
                        param_info[u'required'] = False
                        param_info[u'default'] = [u'-']
                    elif iop == u'single':
                        param_info[u'required'] = False
                        param_info[u'default'] = u'-'
                    else:
                        param_info[u'required'] = True
                        param_info[u'default'] = None
                
                # Mark as IO parameter.
                param_info[u'group'] = u'IO'
                
            # ..otherwise if parameter has a short form, convert to short form..
            elif param_name in _info[u'short_params']:
                
                # Check that this is not a compound type.
                if _Chaperon.supported_types[ param_info[u'type'] ].is_compound:
                    raise TypeError("cannot create short-form parameter {!r} of type {!r}".format(
                        param_name, param_info[u'type'].__name__))
                
                # Set flag to short form.
                param_info[u'flag'] = _info[u'short_params'][param_name][u'flag']
                
                # Check parameter type matches that of short-form.
                if param_info[u'type'] != _info[u'short_params'][param_name][u'type']:
                    raise TypeError("{} has type mismatch for short-form parameter {!r}".format(
                        self.__name__, param_name))
                
                # Short form parameters are treated as optionals.
                # If parameter was positional, set as required.
                if param_info[u'group'] == u'positional':
                    param_info[u'required'] = True
                    param_info[u'default'] = None
                
                try: # Check parameter default matches that of short-form.
                    assert param_info[u'default'] == _info[u'short_params'][param_name][u'default']
                except AssertionError:
                    raise ValueError("{} has default value mismatch for short-form parameter {!r}".format(
                        self.__name__, param_name))
                except KeyError:
                    pass
                
                try: # Check parameter requirement matches that of short-form.
                    assert param_info[u'required'] == _info[u'short_params'][param_name][u'required']
                except AssertionError:
                    raise ValueError("{} has requirement mismatch for short-form parameter {!r}".format(
                        self.__name__, param_name))
                except KeyError:
                    pass
                
                # Mark as short form optional.
                param_info[u'group'] = u'short'
                
            # ..otherwise if parameter is of a compound type, create up to two
            # (mutually exclusive) parameters: one to accept argument as string
            # (if ductile), the other to load it from a file (if fileable)..
            elif _Chaperon.supported_types[ param_info[u'type'] ].is_compound:
                
                # Compound parameters are treated as optionals.
                # If parameter was positional, set as required.
                if param_info[u'group'] == u'positional':
                    param_info[u'required'] = True
                    param_info[u'default'] = None
                
                # Mark as 'compound'.
                param_info[u'group'] = u'compound'
                
                # Set compound parameter title.
                param_info[u'title'] = u'{} argument'.format( param_name.replace(u'_', u'-') )
                
                # If parameter is of a ductile type, set flag for
                # it to be passed directly on the command line.
                if _Chaperon.supported_types[ param_info[u'type'] ].is_ductile:
                    param_info[u'flag'] = u'--{}'.format( param_name.replace(u'_', u'-') )
                
                # Set file parameter name.
                param_info[u'file_dest'] = u'{}_file'.format(param_name)
                
                # Set flag for parameter to be passed as a file.
                param_info[u'file_flag'] = file_flag = u'--{}-file'.format( param_name.replace(u'_', u'-') )
                
                # Check that file option string does
                # not conflict with existing options.
                if file_flag in flag2param:
                    raise ValueError("file flag of {} parameter {!r} conflicts with {!r}".format(
                        self.__name__, param_name, flag2param[file_flag]))
                flag2param[file_flag] = u'{} file flag'.format(param_name)
                
            # ..otherwise if option or switch,
            # create flag from parameter name.
            elif param_info[u'group'] in (u'optional', u'switch'):
                
                if len(param_name) > 1:
                    param_info[u'flag'] = u'--{}'.format( param_name.replace(u'_', u'-') )
                else:
                    param_info[u'flag'] = u'-{}'.format(param_name)
                
            # Append info to argument description as appropriate.
            if param_info[u'group'] != u'positional':
                if param_info[u'default'] is not None:
                    if param_info[u'group'] != u'switch' and not u'docstring_default' in param_info:
                        param_info[u'description'] = u'{} [default: {!r}]'.format(
                            param_info[u'description'], param_info[u'default'])
                elif param_info[u'required']:
                    param_info[u'description'] = u'{} [required]'.format(
                        param_info[u'description'])
            
            try: # Delete docstring default - no longer needed.
                del param_info[u'docstring_default']
            except KeyError:
                pass
            
            # Check for conflicting option strings.
            if u'flag' in param_info:
                flag = param_info[u'flag']
                if flag in flag2param:
                    raise ValueError("flag of {} parameter {!r} conflicts with {!r}".format(
                        self.__name__, param_name, flag2param[flag]))
                flag2param[flag] = param_name
            
            # Update parameter info.
            ap_spec[u'params'][param_name] = param_info
            
        self._data[u'ap_spec'] = ap_spec

class _GactfuncCollection(MutableMapping):
    u"""A gactfunc collection class."""
    
    def __init__(self):
        u"""Init gactfunc collection."""
        self._data = dict()
    
    def __delitem__(self, key):
        raise TypeError("{} object does not support item deletion".format(
            self.__class__.__name__))
    
    def __getitem__(self, commands):
        
        # Ensure variable 'commands' is a non-empty tuple of strings.
        if isinstance(commands, basestring):
            commands = (commands,)
        elif not isinstance(commands, tuple) or len(commands) == 0 or not all(
            isinstance(x, basestring) for x in commands ):
            raise TypeError("invalid {} item key {!r}".format(
                self.__class__.__name__, commands))
        
        try: # Get object indexed by the sequence of commands.
            d = self._data
            for cmd in commands[:-1]:
                d = d[cmd]._data
            value = d[ commands[-1] ]
        except KeyError:
            raise RuntimeError("failed to get {} item for commands ~ {!r}".format(
                self.__class__.__name__, ' '.join(commands)))
        
        return value
    
    def __iter__(self):
        return self._data.__iter__()
    
    def __len__(self):
        return self._data.__len__()
    
    def __repr__(self):
        name = self.__class__.__name__
        data = ', '.join([ '{!r}: {!r}'.format(k, self[k]) for k in self.keys() ])
        return '{}({})'.format(name, data)
    
    def __setitem__(self, commands, value):
        
        # Ensure variable 'commands' is a non-empty tuple of strings.
        if isinstance(commands, basestring):
            commands = (commands,)
        elif not isinstance(commands, tuple) or len(commands) == 0 or not all(
            isinstance(x, basestring) for x in commands ):
            raise TypeError("invalid {} item key {!r}".format(
                self.__class__.__name__, commands))
        
        if not isinstance(value, (_GactfuncCollection, _GactfuncSpec)):
            raise TypeError("{} object does not support values of type {!r}".format(
                self.__class__.__name__, type(value).__name__))
        
        try: # Set value of object indexed by the sequence of commands.
            d = self._data
            for cmd in commands[:-1]:
                if cmd in d:
                    assert isinstance(d[cmd], _GactfuncCollection)
                else:
                    d[cmd] = _GactfuncCollection()
                d = d[cmd]._data
            assert commands[-1] not in d
            d[ commands[-1] ] = value
        except AssertionError:
            raise RuntimeError("failed to set gactfunc collection item for commands ~ {!r}".format(
                ' '.join(commands)))
    
    def dump(self):
        """Dump gactfunc collection info."""
        
        # Ensure data directory exists.
        data_dir = os.path.join('gactutil', 'data')
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)
        
        # Dump gactfunc collection info.
        gaction_file = os.path.join(data_dir, 'gfi.p')
        with open(gaction_file, 'w') as fh:
            pickle.dump(self, fh)
        
    def func_specs(self):
        """Generate leaves of gactfunc command tree.
        
        Yields:
            _GactfuncSpec: Object containing information about an
                           individual gactfunc.
        """
        for _, _, func_spec in self.walk():
            if func_spec is not None:
                yield func_spec
    
    def load(self):
        u"""Load gactfunc collection info."""
        
        # Load gactfunc collection info.
        gaction_file = os.path.join('data', 'gfi.p')
        gaction_path = resource_filename('gactutil', gaction_file)
        with open(gaction_path, 'r') as fh:
            loaded = pickle.load(fh)
        self._data = loaded._data
        
    def populate(self):
        u"""Populate gactfunc collection from GACTutil package modules.
        
        NB: this function should only be called during package setup.
        """
        
        # Validate caller.
        caller_file, caller_func = [ (stack()[1])[i] for i in (1, 3) ]
        if caller_file != 'setup.py' or caller_func != '<module>':
            raise RuntimeError("{} can only be populated during GACTutil package setup".format(
                self.__class__.__name__))
        
        # Get mapping of package module names to their
        # paths relative to the package 'setup.py' script.
        mod_info = dict()
        for [ directory, subdirs, files ] in os.walk('gactutil'):
            prefix = directory.replace(os.sep, '.')
            mod_files = [ f for f in files if f.endswith('.py') ]
            mod_paths = [ os.path.join(directory, f) for f in mod_files ]
            mod_names = [ '{}.{}'.format(prefix, (os.path.splitext(f))[0])
                if f != '__init__.py' else prefix for f in mod_files ]
            mod_info.update( { name: path for name, path in zip(mod_names, mod_paths) } )
        
        # Search GACTutil modules for gactfunc instances (i.e. any functions
        # with the @gactfunc decorator). Create a function spec for each
        # gactfunc instance, while checking for conflicting gactfunc names.
        func_names = set()
        for mod_name, mod_path in mod_info.items():
            
            # Skip modules in which gactfuncs should not be defined.
            if mod_name in ('gactutil', 'gactutil.gaction'):
                continue
            
            # Load module.
            module = load_source(mod_name, mod_path)
            
            # Check members of module for gactfunc instances.
            for member_name, member in getmembers(module):
                
                # If this is a gactfunc, add its spec to gactfunc collection.
                if isinstance(member, gactfunc):
                    
                    # Check for gactfunc naming conflicts.
                    if member_name in func_names:
                        raise RuntimeError("conflicting gactfunc name ~ {!r}".format(member_name))
                    func_names.add(member_name)
                    
                    # Add gactfunc to collection.
                    self[member.commands] = _GactfuncSpec(mod_name,
                        member_name, member.ap_spec)
    
    def prep_argparser(self):
        """Prep command-line argument parser."""
        
        # Set version string.
        prog = os.path.splitext( os.path.basename(__file__) )[0]
        about = _read_about()
        version = '{}-{}'.format(prog, about['version'])
        
        # Init main argument parser.
        ap = ArgumentParser(description='\n{}\n\n{}\n'.format(version, __doc__))
        
        # Add version parameter.
        ap.add_argument('-v', '--version', action='version', version=version)
        
        # Add main subparser.
        sp = ap.add_subparsers(title='commands')
        
        # Ensure gactfunc collection info loaded.
        if len(self) == 0:
            self.load()
        
        # Init parser chain with main parser-subparser pair.
        parser_chain = OrderedDict([ ('gaction', (ap, sp)) ])
        
        # Setup argparser for every node in gactfunc command tree.
        for commands, subcommands, func_spec in self.walk():
            
            # Pop elements of parser chain until its commands
            # do not conflict with those of the current node.
            while len(parser_chain) > 1 and ( len(commands) < len(parser_chain) - 1 or
                next(reversed(parser_chain)) != commands[ len(parser_chain) - 2 ] ):
                parser_chain.popitem()
            
            # Push elements onto parser chain until its
            # commands match those of the current node.
            while len(commands) > len(parser_chain) - 1:
                
                # Get final command in parser chain.
                k = next(reversed(parser_chain))
                
                # Get first command for current node that doesn't
                # have a corresponding parser-subparser pair.
                cmd = commands[ len(parser_chain) - 1 ]
                
                # Create parser from subparser of previous parser chain element.
                cap = parser_chain[k][1].add_parser(cmd)
                
                # If this node has subcommands, add a subparser..
                if len(subcommands) > 0:
                    csp = cap.add_subparsers(title='modifiers')
                # ..otherwise leave subparser unset.
                else:
                    csp = None
                
                # Add parser-subparser pair to parser chain.
                parser_chain[cmd] = (cap, csp)
            
            # If this node corresponds to a gactfunc instance,
            # populate argument parser with info and parameters.
            if func_spec is not None:
                
                # Get function argparser spec.
                ap_spec = func_spec.ap_spec
                
                # Set gactfunc summary.
                cap.summary = ap_spec[u'summary']
                
                # Set gactfunc description, if present.
                if ap_spec[u'description'] is not None:
                    cap.description = u'\n\n{}'.format(ap_spec[u'description'])
                
                # If gactfunc has parameters..
                if u'params' in ap_spec:
                    
                    # ..add each parameter to the argument parser.
                    for param_name in ap_spec[u'params']:
                        
                        # Get info for this parameter.
                        param_info = ap_spec[u'params'][param_name]
                        
                        # Get parameter type name.
                        type_name = param_info[u'type'].__name__
                        
                        if param_info[u'group'] == u'positional':
                            
                            cap.add_argument(param_info[u'dest'],
                                help = param_info[u'description'])
                            
                        elif param_info[u'group'] == u'optional':
                            
                            cap.add_argument(param_info[u'flag'],
                                dest     = param_info[u'dest'],
                                metavar  = type_name.upper(),
                                default  = param_info[u'default'],
                                required = param_info[u'required'],
                                help     = param_info[u'description'])
                            
                        elif param_info[u'group'] == u'short':
                            
                            cap.add_argument(param_info[u'flag'],
                                dest     = param_info[u'dest'],
                                default  = param_info[u'default'],
                                required = param_info[u'required'],
                                help     = param_info[u'description'])
                            
                        elif param_info[u'group'] == u'switch':
                            
                            cap.add_argument(param_info[u'flag'],
                                dest   = param_info[u'dest'],
                                action = 'store_true',
                                help   = param_info[u'description'])
                            
                        elif param_info[u'group'] == u'compound':
                            
                            # If compound object parameter is of a parameter type,
                            # prepare to read from command line or load from file..
                            if _Chaperon.supported_types[ param_info[u'type'] ].is_ductile:
                                
                                # Set info for pair of alternative parameters.
                                item_help = 'Set {} from string.'.format(type_name)
                                file_help = 'Load {} from file.'.format(type_name)
                                
                                # Add (mutually exclusive) pair of alternative parameters.
                                ag = cap.add_argument_group(
                                    title       = param_info[u'title'],
                                    description = param_info[u'description'])
                                mxg = ag.add_mutually_exclusive_group(
                                    required    = param_info[u'required'])
                                mxg.add_argument(param_info[u'flag'],
                                    dest        = param_info[u'dest'],
                                    metavar     = 'STR',
                                    default     = param_info[u'default'],
                                    help        = item_help)
                                mxg.add_argument(param_info[u'file_flag'],
                                    dest        = param_info[u'file_dest'],
                                    metavar     = 'PATH',
                                    help        = file_help)
                                
                            # ..otherwise prepare to load it from file.
                            else:
                                
                                cap.add_argument(param_info[u'file_flag'],
                                    dest     = param_info[u'file_dest'],
                                    metavar  = 'PATH',
                                    default  = param_info[u'default'],
                                    required = param_info[u'required'],
                                    help     = param_info[u'description'])
                                
                        elif param_info[u'group'] == u'IO':
                            
                            cap.add_argument(param_info[u'flag'],
                                dest     = param_info[u'dest'],
                                metavar  = param_info[u'metavar'],
                                default  = param_info[u'default'],
                                required = param_info[u'required'],
                                help     = param_info[u'description'])
                
                # Set module and function name for this gactfunc.
                cap.set_defaults(
                    gactfunc_module = func_spec.module,
                    gactfunc_function = func_spec.function
                )
        
        return ap
    
    def proc_args(self, args):
        u"""Process parsed command-line arguments."""
        
        # Pop return-value output file, if present.
        retfile = args.__dict__.pop(u'retfile', None)
        
        try: # Pop gactfunc info, get function.
            mod_name = args.__dict__.pop(u'gactfunc_module')
            func_name = args.__dict__.pop(u'gactfunc_function')
            module = import_module(mod_name)
            function = getattr(module, func_name)
        except KeyError:
            raise RuntimeError("cannot run command - no function available")
        
        # Get parameter info for this gactfunc.
        param_info = function.ap_spec[u'params']
        
        # Process each argument.
        for param_name in function.params:
            
            # Assume argument is not to be loaded from file.
            filebound = False
            
            # Get expected argument type.
            param_type = param_info[param_name][u'type']
            
            # Get argument value.
            try:
                arg = args.__dict__[param_name]
            except KeyError: # Filebound compound type.
                arg = args.__dict__[param_name] = None
            
            # If parameter is in compound group,
            # check both alternative arguments.
            if param_info[param_name][u'group'] == u'compound':
                
                # Get file argument value.
                file_arg = args.__dict__[ param_info[param_name][u'file_dest'] ]
                
                # If file argument specified, set argument value from file
                # argument, indicate argument value is to be loaded from file..
                if file_arg is not None:
                    arg = file_arg
                    filebound = True
                # ..otherwise check argument specified (if required).
                elif arg is None and param_info[param_name][u'required']:
                    raise RuntimeError("{} is required".format(param_info[u'title']))
                
                # Remove file parameter from parsed arguments.
                del args.__dict__[ param_info[param_name][u'file_dest'] ]
            
            # If argument specified, get from file or string.
            if arg is not None:
                if filebound:
                    args.__dict__[param_name] = _Chaperon.from_file(arg, param_type).value
                elif param_info[param_name][u'group'] != u'switch':
                    args.__dict__[param_name] = _Chaperon.from_line(arg, param_type).value
        
        return function, args, retfile
    
    def walk(self):
        u"""Generate nodes of gactfunc command tree.
        
        Yields:
            tuple: Contains three elements: `commands` are the keys specifying
                   the current node in the gactfunc command tree, and matching
                   the commands input in the terminal; `subcommands` are the
                   subcommands available at this node; and `func_spec` gives
                   information about a gactfunc, if defined at the current node.
        """
        
        # Init command stack from root of gactfunc command tree.
        command_stack = [ (list(), self) ]
        
        # Init list of checked commands. This is used to handle the
        # (unlikely) event of a cycle in the gactfunc command tree.
        checked = list()
        
        while True:
            
            try: # Pop next node from command stack.
                (commands, x) = command_stack.pop()
            except IndexError:
                break
            
            # Skip previously checked nodes.
            if x in checked:
                continue
            
            # Mark node as checked.
            checked.append(x)
            
            # If this is a function specification, return that..
            if isinstance(x, _GactfuncSpec):
                
                # Set function info.
                func_spec = x
                
                # No subcommands.
                subcommands = tuple()
                
            # ..otherwise push subcommands onto command stack, and return those.
            elif isinstance(x, _GactfuncCollection):
                
                # Get subcommands in alphabetical order.
                subcommands = tuple( sorted( x.keys() ) )
                
                # Push subcommands onto command stack in reverse alphabetical
                # order, so that they will be yielded in alphabetical order.
                for k in reversed(subcommands):
                    command_stack.append( (commands + [k], x[k]) )
                
                # No function.
                func_spec = None
                
            else:
                raise RuntimeError("invalid gactfunc command tree")
            
            yield tuple(commands), tuple(subcommands), func_spec

################################################################################

def gaction(argv=None):
    u"""Run gaction command."""
    
    if argv is None:
        argv = sys.argv[1:]
    
    gf = _GactfuncCollection()
    
    ap = gf.prep_argparser()
    
    args = ap.parse_args(argv)
    
    function, args, retfile = gf.proc_args(args)
    
    return_value = function( **vars(args) )
     
    if function.return_spec is not None and return_value is not None:
        result = _Chaperon(return_value)
        result.to_file(retfile)

def main():
    gaction()

################################################################################

if __name__ == '__main__':
    main()

################################################################################
