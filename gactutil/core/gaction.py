#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil command-line interface."""

from __future__ import absolute_import
from __future__ import print_function
import argparse
from collections import deque
from collections import Mapping
from collections import Iterable
from collections import MutableMapping
from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from datetime import date
from functools import partial
from imp import load_source
from importlib import import_module
import inspect
import io
import os
import pickle
from pkg_resources import resource_filename
import re
import sys
from textwrap import dedent
from tokenize import generate_tokens
from tokenize import TokenError
from types import NoneType

from gactutil.core import const
from gactutil.core import contains_newline
from gactutil.core import fsdecode
from gactutil.core import fsencode
from gactutil.core import rellipt
from gactutil.core.about import about
from gactutil.core.deep import DeepDict
from gactutil.core.frozen import FrozenDict
from gactutil.core.frozen import FrozenList
from gactutil.core.frozen import FrozenTable
from gactutil.core.rw import TextReader
from gactutil.core.rw import TextWriter
from gactutil.core.unicsv import UTF8Reader
from gactutil.core.unicsv import UTF8Writer
from gactutil.core.uniyaml import unidump
from gactutil.core.uniyaml import uniload
from gactutil.core.uniyaml import unidump_scalar
from gactutil.core.uniyaml import uniload_scalar
from gactutil.core.uniyaml import YAMLError

################################################################################

_ginfo = {
    
    u'reserved_params': frozenset([
        u'commands',          # argparse commands option
        u'help',              # argparse help option
        u'version',           # argparse version option
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
    # parameters to take a short form on the command line.
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

def _float_from_file(f):
    u"""Get float from file."""
    with TextReader(f) as fh:
        s = fh.read()
    return _float_from_line(s)

def _float_from_line(s):
    u"""Get float from single-line string."""
    
    s = fsdecode(s)
    
    x = uniload_scalar(s)
    
    if type(x) == int:
        x = float(x)
    elif type(x) != float:
        raise ValueError("failed to convert line to valid float: {!r}".format(s))
    
    return x

def _FrozenDict_from_file(f):
    u"""Get FrozenDict from file."""
    
    try:
        with TextReader(f) as reader:
            x = uniload(reader)
        x = FrozenDict(x)
    except (IOError, TypeError, YAMLError):
        raise ValueError("failed to read valid FrozenDict from file: {!r}".format(f))
    
    return x

def _FrozenDict_from_line(s):
    u"""Get FrozenDict from single-line string."""
    
    s = fsdecode(s)
    
    if not ( s.startswith(u'{') and s.endswith(u'}') ):
        s = u'{' + s + u'}'
    
    try:
        x = uniload(s)
        x = FrozenDict(x)
    except (TypeError, YAMLError):
        raise ValueError("failed to convert line to valid FrozenDict: {!r}".format(s))
    
    return x

def _FrozenDict_to_file(x, f):
    u"""Output FrozenDict to file."""
    
    try:
        x = x.thaw()
        with TextWriter(f) as writer:
            unidump(x, writer, default_flow_style=False, width=sys.maxint)
    except (IOError, YAMLError):
        raise ValueError("failed to output FrozenDict to file: {!r}".format(x))

def _FrozenDict_to_line(x):
    u"""Convert FrozenDict to a single-line unicode string."""
    
    try:
        x = x.thaw()
        s = unidump(x, default_flow_style=True, width=sys.maxint)
        s = s.rstrip()
        assert not contains_newline(s)
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert FrozenDict to line: {!r}".format(x))
    
    return s

def _FrozenList_from_file(f):
    u"""Get FrozenList from file."""
    
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
                element = _FrozenDict_from_line(line)
            elif line.startswith(u'[') and line.endswith(u']'):
                element = _FrozenList_from_line(line)
            else:
                element = _scalar_from_line(line)
            
            # Append element to list.
            x.append(element)
        
        # Pop trailing empty lines.
        while len(x) > last_nonempty_line:
            x.pop()
    
    return FrozenList(x)

def _FrozenList_from_line(s):
    u"""Get FrozenList from single-line string."""
    
    s = fsdecode(s)
    
    if not ( s.startswith(u'[') and s.endswith(u']') ):
        s = u'[' + s + u']'
    
    try:
        x = uniload(s)
        x = FrozenList(x)
    except (TypeError, YAMLError):
        raise ValueError("failed to convert line to valid FrozenList: {!r}".format(s))
    
    return x

def _FrozenList_to_file(x, f):
    u"""Output FrozenList to file."""
    
    with TextWriter(f) as writer:
        
        for element in x:
            
            try:
                # Convert element to a single-line.
                if isinstance(element, FrozenDict):
                    line = _FrozenDict_to_line(element)
                elif isinstance(element, FrozenList):
                    line = _FrozenList_to_line(element)
                elif isinstance(element, _Chaperon.scalar_types):
                    line = _scalar_to_line(element)
                else:
                    raise TypeError
                
                # Write line to output file.
                writer.write( u'{}{}'.format(line.rstrip(), u'\n') )
                
            except (IOError, TypeError, ValueError):
                raise ValueError("failed to output FrozenList to file: {!r}".format(x))

def _FrozenList_to_line(x):
    u"""Convert FrozenList to a single-line unicode string."""
    
    try:
        x = x.thaw()
        s = unidump(x, default_flow_style=True, width=sys.maxint)
        s = s.rstrip()
        assert not contains_newline(s)
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert FrozenList to line: {!r}".format(x))
    
    return s

def _FrozenTable_from_file(f):
    u"""Get FrozenTable from file."""
    
    with TextReader(f) as fh:
        
        reader = UTF8Reader(fh)
        fieldnames = ()
        data = list()
        
        for r, row in enumerate(reader):
            if r > 0:
                data.append([ uniload_scalar(x) for x in row ])
            else:
                fieldnames = row # list of unicode strings
    
    return FrozenTable(data, fieldnames)

def _FrozenTable_from_line(s):
    u"""Get FrozenTable from single-line string."""
    d = _FrozenDict_from_line(s)
    return FrozenTable.from_dict({ k: [x] for k, x in d.items() })
    
def _FrozenTable_to_file(x, f):
    u"""Output FrozenTable to file."""
    with TextWriter(f) as fh:
        writer = UTF8Writer(fh)
        writer.writerow( x.fieldnames )
        for row in x.thaw():
            writer.writerow([ unidump_scalar(x) for x in row ])

def _FrozenTable_to_line(x):
    u"""Convert FrozenTable to a single-line unicode string."""
    if len(x) != 1:
        raise ValueError("cannot represent FrozenTable with {} rows on a single line".format(len(x)))
    d = x.to_dict()
    d = FrozenDict( (k, x[0]) for k, x in d.items() )
    return _FrozenDict_to_line(d)

def _long_from_file(f):
    u"""Get long integer from file."""
    with TextReader(f) as fh:
        s = fh.read()
    return _long_from_line(s)

def _long_from_line(s):
    u"""Get long integer from single-line string."""
    
    s = fsdecode(s)
    
    x = uniload_scalar(s)
    
    if type(x) == int:
        x = long(x)
    elif type(x) != long:
        raise ValueError("failed to convert line to valid long: {!r}".format(s))
    
    return x

def _scalar_from_file(f, scalar_type=None):
    u"""Get scalar from file."""
    with TextReader(f) as fh:
        s = fh.read()
    return _scalar_from_line(s, scalar_type=scalar_type)

def _scalar_from_line(s, scalar_type=None):
    u"""Get scalar from single-line string."""
    
    s = fsdecode(s)
    
    x = uniload_scalar(s)
    
    if scalar_type is not None and type(x) != scalar_type:
        raise ValueError("failed to convert line to valid {}: {!r}".format(
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
    supported_types = (NoneType, bool, unicode, float, int, long, datetime,
        date, FrozenDict, FrozenList, FrozenTable)
    
    # Scalar gactfunc parameter/return types.
    scalar_types = (NoneType, bool, unicode, float, int, long, datetime, date)
    
    # Mapping of each supported type name to its corresponding type object.
    _name2type = OrderedDict([
        (u'NoneType',    NoneType),
        (u'bool',        bool),
        (u'unicode',     unicode),
        (u'float',       float),
        (u'int',         int),
        (u'long',        long),
        (u'datetime',    datetime),
        (u'date',        date),
        (u'FrozenDict',  FrozenDict),
        (u'FrozenList',  FrozenList),
        (u'FrozenTable', FrozenTable)
    ])
    
    # Mapping of each supported type to its corresponding file-loading function.
    _from_file = OrderedDict([
        (NoneType,    partial(_scalar_from_file, scalar_type=NoneType)),
        (bool,        partial(_scalar_from_file, scalar_type=bool)),
        (unicode,     partial(_scalar_from_file, scalar_type=unicode)),
        (float,       _float_from_file),
        (int,         partial(_scalar_from_file, scalar_type=int)),
        (long,        _long_from_file),
        (datetime,    partial(_scalar_from_file, scalar_type=datetime)),
        (date,        partial(_scalar_from_file, scalar_type=date)),
        (FrozenDict,  _FrozenDict_from_file),
        (FrozenList,  _FrozenList_from_file),
        (FrozenTable, _FrozenTable_from_file)
    ])
    
    # Mapping of each supported type to its corresponding line-loading function.
    _from_line = OrderedDict([
        (NoneType,    partial(_scalar_from_line, scalar_type=NoneType)),
        (bool,        partial(_scalar_from_line, scalar_type=bool)),
        (unicode,     partial(_scalar_from_line, scalar_type=unicode)),
        (float,       _float_from_line),
        (int,         partial(_scalar_from_line, scalar_type=int)),
        (long,        _long_from_line),
        (datetime,    partial(_scalar_from_line, scalar_type=datetime)),
        (date,        partial(_scalar_from_line, scalar_type=date)),
        (FrozenDict,  _FrozenDict_from_line),
        (FrozenList,  _FrozenList_from_line),
        (FrozenTable, _FrozenTable_from_line)
    ])
    
    # Mapping of each supported type to its corresponding file-dumping function.
    _to_file = OrderedDict([
        (NoneType,    _scalar_to_file),
        (bool,        _scalar_to_file),
        (unicode,     _scalar_to_file),
        (float,       _scalar_to_file),
        (int,         _scalar_to_file),
        (long,        _scalar_to_file),
        (datetime,    _scalar_to_file),
        (date,        _scalar_to_file),
        (FrozenDict,  _FrozenDict_to_file),
        (FrozenList,  _FrozenList_to_file),
        (FrozenTable, _FrozenTable_to_file)
    ])
    
    # Mapping of each supported type to its corresponding line-dumping function.
    _to_line = OrderedDict([
        (NoneType,    _scalar_to_line),
        (bool,        _scalar_to_line),
        (unicode,     _scalar_to_line),
        (float,       _scalar_to_line),
        (int,         _scalar_to_line),
        (long,        _scalar_to_line),
        (datetime,    _scalar_to_line),
        (date,        _scalar_to_line),
        (FrozenDict,  _FrozenDict_to_line),
        (FrozenList,  _FrozenList_to_line),
        (FrozenTable, _FrozenTable_to_line)
    ])
    
    @staticmethod
    def _validate_ductile(x):
        u"""Validate ductile object type."""
        
        object_type = type(x)
        
        if object_type in _Chaperon.scalar_types:
            
            if object_type == unicode and contains_newline(x):
                raise ValueError("unicode string is not ductile:\n{!r}".format(x))
            
        elif object_type == FrozenDict:
            
            try:
                for key, value in x.items():
                    _Chaperon._validate_ductile(key)
                    _Chaperon._validate_ductile(value)
            except ValueError:
                raise ValueError("FrozenDict is not ductile:\n{!r}".format(x))
            
        elif object_type == FrozenList:
            
            try:
                for element in x:
                    _Chaperon._validate_ductile(element)
            except ValueError:
                raise ValueError("FrozenList is not ductile:\n{!r}".format(x))
            
        elif object_type == FrozenTable:
            
            # NB: FrozenTable headings/elements can't contain
            # newlines, so we can simply check if it has one row.
            if len(x) > 1:
                raise ValueError("FrozenTable is not ductile:\n{!r}".format(x))
            
        elif object_type not in _Chaperon.supported_types:
            raise TypeError("unknown gactfunc parameter/return type: {!r}".format(
                object_type.__name__))
    
    @classmethod
    def from_file(cls, filepath, object_type):
        u"""Get chaperon object from file."""
        try:
            x = _Chaperon._from_file[object_type](filepath)
        except KeyError:
            raise TypeError("unsupported type: {!r}".format(type(x).__name__))
        return _Chaperon(x)
        
    @classmethod
    def from_line(cls, string, object_type):
        u"""Get chaperon object from single-line string."""
        try:
            x = _Chaperon._from_line[object_type](string)
        except KeyError:
            raise TypeError("unsupported type: {!r}".format(type(x).__name__))
        return _Chaperon(x)
    
    @property
    def value(self):
        u"""Get chaperoned object."""
        return self._obj
    
    def __init__(self, x):
        if type(x) not in _Chaperon.supported_types:
            raise TypeError("unsupported type: {!r}".format(type(x).__name__))
        self._obj = x
        
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._obj))
    
    def __setattr__(self, name, value):
        if hasattr(self, '_obj'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[name] = value
    
    def __str__(self):
        return fsencode( self._to_line[type(self._obj)](self._obj) )
    
    def __unicode__(self):
        return self._to_line[type(self._obj)](self._obj)
        
    def to_file(self, filepath):
        u"""Output chaperoned object to file."""
        self._to_file[type(self._obj)](self._obj, filepath)

class _CommandsAction(argparse.Action):

    def __init__(self, option_strings, dest=argparse.SUPPRESS,
        default=argparse.SUPPRESS, help=None, gactnode_commands=()):
        
        super(_CommandsAction, self).__init__(option_strings=option_strings,
            dest=dest, default=default, nargs=0, help=help)
        
        if not isinstance(gactnode_commands, tuple):
            raise TypeError("gactfunc node commands must be of type 'tuple', not {!r}".format(
                type(gactnode_commands).__name__))
        
        self._gactnode_commands = gactnode_commands

    def __call__(self, parser, namespace, values, option_string=None):
        parser.print_usage()
        gfi = _GactfuncInterface()
        gfi.print_command_info(self._gactnode_commands)
        sys.exit(0)

class _GactfuncInterface(DeepDict):
    u"""A gactfunc collection class."""
    
    @classmethod
    def _validate_keys(cls, keys):
        
        keys = super(_GactfuncInterface, cls)._validate_keys(keys)
        
        # Check each key is of string type.
        for key in keys:
            if isinstance(key, Iterable) and not isinstance(key, basestring):
                raise TypeError("invalid {} item key: {!r}".format(
                    cls.__name__, keys))
        
        return keys
    
    def __init__(self):
        u"""Init gactfunc collection."""
        super(_GactfuncInterface, self).__init__()
    
    def __delitem__(self, key):
        raise TypeError("{} object does not support item deletion".format(
            self.__class__.__name__))
    
    def __setitem__(self, commands, value):
        
        try: # Check if called internally.
            parent = (inspect.stack())[1][0]
            called_internally = isinstance(parent.f_locals['self'], _GactfuncInterface)
        except (IndexError, KeyError):
            called_internally = False
        
        if not called_internally:
            raise TypeError("{} does not support direct item assignment".format(
                self.__class__.__name__))
        
        if not isinstance(value, _GactfuncSpec):
            raise TypeError("{} does not support values of type {!r}".format(
                self.__class__.__name__, type(value).__name__))
        
        super(_GactfuncInterface, self).__setitem__(commands, value)
    
    def dump(self):
        u"""Dump gactfunc collection info."""
        
        # Ensure data directory exists.
        data_dir = os.path.join(u'gactutil', u'data')
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)
        
        # Dump gactfunc collection info.
        gaction_file = os.path.join(data_dir, u'gfi.p')
        with open(gaction_file, 'w') as fh:
            pickle.dump(self, fh)
    
    def load(self):
        u"""Load gactfunc collection info."""
        gaction_file = os.path.join(u'data', u'gfi.p')
        gaction_path = resource_filename('gactutil', gaction_file)
        with open(gaction_path, 'r') as fh:
            loaded = pickle.load(fh)
        self._data.clear()
        for k in loaded:
            self._data[k] = loaded[k]
    
    def populate(self):
        u"""Populate gactfunc collection from GACTutil package modules.
        
        NB: this function should only be called during package setup.
        """
        
        try: # Validate caller.
            parentframe = (inspect.stack())[1][0]
            assert parentframe.f_globals['__file__'] == 'setup.py'
            assert parentframe.f_globals['__name__'] == '__main__'
        except (AssertionError, KeyError):
            raise RuntimeError("{} can only be populated during GACTutil "
                "package setup".format(self.__class__.__name__))
        
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
            if mod_name.startswith('gactutil.core') or mod_name in ('gactutil',
                'gactutil.gaction'):
                continue
            
            # Load module.
            module = load_source(mod_name, mod_path)
            
            # Check members of module for gactfunc instances.
            for member_name, member in inspect.getmembers(module):
                
                # If this is a gactfunc, add its spec to gactfunc collection.
                if isinstance(member, gactfunc):
                    
                    # Check for gactfunc naming conflicts.
                    if member_name in func_names:
                        raise RuntimeError("conflicting gactfunc name: {!r}".format(
                            member_name))
                    func_names.add(member_name)
                    
                    # Add gactfunc to collection.
                    self[member.commands] = _GactfuncSpec(mod_name,
                        member_name, member.ap_spec)
    
    def prep_argparser(self):
        u"""Prep command-line argument parser."""
        
        # Set version string.
        prog = os.path.splitext( os.path.basename(__file__) )[0]
        
        version = '{}-{}'.format(prog, about[u'version'])
        
        # Init main argument parser.
        ap = argparse.ArgumentParser(description=u'\n{}\n\n{}\n'.format(
            version, __doc__))
        
        # Add version parameter.
        ap.add_argument('-v', '--version', action='version', version=version)
        
        # Add 'commands' parameter.
        ap.add_argument('-c', '--commands', dest='commands', action=_CommandsAction,
            help='show terminal commands and exit')
        
        ap._optionals.title = 'keyword arguments'
        
        # Add main subparser.
        sp = ap.add_subparsers(title='commands')
        
        # Ensure gactfunc collection info loaded.
        if len(self) == 0:
            self.load()
        
        cap = None
        
        # Init parser chain with main parser-subparser pair.
        parser_chain = OrderedDict([ ('gaction', (ap, sp)) ])
        
        # Setup argparser for every node in gactfunc command tree.
        for commands, subcommands, node in self.walk():
            
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
            
            # If node is a gactfunc spec instance, populate
            # argument parser with info and parameters..
            if isinstance(node, _GactfuncSpec):
                
                # Get gactfunc argparser spec.
                ap_spec = node.ap_spec
                
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
                            
                            # Prepare to read compound object from
                            # command line or load from file.
                            
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
                            
                        elif param_info[u'group'] == u'IO':
                            
                            cap.add_argument(param_info[u'flag'],
                                dest     = param_info[u'dest'],
                                metavar  = param_info[u'metavar'],
                                default  = param_info[u'default'],
                                required = param_info[u'required'],
                                help     = param_info[u'description'])
                
                # Set module and function name for this gactfunc.
                cap.set_defaults(
                    gactfunc_module = node.module,
                    gactfunc_function = node.function
                )
                
            elif len(commands) > 0:
                
                cap.add_argument('-c', '--commands', dest='commands',
                    action=_CommandsAction, gactnode_commands=commands,
                    help='show terminal commands and exit')
            
            if cap is not None:
                cap._optionals.title = 'keyword arguments'
        
        return ap
    
    def print_command_info(self, gactnode_commands):
        u"""Print terminal command function info for given commands."""
        
        left_padding = u'  ... ' # padding before each modifier listing
        mid_padding = u'  '      # padding between modifiers and summaries
        
        # Ensure gactfunc collection info loaded.
        if len(self) == 0: self.load()
        
        # Assume line width is 80 columns.
        # TODO: get actual line width.
        line_width = 80
        
        # If command/modifiers specified, get node of gactfunc command tree..
        if len(gactnode_commands) > 0:
            node = DeepDict(self[gactnode_commands])
        # ..otherwise get root of gactfunc command tree.
        else:
            node = self
        
        # Get depth in gactfunc command tree
        # (i.e. number of commands/modifiers).
        gactnode_depth = len(gactnode_commands)
        
        subcommands = list()
        summaries = list()
        subcmd_widths = set()
        summary_widths = set()
        
        for func_info in node.leafvalues():
            
            # Get modifiers for each gactfunc relative
            # to this node of the gactfunc command tree.
            gactfunc_commands = func_info.ap_spec[u'commands']
            gactfunc_subcmds = gactfunc_commands[gactnode_depth:]
            subcommand = u' '.join(x for x in gactfunc_subcmds)
            subcmd_widths.add( len(subcommand) )
            subcommands.append(subcommand)
            
            # Get summary for each gactfunc.
            summary = func_info.ap_spec[u'summary']
            summary_widths.add( len(summary) )
            summaries.append(summary)
        
        # Get space used by subcommands.
        subcmd_space = max(subcmd_widths)
        
        # Get space available for summaries.
        summary_space = line_width - ( len(left_padding) +
            subcmd_space + len(mid_padding) )
        
        if summary_space >= max(summary_widths):
            summary_mode = u'unchanged'
        elif summary_space >= min(summary_widths):
            summary_mode = u'ellipted'
        else:
            summary_mode = u'omitted'
        
        # Print listing of available terminal commands.
        print(u"\nterminal commands: ")
        print(u"\n  gaction {}...\n".format( ''.join('{} '.format(cmd)
            for cmd in gactnode_commands) ) )
        
        for subcommand, summary in zip(subcommands, summaries):
            
            subcommand = subcommand.ljust(subcmd_space)
            
            if summary_mode == u'unchanged':
                command_info = u'{}{}{}{}'.format(left_padding,
                    subcommand, mid_padding, summary)
            elif summary_mode == u'ellipted':
                command_info = u'{}{}{}{}'.format(left_padding,
                    subcommand, mid_padding, rellipt(summary, summary_space))
            elif summary_mode == u'omitted':
                command_info = u'{}{}'.format(left_padding, subcommand)
            
            command_info = fsencode(command_info)
            
            print(command_info)
    
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
            
            try: # Get argument value.
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
    
    def __setattr__(self, name, value):
        if hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[name] = value

################################################################################

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
        if not inspect.isfunction(function):
            return TypeError("object is not a function: {!r}".format(func_name))
        
        # Get function docstring.
        docstring = function.__doc__
        
        # Set default parsed docstring.
        doc_info = None
        
        # Parse docstring if present.
        if docstring is not None and docstring.strip() != u'':
            
            # Init raw docstring.
            raw_info = OrderedDict()
            
            # Split docstring into lines.
            lines = deque( docstring.splitlines() )
            
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
            lines = deque( (dedent( u'\n'.join(lines) ) ).splitlines() )
            
            # Init docstring description.
            raw_info[u'Description'] = list()
            
            # Docstring description includes everything before the first header.
            h = u'Description'
            
            # Group content by docstring section.
            while len(lines) > 0:
                
                # Get first of remaining lines.
                line = lines.popleft()
                
                # Try to match line to a docstring header.
                m = _ginfo[u'regex'][u'docstring_header'].match(line)
                
                # If matches, set header of new section..
                if m is not None:
                    
                    # Set current header.
                    h = m.group(1)
                    
                    # Map header to alias, if relevant.
                    if h in _ginfo[u'docstring_headers'][u'alias_mapping']:
                        h = _ginfo[u'docstring_headers'][u'alias_mapping'][h]
                    
                    # Check header is known.
                    if h not in _ginfo[u'docstring_headers'][u'known']:
                        raise ValueError("unknown docstring header: {!r}".format(h))
                    
                    # Check header is supported.
                    if h not in _ginfo[u'docstring_headers'][u'supported']:
                        raise ValueError("unsupported docstring header: {!r}".format(h))
                    
                    # Check for duplicate headers.
                    if h in raw_info:
                        raise ValueError("duplicate docstring header: {!r}".format(h))
                    
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
                raw_info[h] = ( dedent( u'\n'.join(raw_info[h]) ) ).splitlines()
                
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
                            m = _ginfo[u'regex'][u'docstring_param'].match(line)
                            
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
                                    raise ValueError("{} docstring contains duplicate parameter: {!r}".format(
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
                                raise ValueError("failed to parse docstring for gactfunc: {!r}".format(
                                    func_name))
                    
                    # Validate docstring default info.
                    for param_name in param_info:
                        
                        # Try to match default definition pattern in parameter description.
                        defaults = _ginfo[u'regex'][u'docstring_default'].findall(
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
                            m = _ginfo[u'regex'][u'docstring_return'].match(line)
                            
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
                                raise ValueError("failed to parse docstring for gactfunc: {!r}".format(
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
        if not inspect.isfunction(function):
            return TypeError("object is not a function: {!r}".format(func_name))
        
        # Try to match function name to expected gactfunc pattern.
        m = _ginfo[u'regex'][u'gactfunc'].match(func_name)
        
        try: # Split gactfunc name into commands.
            assert m is not None
            commands = tuple( func_name.split(u'_') )
            assert len(commands) >= 2
            assert len(set(commands)) == len(commands)
        except AssertionError:
            raise ValueError("gactfunc {!r} does not follow naming convention".format(
                func_name))
        
        # Check gactfunc name is not too long for display at terminal.
        # Assume 80 characters, subtract 6 for display formatting.
        max_name_length = 74
        if len(func_name) > max_name_length:
            raise ValueError("gactfunc {!r} has {} characters (max={})".format(
                func_name, len(func_name), max_name_length))
        
        return commands
    
    @staticmethod
    def _tokenise_source(source):
        """Tokenise source code into token strings."""
        buf = io.BytesIO(source)
        try:
            token_strings = [ x[1] for x in generate_tokens(buf.readline) ]
        except TokenError:
            raise RuntimeError("failed to tokenise source")
        return token_strings
    
    @staticmethod
    def _validate_argument(x, param_type=None):
        u"""Validate argument."""
        
        if param_type is not None and type(x) != param_type:
            
            try:
                if param_type == FrozenTable and isinstance(x, Table):
                    x = FrozenTable.freeze(x)
                elif param_type == FrozenDict and isinstance(x, Mapping):
                    x = FrozenDict.freeze(x)
                elif ( param_type == FrozenList and isinstance(x, Iterable) and
                    not isinstance(x, basestring) ):
                    x = FrozenList.freeze(x)
                elif param_type == float and isinstance(x, int):
                    x = float(x)
                elif param_type == long and isinstance(x, int):
                    x = long(x)
                else:
                    raise TypeError
                
            except TypeError:
                raise TypeError("argument type ({!r}) differs from that expected ({!r})".format(
                    type(x).__name__, param_type.__name__))
        
        _Chaperon._validate_ductile(x)
        
        return x
        
    @staticmethod
    def _validate_return_value(x, return_type=None):
        u"""Validate return value."""
        
        if return_type is not None and type(x) != return_type:
            raise TypeError("return value type ({}) differs from that expected ({})".format(
                type(x).__name__, return_type.__name__))
        
        _Chaperon._validate_ductile(x)
        
    def __init__(self, function):
        u"""Init gactfunc wrapper from wrapped function."""
        
        try:
            parentframe = (inspect.stack())[1][0]
            assert parentframe.f_globals['__name__'].startswith('gactutil')
        except (AssertionError, IndexError, KeyError):
            raise RuntimeError("{} decorator can only be invoked within the "
                "GACTutil package".format(self.__class__.__name__))
        
        # Init gactfunc data.
        self._data = dict()
        
        # Get function name.
        func_name = function.__name__
        
        # Get commands from function name.
        self._data[u'commands'] = self._parse_function_name(function)
        
        # Get function argspec.
        arg_spec = inspect.getargspec(function)
        
        # Check that there are no unenumerated arguments.
        if arg_spec.varargs is not None or arg_spec.keywords is not None:
            raise ValueError("{} cannot have unenumerated arguments".format(
                self.__class__.__name__))
        
        # Get enumerated parameter names.
        param_names = arg_spec.args
        
        # Check for reserved parameter names.
        res_params = [ p for p in param_names if p in _ginfo[u'reserved_params'] ]
        if len(res_params) > 0:
            raise ValueError("{} {!r} uses reserved parameter names: {!r}".format(
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
            param_spec = doc_info[u'Args']
            
            # Get set of documented parameters.
            doc_param_set = set(param_spec)
            
            # Get set of parameters specified in function definition.
            spec_param_set = set(param_names)
            
            # Check for parameters in docstring but not in function definition.
            undef_params = list(doc_param_set - spec_param_set)
            if len(undef_params) > 0:
                raise ValueError("{} {!r} parameters documented but not defined: {!r}".format(
                    self.__class__.__name__, func_name, undef_params))
            
            # Check for parameters in function definition but not in docstring.
            undoc_params = list(spec_param_set - doc_param_set)
            if len(undoc_params) > 0:
                raise ValueError("{} {!r} parameters defined but not documented: {!r}".format(
                    self.__class__.__name__, func_name, undoc_params))
            
            # Validate any formal keyword parameters.
            if spec_def_info is not None:
                
                for param_name, default in spec_def_info.items():
                    
                    # Skip unspecified defaults as we cannot validate them.
                    if default is None:
                        param_spec[param_name][u'default'] = default
                        continue
                    
                    # Get specified parameter type.
                    param_type = param_spec[param_name][u'type']
                    
                    # Ensure that the defined default value is of the
                    # type specified in the function documentation.
                    try:
                        default = gactfunc._validate_argument(default, param_type)
                        param_spec[param_name][u'default'] = default
                    except (TypeError, ValueError):
                        raise TypeError("definition of {} {!r} has default type mismatch for parameter {!r}".format(
                            self.__class__.__name__, func_name, param_name))
                    
                    # Skip undocumented defaults.
                    if u'docstring_default' not in param_spec[param_name]:
                        continue
                    
                    # Get string representation of docstring default.
                    docstring_default = param_spec[param_name][u'docstring_default']
                    
                    try: # Coerce documented default from string.
                        coerced_default = _Chaperon._from_line[param_type](docstring_default)
                    except (KeyError, TypeError, ValueError):
                        raise TypeError("docstring of {} {!r} has default type mismatch for parameter {!r}".format(
                            self.__class__.__name__, func_name, param_name))
                    
                    # Check that documented default matches actual default.
                    if coerced_default != default:
                        raise ValueError("{} {!r} has default value mismatch for parameter {!r}".format(
                            self.__class__.__name__, func_name, param_name))
        
        # ..otherwise, check that no parameters were defined.
        else:
            if len(param_names) > 0:
                raise ValueError("{} {!r} parameters defined but not documented: {!r}".format(
                    self.__class__.__name__, func_name, param_names))
            param_spec = None
        
        self._data[u'param_spec'] = param_spec
        
        # Init input/output parameter set info.
        self._data[u'iop'] = { channel: None
            for channel in _ginfo[u'iop'] }
        
        # Check if function contains explicit return.
        explicit_return = any( token == 'return' for token in
            reversed( self._tokenise_source( inspect.getsource(function) ) ) )
        
        # If gactfunc has explicit return, check that it is
        # documented, then set return spec and IO pattern.
        if explicit_return:
            if not u'Returns' in doc_info:
                raise ValueError("{} {!r} return value defined but not documented".format(
                    self.__class__.__name__, func_name))
            self._data[u'return_spec'] = doc_info[u'Returns']
            self._data[u'iop'][u'output'] = { u'type': u'returned' }
        
        # ..otherwise, check that no return value was documented.
        else:
            if u'Returns' in doc_info:
                raise ValueError("{} {!r} return value documented but not defined".format(
                      self.__class__.__name__, func_name))
            self._data[u'return_spec'] = None
            
        # Get info on gactfunc input/output (IO) patterns.
        for channel in _ginfo[u'iop']:
            
            # Check for each IO pattern, store info on matching pattern.
            for iop in _ginfo[u'iop'][channel]:
                
                # Get info on this IO pattern.
                regex, metavar, flag = [ _ginfo[u'iop'][channel][iop][k]
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
                    raise ValueError("{} {!r} has conflicting IO patterns".format(
                        self.__class__.__name__, func_name))
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
                        raise TypeError("{} {!r} {} parameter {!r} must be of type 'unicode', not {!r}".format(
                            self.__class__.__name__, func_name, channel, param_name, param_type.__name__))
                
                if iop == u'indexed':
                    
                    # Check indexed parameters are as expected:
                    # * numbered indices start at 1, increment by 1
                    # * unindexed parameter not present without indexed parameters
                    indices = self._data[u'iop'][channel][u'params'].keys()
                    numbers = sorted( i for i in indices if i != u'U' )
                    if numbers[0] != 1 or any( j - i != 1
                        for i, j in zip(numbers[:-1], numbers[1:]) ):
                        raise ValueError("sparse indices in {} parameters of {} {!r}".format(
                            channel, self.__class__.__name__, func_name))
                    if u'U' in indices and len(indices) == 1:
                        raise ValueError("{} {!r} defines unindexed {2} parameter but not indexed {2} parameters".format(
                            self.__class__.__name__, func_name, channel))
                    
                    # Check required indexed parameters are as expected:
                    # * numbered indices start at 1, increment by 1
                    # * unindexed parameter not present without indexed parameters
                    indices = [ i for i, p in
                        self._data[u'iop'][channel][u'params'].items()
                        if u'default' not in self._data[u'param_spec'][p] ]
                    numbers = sorted( i for i in indices if i != u'U' )
                    if numbers[0] != 1 or any( j - i != 1
                        for i, j in zip(numbers[:-1], numbers[1:]) ):
                        raise ValueError("sparse indices in required {} parameters of {} {!r}".format(
                            channel, self.__class__.__name__, func_name))
                    if u'U' in indices and len(indices) == 1:
                        raise ValueError("{} {!r} requires unindexed {2} parameter but not indexed {2} parameters".format(
                            self.__class__.__name__, func_name, channel))
        
        self.__name__ = function.__name__
        self._data[u'function'] = function

    def __call__(self, *args, **kwargs):
        u"""Call gactfunc wrapper."""
        
        try: # Check if called by gactfunc.
            grandparent = (inspect.stack())[2][0]
            called_by_gactfunc = isinstance(grandparent.f_locals['self'], gactfunc)
        except (IndexError, KeyError):
            called_by_gactfunc = False
        
        # Bind arguments to gactfunc parameters.
        kwargs = inspect.getcallargs(self.function, *args, **kwargs)
        
        if not called_by_gactfunc:
            
            param_spec = self._data[u'param_spec']
            
            for param_name in param_spec:
                
                param_type = param_spec[param_name][u'type']
                
                arg_value = kwargs[param_name]
                
                if ( u'default' not in param_spec[param_name] or
                    param_spec[param_name][u'default'] is not None or
                    arg_value is not None ):
                    kwargs[param_name] = self._validate_argument(
                        arg_value, param_type=param_type)
        
        return self.function(**kwargs)

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
                u'flag': _ginfo[u'iop'][u'output'][u'returned'][u'flag'],
                u'metavar': _ginfo[u'iop'][u'output'][u'returned'][u'metavar'],
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
            regex, metavar, flag = [ _ginfo[u'iop'][channel][iop][k]
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
            elif param_name in _ginfo[u'short_params']:
                
                # Check that this is not a compound type.
                if param_info[u'type'] not in _Chaperon.scalar_types:
                    raise TypeError("cannot create short-form parameter {!r} of type {!r}".format(
                        param_name, param_info[u'type'].__name__))
                
                # Set flag to short form.
                param_info[u'flag'] = _ginfo[u'short_params'][param_name][u'flag']
                
                # Check parameter type matches that of short-form.
                if param_info[u'type'] != _ginfo[u'short_params'][param_name][u'type']:
                    raise TypeError("{} {!r} has type mismatch for short-form parameter {!r}".format(
                        self.__class__.__name__, self.__name__, param_name))
                
                # Short form parameters are treated as optionals.
                # If parameter was positional, set as required.
                if param_info[u'group'] == u'positional':
                    param_info[u'required'] = True
                    param_info[u'default'] = None
                
                try: # Check parameter default matches that of short-form.
                    assert param_info[u'default'] == _ginfo[u'short_params'][param_name][u'default']
                except AssertionError:
                    raise ValueError("{} {!r} has default value mismatch for short-form parameter {!r}".format(
                        self.__class__.__name__, self.__name__, param_name))
                except KeyError:
                    pass
                
                try: # Check parameter requirement matches that of short-form.
                    assert param_info[u'required'] == _ginfo[u'short_params'][param_name][u'required']
                except AssertionError:
                    raise ValueError("{} {!r} has requirement mismatch for short-form parameter {!r}".format(
                        self.__class__.__name__, self.__name__, param_name))
                except KeyError:
                    pass
                
                # Mark as short form optional.
                param_info[u'group'] = u'short'
                
            # ..otherwise if parameter is of a compound type, create up to two
            # (mutually exclusive) parameters: one to accept argument as string
            # (if ductile), the other to load it from a file (if fileable)..
            elif param_info[u'type'] not in _Chaperon.scalar_types:
                
                # Compound parameters are treated as optionals.
                # If parameter was positional, set as required.
                if param_info[u'group'] == u'positional':
                    param_info[u'required'] = True
                    param_info[u'default'] = None
                
                # Mark as 'compound'.
                param_info[u'group'] = u'compound'
                
                # Set compound parameter title.
                param_info[u'title'] = u'{} argument'.format( param_name.replace(u'_', u'-') )
                
                # Set flag for parameter to be passed directly on the command line.
                # NB: this flag can only be used for an argument that fits in a single line.
                param_info[u'flag'] = u'--{}'.format( param_name.replace(u'_', u'-') )
                
                # Set file parameter name.
                param_info[u'file_dest'] = u'{}_file'.format(param_name)
                
                # Set flag for parameter to be passed as a file.
                param_info[u'file_flag'] = file_flag = u'--{}-file'.format( param_name.replace(u'_', u'-') )
                
                # Check that file option string does
                # not conflict with existing options.
                if file_flag in flag2param:
                    raise ValueError("{} {!r} has file flag parameter {!r} conflicting with {!r}".format(
                        self.__class__.__name__, self.__name__, param_name, flag2param[file_flag]))
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
                    raise ValueError("{} {!r} has flag parameter {!r} conflicting with {!r}".format(
                        self.__class__.__name__, self.__name__, param_name, flag2param[flag]))
                flag2param[flag] = param_name
            
            # Update parameter info.
            ap_spec[u'params'][param_name] = param_info
            
        self._data[u'ap_spec'] = ap_spec

################################################################################

def gaction(argv=None):
    u"""Run gaction command."""
    
    if argv is None:
        argv = sys.argv[1:]
    
    gfi = _GactfuncInterface()
    
    ap = gfi.prep_argparser()
    
    args = ap.parse_args(argv)
    
    function, args, retfile = gfi.proc_args(args)
    
    return_value = function( **vars(args) )
     
    if function.return_spec is not None:
        result = _Chaperon(return_value)
        result.to_file(retfile)

def main():
    gaction()

################################################################################

__all__ = ['gactfunc', 'gaction']

if __name__ == '__main__':
    main()

################################################################################
