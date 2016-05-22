#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil YAML utilities."""

from __future__ import absolute_import
from collections import OrderedDict
from datetime import date
from datetime import datetime
from types import NoneType

from yaml import safe_dump
from yaml import safe_load
from yaml import YAMLError
from yaml.constructor import SafeConstructor
from yaml.dumper import SafeDumper
from yaml.loader import SafeLoader
from yaml.nodes import ScalarNode
from yaml.representer import SafeRepresenter

################################################################################

_info = {
    
    # Supported YAML scalar tags.
    u'yaml_scalar_tags': (
        u'tag:yaml.org,2002:null',
        u'tag:yaml.org,2002:bool',
        u'tag:yaml.org,2002:str',
        u'tag:yaml.org,2002:float',
        u'tag:yaml.org,2002:int',
        u'tag:yaml.org,2002:timestamp'
    ),
    
    # Supported YAML scalar types.
    u'yaml_scalar_types': (
        NoneType,
        bool,
        unicode,
        str,
        float,
        int,
        datetime,
        date
    )
}

################################################################################

def construct_yaml_str(self, node):
    u"""Construct YAML string as unicode, even if convertible to a byte string."""
    return self.construct_scalar(node)

# Ensure SafeDumper represents str as str, and unicode as unicode.
SafeDumper.add_representer(str, SafeRepresenter.represent_str)
SafeDumper.add_representer(unicode, SafeRepresenter.represent_unicode)

class SafeLoader(SafeLoader):
    u"""YAML unicode-preferring loader."""
    pass

# Ensure SafeLoader constructs strings as unicode.
SafeLoader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)

# Ensure SafeConstructor constructs strings as unicode.
SafeConstructor.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)

def _init_scalar_representer_info():
    u"""Init YAML scalar representer method info."""
    
    representers = dict()
    
    for t in SafeDumper.yaml_representers:
        if t in _info[u'yaml_scalar_types']:
            representers[t] = SafeDumper.yaml_representers[t]
    
    return representers

def _init_scalar_resolver_info():
    """Init YAML scalar resolver method info."""
    
    resolvers = dict()
    
    for prefix in SafeLoader.yaml_implicit_resolvers:
        for tag, regexp in SafeLoader.yaml_implicit_resolvers[prefix]:
            if tag in _info[u'yaml_scalar_tags']:
                resolvers.setdefault(prefix, OrderedDict())
                resolvers[prefix][tag] = regexp
    
    # Ensure empty string is resolved as None.
    resolvers[u''] = OrderedDict( SafeLoader.yaml_implicit_resolvers[u'~'] )
    
    return resolvers

def _resolve_scalar(value):
    """Resolve YAML scalar tag from string representation."""
    
    if not isinstance(value, basestring):
        raise TypeError("cannot resolve value of type {!r}".format(
            type(value).__name__))
    
    try:
        key = value[0]
    except IndexError: # Resolve empty string as None.
        return u'tag:yaml.org,2002:null'
    
    resolvers = _scalar_resolver_methods.get(key, OrderedDict())
    
    for tag, regexp in resolvers.items():
        if regexp.match(value):
            return tag
    
    return u'tag:yaml.org,2002:str'

# Init YAML scalar handlers.
_scalar_constructor = SafeConstructor()
_scalar_representer = SafeRepresenter()
_scalar_representer_methods = _init_scalar_representer_info()
_scalar_resolver_methods = _init_scalar_resolver_info()

################################################################################

def is_multiline_string(string):
    """Test if object is a multiline string."""
    
    known_line_breaks = (u'\r\n', u'\n', u'\r')
    
    if isinstance(string, basestring):
        
        for line_break in known_line_breaks:
            
            if string.endswith(line_break):
                string = string[:-len(line_break)]
                break
        
        if any( line_break in string for line_break in known_line_breaks ):
            return True
    
    return False

def unidump(data, stream=None, **kwds):
    u"""Dump data to YAML unicode stream."""
    
    fixed_kwargs = { 'allow_unicode': True, 'encoding': None }
    
    for k, x in fixed_kwargs.items():
        if k in kwds:
            raise RuntimeError("cannot set reserved keyword argument {!r}".format(k))
        kwds[k] = x
    
    return safe_dump(data, stream=stream, **kwds)

def uniload(stream):
    u"""Load data from YAML unicode stream."""
    return safe_load(stream)

def unidump_scalar(data, stream=None):
    u"""Dump scalar to YAML unicode stream."""
    
    try:
        method = _scalar_representer_methods[type(data)]
    except KeyError:
        raise TypeError("cannot dump data of type {!r}".format(type(data).__name__))
    
    if is_multiline_string(data):
        raise ValueError("cannot dump multiline string ~ {!r}".format(data))
    
    node = method(_scalar_representer, data)
    
    if stream is not None:
        stream.write(u'{}{}'.format(node.value.rstrip(u'\n'), u'\n'))
    else:
        return node.value

def uniload_scalar(stream):
    u"""Load scalar from YAML unicode stream."""
    
    if isinstance(stream, basestring):
        
        lines = stream.splitlines()
        
    else:
        
        try:
            lines = [ line for line in stream ]
        except TypeError:
            raise TypeError("cannot load scalar from input of type {!r}".format(
                type(stream).__name__))
    
    # Strip YAML comments and flanking whitespace from each line.
    lines = [ ystrip(line) for line in lines ]
    
    # Strip trailing empty lines.
    while len(lines) > 0 and lines[-1] == u'':
        lines.pop()
    
    # Allow for document end indicator.
    if len(lines) > 2 or ( len(lines) == 2 and lines[1] != u'...' ):
        raise ValueError("cannot load scalar from multiline input")
    
    try: # Take scalar string representation from first line.
        value = lines[0]
    except IndexError: # Resolve empty stream as None.
        return None
    
    # Resolve and construct scalar object from string representation.
    tag = _resolve_scalar(value)
    node = ScalarNode(tag, value)
    return _scalar_constructor.construct_object(node)

def ystrip(line):
    u"""Strip YAML comments and flanking whitespace from a single-line string."""
    
    if not isinstance(line, basestring):
        raise TypeError("cannot strip object of type {!r}".format(
                type(line).__name__))
    
    if is_multiline_string(line):
        raise ValueError("cannot strip multiline string ~ {!r}".format(line))
    
    try: # Strip comments.
        j = line.index(u'#')
    except ValueError:
        pass
    else:
        line = line[:j]
    
    # Strip leading/trailing whitespace.
    line = line.strip()
    
    return line

################################################################################
