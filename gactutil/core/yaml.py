#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil YAML utilities.

This module contains classes UniConstructor, UniRepresenter, UniDumper, and
UniLoader, which are almost identical to PyYAML classes SafeConstructor,
SafeRepresenter, Dumper, and Loader, respectively. As such, these classes
are included under the following license.

Copyright (c) 2006 Kirill Simonov

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from __future__ import absolute_import
from collections import OrderedDict
import datetime

from yaml import load
from yaml import dump
from yaml import MappingNode
from yaml import ScalarNode
from yaml import SequenceNode
from yaml import YAMLError

from yaml.composer import Composer
from yaml.constructor import SafeConstructor
from yaml.constructor import BaseConstructor
from yaml.constructor import ConstructorError
from yaml.emitter import Emitter
from yaml.parser import Parser
from yaml.reader import Reader
from yaml.representer import BaseRepresenter
from yaml.representer import RepresenterError
from yaml.representer import SafeRepresenter
from yaml.resolver import Resolver
from yaml.scanner import Scanner
from yaml.serializer import Serializer

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
        type(None),
        bool,
        unicode,
        str,
        float,
        int,
        long,
        datetime.datetime,
        datetime.date
    )
}

################################################################################

class UniConstructor(BaseConstructor):
    
    # Set class data members directly from SafeConstructor.
    bool_values = SafeConstructor.bool_values
    inf_value = SafeConstructor.inf_value
    nan_value = SafeConstructor.nan_value
    timestamp_regexp = SafeConstructor.timestamp_regexp
    
    def construct_yaml_str(self, node):
        u"""Construct YAML unicode string as unicode."""
        return self.construct_scalar(node)
    
    # NB: methods defined after this point are identical to those of SafeConstructor.
    
    def construct_scalar(self, node):
        if isinstance(node, MappingNode):
            for key_node, value_node in node.value:
                if key_node.tag == u'tag:yaml.org,2002:value':
                    return self.construct_scalar(value_node)
        return BaseConstructor.construct_scalar(self, node)
    
    def flatten_mapping(self, node):
        merge = []
        index = 0
        while index < len(node.value):
            key_node, value_node = node.value[index]
            if key_node.tag == u'tag:yaml.org,2002:merge':
                del node.value[index]
                if isinstance(value_node, MappingNode):
                    self.flatten_mapping(value_node)
                    merge.extend(value_node.value)
                elif isinstance(value_node, SequenceNode):
                    submerge = []
                    for subnode in value_node.value:
                        if not isinstance(subnode, MappingNode):
                            raise ConstructorError("while constructing a mapping",
                                    node.start_mark,
                                    "expected a mapping for merging, but found %s"
                                    % subnode.id, subnode.start_mark)
                        self.flatten_mapping(subnode)
                        submerge.append(subnode.value)
                    submerge.reverse()
                    for value in submerge:
                        merge.extend(value)
                else:
                    raise ConstructorError("while constructing a mapping", node.start_mark,
                            "expected a mapping or list of mappings for merging, but found %s"
                            % value_node.id, value_node.start_mark)
            elif key_node.tag == u'tag:yaml.org,2002:value':
                key_node.tag = u'tag:yaml.org,2002:str'
                index += 1
            else:
                index += 1
        if merge:
            node.value = merge + node.value
    
    def construct_mapping(self, node, deep=False):
        if isinstance(node, MappingNode):
            self.flatten_mapping(node)
        return BaseConstructor.construct_mapping(self, node, deep=deep)
    
    def construct_yaml_null(self, node):
        self.construct_scalar(node)
        return None
    
    def construct_yaml_bool(self, node):
        value = self.construct_scalar(node)
        return self.bool_values[value.lower()]
    
    def construct_yaml_int(self, node):
        value = str(self.construct_scalar(node))
        value = value.replace('_', '')
        sign = +1
        if value[0] == '-':
            sign = -1
        if value[0] in '+-':
            value = value[1:]
        if value == '0':
            return 0
        elif value.startswith('0b'):
            return sign*int(value[2:], 2)
        elif value.startswith('0x'):
            return sign*int(value[2:], 16)
        elif value[0] == '0':
            return sign*int(value, 8)
        elif ':' in value:
            digits = [int(part) for part in value.split(':')]
            digits.reverse()
            base = 1
            value = 0
            for digit in digits:
                value += digit*base
                base *= 60
            return sign*value
        else:
            return sign*int(value)
    
    def construct_yaml_float(self, node):
        value = str(self.construct_scalar(node))
        value = value.replace('_', '').lower()
        sign = +1
        if value[0] == '-':
            sign = -1
        if value[0] in '+-':
            value = value[1:]
        if value == '.inf':
            return sign*self.inf_value
        elif value == '.nan':
            return self.nan_value
        elif ':' in value:
            digits = [float(part) for part in value.split(':')]
            digits.reverse()
            base = 1
            value = 0.0
            for digit in digits:
                value += digit*base
                base *= 60
            return sign*value
        else:
            return sign*float(value)
    
    def construct_yaml_timestamp(self, node):
        value = self.construct_scalar(node)
        match = self.timestamp_regexp.match(node.value)
        values = match.groupdict()
        year = int(values['year'])
        month = int(values['month'])
        day = int(values['day'])
        if not values['hour']:
            return datetime.date(year, month, day)
        hour = int(values['hour'])
        minute = int(values['minute'])
        second = int(values['second'])
        fraction = 0
        if values['fraction']:
            fraction = values['fraction'][:6]
            while len(fraction) < 6:
                fraction += '0'
            fraction = int(fraction)
        delta = None
        if values['tz_sign']:
            tz_hour = int(values['tz_hour'])
            tz_minute = int(values['tz_minute'] or 0)
            delta = datetime.timedelta(hours=tz_hour, minutes=tz_minute)
            if values['tz_sign'] == '-':
                delta = -delta
        data = datetime.datetime(year, month, day, hour, minute, second, fraction)
        if delta:
            data -= delta
        return data
    
    def construct_yaml_seq(self, node):
        data = []
        yield data
        data.extend(self.construct_sequence(node))
    
    def construct_yaml_map(self, node):
        data = {}
        yield data
        value = self.construct_mapping(node)
        data.update(value)
    
    def construct_yaml_object(self, node, cls):
        data = cls.__new__(cls)
        yield data
        if hasattr(data, '__setstate__'):
            state = self.construct_mapping(node, deep=True)
            data.__setstate__(state)
        else:
            state = self.construct_mapping(node)
            data.__dict__.update(state)
    
    def construct_undefined(self, node):
        raise ConstructorError(None, None,
                "could not determine a constructor for the tag %r" % node.tag.encode('utf-8'),
                node.start_mark)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:null',
        UniConstructor.construct_yaml_null)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:bool',
        UniConstructor.construct_yaml_bool)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:int',
        UniConstructor.construct_yaml_int)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:float',
        UniConstructor.construct_yaml_float)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:timestamp',
        UniConstructor.construct_yaml_timestamp)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:str',
        UniConstructor.construct_yaml_str)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:seq',
        UniConstructor.construct_yaml_seq)

UniConstructor.add_constructor(
        u'tag:yaml.org,2002:map',
        UniConstructor.construct_yaml_map)

UniConstructor.add_constructor(None,
        UniConstructor.construct_undefined)

class UniRepresenter(BaseRepresenter):
    
    # Set class data members directly from SafeRepresenter.
    inf_value = SafeRepresenter.inf_value
    
    # NB: methods defined after this point are identical to those of SafeRepresenter.
    
    def ignore_aliases(self, data):
        if data in [None, ()]:
            return True
        if isinstance(data, (str, unicode, bool, int, float)):
            return True
    
    def represent_none(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:null',
                u'null')
    
    def represent_str(self, data):
        tag = None
        style = None
        try:
            data = unicode(data, 'ascii')
            tag = u'tag:yaml.org,2002:str'
        except UnicodeDecodeError:
            try:
                data = unicode(data, 'utf-8')
                tag = u'tag:yaml.org,2002:str'
            except UnicodeDecodeError:
                data = data.encode('base64')
                tag = u'tag:yaml.org,2002:binary'
                style = '|'
        return self.represent_scalar(tag, data, style=style)
    
    def represent_unicode(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:str', data)
    
    def represent_bool(self, data):
        if data:
            value = u'true'
        else:
            value = u'false'
        return self.represent_scalar(u'tag:yaml.org,2002:bool', value)
    
    def represent_int(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:int', unicode(data))
    
    def represent_long(self, data):
        return self.represent_scalar(u'tag:yaml.org,2002:int', unicode(data))
    
    def represent_float(self, data):
        if data != data or (data == 0.0 and data == 1.0):
            value = u'.nan'
        elif data == self.inf_value:
            value = u'.inf'
        elif data == -self.inf_value:
            value = u'-.inf'
        else:
            value = unicode(repr(data)).lower()
            if u'.' not in value and u'e' in value:
                value = value.replace(u'e', u'.0e', 1)
        return self.represent_scalar(u'tag:yaml.org,2002:float', value)
    
    def represent_list(self, data):
            return self.represent_sequence(u'tag:yaml.org,2002:seq', data)
    
    def represent_dict(self, data):
        return self.represent_mapping(u'tag:yaml.org,2002:map', data)
    
    def represent_date(self, data):
        value = unicode(data.isoformat())
        return self.represent_scalar(u'tag:yaml.org,2002:timestamp', value)
    
    def represent_datetime(self, data):
        value = unicode(data.isoformat(' '))
        return self.represent_scalar(u'tag:yaml.org,2002:timestamp', value)
    
    def represent_yaml_object(self, tag, data, cls, flow_style=None):
        if hasattr(data, '__getstate__'):
            state = data.__getstate__()
        else:
            state = data.__dict__.copy()
        return self.represent_mapping(tag, state, flow_style=flow_style)
    
    def represent_undefined(self, data):
        raise RepresenterError("cannot represent an object: %s" % data)

UniRepresenter.add_representer(type(None),
        UniRepresenter.represent_none)

UniRepresenter.add_representer(str,
        UniRepresenter.represent_str)

UniRepresenter.add_representer(unicode,
        UniRepresenter.represent_unicode)

UniRepresenter.add_representer(bool,
        UniRepresenter.represent_bool)

UniRepresenter.add_representer(int,
        UniRepresenter.represent_int)

UniRepresenter.add_representer(long,
        UniRepresenter.represent_long)

UniRepresenter.add_representer(float,
        UniRepresenter.represent_float)

UniRepresenter.add_representer(list,
        UniRepresenter.represent_list)

UniRepresenter.add_representer(dict,
        UniRepresenter.represent_dict)

UniRepresenter.add_representer(datetime.date,
        UniRepresenter.represent_date)

UniRepresenter.add_representer(datetime.datetime,
        UniRepresenter.represent_datetime)

UniRepresenter.add_representer(None,
        UniRepresenter.represent_undefined)

class UniDumper(Emitter, Serializer, UniRepresenter, Resolver):
    
    def __init__(self, stream,
            default_style=None, default_flow_style=None,
            canonical=None, indent=None, width=None,
            allow_unicode=None, line_break=None,
            encoding=None, explicit_start=None, explicit_end=None,
            version=None, tags=None):
        Emitter.__init__(self, stream, canonical=canonical,
                indent=indent, width=width,
                allow_unicode=allow_unicode, line_break=line_break)
        Serializer.__init__(self, encoding=encoding,
                explicit_start=explicit_start, explicit_end=explicit_end,
                version=version, tags=tags)
        UniRepresenter.__init__(self, default_style=default_style,
                default_flow_style=default_flow_style)
        Resolver.__init__(self)

class UniLoader(Reader, Scanner, Parser, Composer, UniConstructor, Resolver):
    
    def __init__(self, stream):
        Reader.__init__(self, stream)
        Scanner.__init__(self)
        Parser.__init__(self)
        Composer.__init__(self)
        UniConstructor.__init__(self)
        Resolver.__init__(self)

################################################################################

def _init_scalar_representer_info():
    u"""Init YAML scalar representer method info."""
    
    representers = dict()
    
    for t in UniDumper.yaml_representers:
        if t in _info[u'yaml_scalar_types']:
            representers[t] = UniDumper.yaml_representers[t]
    
    return representers

def _init_scalar_resolver_info():
    """Init YAML scalar resolver method info."""
    
    resolvers = dict()
    
    for prefix in UniLoader.yaml_implicit_resolvers:
        for tag, regexp in UniLoader.yaml_implicit_resolvers[prefix]:
            if tag in _info[u'yaml_scalar_tags']:
                resolvers.setdefault(prefix, OrderedDict())
                resolvers[prefix][tag] = regexp
    
    # Ensure empty string is resolved as None.
    resolvers[u''] = OrderedDict( UniLoader.yaml_implicit_resolvers[u'~'] )
    
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
_scalar_constructor = UniConstructor()
_scalar_representer = UniRepresenter()
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
    
    fixed_kwargs = {
        'Dumper': UniDumper,
        'allow_unicode': True,
        'encoding': None
    }
    
    for k, x in fixed_kwargs.items():
        if k in kwds:
            raise RuntimeError("cannot set reserved keyword argument {!r}".format(k))
        kwds[k] = x
    
    return dump(data, stream=stream, **kwds)

def uniload(stream):
    u"""Load data from YAML unicode stream."""
    return load(stream, Loader=UniLoader)

def unidump_scalar(data, stream=None):
    u"""Dump scalar to YAML unicode stream."""
    
    if data is not None:
        
        try:
            method = _scalar_representer_methods[type(data)]
        except KeyError:
            raise TypeError("cannot dump data of type {!r}".format(type(data).__name__))
        
        if is_multiline_string(data):
            raise ValueError("cannot dump multiline string ~ {!r}".format(data))
        
        node = method(_scalar_representer, data)
        value = node.value
        
    else: # Represent None as empty string.
        value = u''
    
    if stream is not None:
        stream.write(u'{}{}'.format(value.rstrip(u'\n'), u'\n'))
    else:
        return value

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
