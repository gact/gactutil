#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
"""GACTutil command-line interface."""

from argparse import ArgumentError
from argparse import ArgumentParser
from argparse import FileType
from argparse import RawDescriptionHelpFormatter
from argparse import REMAINDER
from collections import deque
from collections import namedtuple
from collections import OrderedDict
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
from pkg_resources import resource_filename
import re
import sys
from textwrap import dedent
from types import IntType
from types import NoneType
from yaml import dump
from yaml import load
from yaml import safe_dump
from yaml import safe_load
from yaml import YAMLError

from gactutil import _read_about
from gactutil import _tokenise_source
from gactutil import TextReader
from gactutil import TextWriter

################################################################################

# Named tuple for specification of gactfunc info.
_GactfuncSpec = namedtuple('_GactfuncSpec', ['module', 'function'])

# Named tuple for specification of gactfunc parameter/return types.
_GFTS = namedtuple('GFTS', [
    'name',           # Name of gactfunc data type.
    'is_compound',    # Composite data type.
    'is_delimitable', # Convertible to a delimited string, and vice versa.
    'is_ductile',     # Convertible to a single-line string, and vice versa.
    'match'           # Function to match object to the given type spec.
])

# Supported gactfunc parameter/return types. These must be suitable for use both
# as Python function arguments and as command-line arguments, whether loaded
# from a file or converted from a simple string.
_gtypes = OrderedDict([
  #                          NAME   COMP  DELIM   DUCT  MATCH
  ('NoneType',  _GFTS( 'NoneType', False,  True,  True, lambda x: isinstance(x, NoneType))),
  ('bool',      _GFTS(     'bool', False,  True,  True, lambda x: isinstance(x, bool))),
  ('float',     _GFTS(    'float', False,  True,  True, lambda x: isinstance(x, float))),
  ('int',       _GFTS(      'int', False,  True,  True, lambda x: isinstance(x, IntType))),
  ('string',    _GFTS(   'string', False,  True,  True, lambda x: isinstance(x, basestring))),
  ('dict',      _GFTS(     'dict',  True,  True,  True, lambda x: isinstance(x, dict))),
  ('list',      _GFTS(     'list',  True,  True,  True, lambda x: isinstance(x, list))),
  ('DataFrame', _GFTS('DataFrame',  True, False, False, lambda x: isinstance(x, DataFrame)))
])

_info = {
    
    # True values from PyYAML-3.11 <http://pyyaml.org/browser/pyyaml> [Accessed: 5 Apr 2016].
    'true_values': ('yes', 'Yes', 'YES', 'true', 'True', 'TRUE', 'on', 'On', 'ON'),
    
    # False values from PyYAML-3.11 <http://pyyaml.org/browser/pyyaml> [Accessed: 5 Apr 2016].
    'false_values': ('no', 'No', 'NO', 'false', 'False', 'FALSE', 'off', 'Off', 'OFF'),
    
    # Null values from PyYAML-3.11 <http://pyyaml.org/browser/pyyaml> [Accessed: 5 Apr 2016].
    'na_values': ('null', 'Null', 'NULL'),
    
    # Input/output patterns.
    'iop': {
        
        # Input parameter patterns.
        'input': {
            
            'single': {
                'regex': re.compile('^infile$'),
                'metavar': 'FILE',
                'flag': '-i'
            },
            
            'listed': {
                'regex': re.compile('^infiles$'),
                'metavar': 'FILES',
                'flag': '-i'
            },
            
            'indexed': {
                'regex': re.compile('^infile(?P<index>[1-9]+|U)$'),
                'metavar': 'FILE\g<index>',
                'flag': '-\g<index>'
            },
            
            'directory': {
                'regex': re.compile('^indir$'),
                'metavar': 'DIR',
                'flag': '-i'
            },
            
            'prefix': {
                'regex': re.compile('^inprefix$'),
                'metavar': 'PREFIX',
                'flag': '-i'
            }
        },
        
        # Output parameter patterns.
        'output': {
            'single': {
                'regex': re.compile('^outfile$'),
                'metavar': 'FILE',
                'flag': '-o'
                
            },
            
            'listed': {
                'regex': re.compile('^outfiles$'),
                'metavar': 'FILES',
                'flag': '-o'
            },
            
            'indexed': {
                'regex': re.compile('^outfile(?P<index>[1-9]+|U)$'),
                'metavar': 'FILE\g<index>',
                'flag': '--\g<index>'
            },
            
            'directory': {
                'regex': re.compile('^outdir$'),
                'metavar': 'DIR',
                'flag': '-o'
            },
            
            'prefix': {
                'regex': re.compile('^outprefix$'),
                'metavar': 'PREFIX',
                'flag': '-o'
            },
            
            'returned': {
                'regex': None,
                'metavar': 'FILE',
                'flag': '-o'
            }
        }
    },
    
    # Short-form parameters: mappings of Python function parameters to
    # short-form command-line flags. These make it possible for common
    # parameters to take a short form on the command line. If a gactfunc
    # uses a short-form parameter, this is automatically converted to
    # the corresponding flag by '_setup_commands'.
    'short_params': {
        
        'threads': {
            'default': 1,
            'flag': '-t',
            'required': False,
            'type': 'int'
        }
    },
    
    # Gactfunc docstring headers.
    'docstring_headers': {
        
        'known': ('Args', 'Arguments', 'Attributes', 'Example', 'Examples',
                  'Keyword Args', 'Keyword Arguments', 'Methods', 'Note',
                  'Notes', 'Other Parameters', 'Parameters', 'Return',
                  'Returns', 'Raises', 'References', 'See Also', 'Warning',
                  'Warnings', 'Warns', 'Yield', 'Yields'),
            
        'supported': ('Args', 'Arguments', 'Note', 'Notes', 'Parameters',
                      'Return', 'Returns', 'References', 'See Also'),
        
        'alias_mapping': { 'Arguments': 'Args', 'Parameters': 'Args',
                           'Return': 'Returns' }
    },
    
    'regex': {
        'gactfunc': re.compile('^(?:[A-Z0-9]+)(?:_(?:[A-Z0-9]+))*$', re.IGNORECASE),
        'docstring_header': re.compile('^(\w+):\s*$'),
        'docstring_param': re.compile('^([*]{0,2}\w+)\s*(?:\((\w+)\))?:\s+(.+)$'),
        'docstring_return': re.compile('^(?:(\w+):\s+)?(.+)$'),
        'docstring_default': re.compile('[[(]default:\s+(.+?)\s*[])]', re.IGNORECASE)
    }
}

################################################################################

class _Gactfunc(object):
    """A gactfunc wrapper class."""
    
    @staticmethod
    def _parse_function_docstring(function):
        """Parse gactfunc docstring.
        
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
        if docstring is not None and docstring.strip() != '':
            
            # Init raw docstring.
            raw_info = OrderedDict()
            
            # Split docstring into lines.
            lines = deque( docstring.split('\n'))
            
            # Set summary from first non-blank line.
            line = lines.popleft().strip()
            if line == '':
                line = lines.popleft().strip()
                if line == '':
                    raise ValueError("{} docstring summary is a blank line".format(func_name))
            raw_info['Summary'] = [line]
            
            # Check summary followed by a blank line.
            if len(lines) > 0:
                line = lines.popleft().strip()
                if line != '':
                    raise ValueError("{} docstring summary is not followed by a blank line".format(func_name))
            
            # Get list of remaining lines, with common indentation removed.
            lines = deque( (dedent( '\n'.join(lines) ) ).split('\n') )
            
            # Init docstring description.
            raw_info['Description'] = list()
            
            # Docstring description includes everything before the first header.
            h = 'Description'
            
            # Group content by docstring section.
            while len(lines) > 0:
                
                # Get first of remaining lines.
                line = lines.popleft()
                
                # Try to match line to a docstring header.
                m = _info['regex']['docstring_header'].match(line)
                
                # If matches, set header of new section..
                if m is not None:
                    
                    # Set current header.
                    h = m.group(1)
                    
                    # Map header to alias, if relevant.
                    if h in _info['docstring_headers']['alias_mapping']:
                        h = _info['docstring_headers']['alias_mapping'][h]
                    
                    # Check header is known.
                    if h not in _info['docstring_headers']['known']:
                        raise ValueError("unknown docstring header ~ {!r}".format(h))
                    
                    # Check header is supported.
                    if h not in _info['docstring_headers']['supported']:
                        raise ValueError("unsupported docstring header ~ {!r}".format(h))
                    
                    # Check for duplicate headers.
                    if h in raw_info:
                        raise ValueError("duplicate docstring header ~ {!r}".format(h))
                    
                    raw_info[h] = list()
                    
                # ..otherwise append line to current section.
                else:
                    raw_info[h].append(line)
            
            # Remove docstring description, if empty.
            if len(raw_info['Description']) == 0:
                del raw_info['Description']
            
            # Init parsed docstring.
            doc_info = OrderedDict()
            
            # Process each docstring section.
            for h in raw_info:
                
                # Get docstring section as unindented lines.
                raw_info[h] = ( dedent( '\n'.join(raw_info[h]) ) ).split('\n')
                
                if h == 'Args':
                    
                    # Init parsed parameter info.
                    param_info = OrderedDict()
                    
                    param_name = None
                    
                    # Group content by parameter.
                    for line in raw_info[h]:
                        
                        line = line.strip()
                        
                        # Skip blank lines.
                        if line != '':
                            
                            # Try to match line to expected pattern of parameter.
                            m = _info['regex']['docstring_param'].match(line)
                        
                            # If this is a parameter definition line, get parameter info..
                            if m is not None:
                                
                                param_name, type_name, param_desc = m.groups()
                                
                                # Check parameter does not denote unenumerated arguments.
                                if param_name.startswith('*'):
                                    raise RuntimeError("{} docstring must not specify unenumerated arguments".format(
                                        func_name))
                                
                                # Check parameter type specified.
                                if type_name is None:
                                    raise ValueError("{} docstring must specify a type for parameter {!r}".format(
                                        func_name, param_name))
                                
                                # Check type name is not 'None'.
                                if type_name == 'None':
                                    raise ValueError("{} docstring specifies 'NoneType' for parameter {!r}".format(
                                        func_name, param_name))
                                
                                # Check parameter type is supported.
                                if not _gtypes[type_name].is_ductile:
                                    raise ValueError("{} docstring specifies unsupported type {!r} for parameter {!r}".format(
                                        func_name, type_name, param_name))
                                
                                # Check for duplicate parameters.
                                if param_name in param_info:
                                    raise ValueError("{} docstring contains duplicate parameter ~ {!r}".format(
                                        func_name, param_name))
                                
                                param_info[param_name] = {
                                    'type': type_name,
                                    'description': param_desc
                                }
                            
                            # ..otherwise if parameter defined, treat this as
                            # a continuation of the parameter description..
                            elif param_name is not None:
                                
                                param_info[param_name]['description'] = '{} {}'.format(
                                    param_info[param_name]['description'], line)
                                    
                            # ..otherwise this is not a valid docstring parameter.
                            else:
                                raise ValueError("failed to parse docstring for function ~ {!r}".format(
                                    func_name))
                    
                    # Validate docstring default info.
                    for param_name in param_info:
                        
                        # Try to match default definition pattern in parameter description.
                        defaults = _info['regex']['docstring_default'].findall(
                            param_info[param_name]['description'])
                        
                        # If a default definition matched, keep
                        # string representation of default value..
                        if len(defaults) == 1:
                            param_info[param_name]['docstring_default'] = defaults[0]
                        # ..otherwise the description has ambiguous default info.
                        elif len(defaults) > 1:
                            raise ValueError("{} docstring has multiple defaults for parameter {!r}".format(
                                func_name, param_name))
                    
                    # Set parsed parameter info for docstring.
                    doc_info[h] = param_info
                    
                elif h == 'Returns':
                    
                    type_name = None
                    description = list()
                    
                    # Process each line of return value section.
                    for line in raw_info[h]:
                        
                        line = line.strip()
                        
                        # Skip blank lines.
                        if line != '':
                            
                            # Try to match line to expected pattern of return value.
                            m = _info['regex']['docstring_return'].match(line)
                                
                            # If return value type info is present,
                            # get type info and initial description..
                            if m is not None:
                                    
                                type_name = m.group(1)
                                description.append( m.group(2) )
                                    
                                # Check parameter type specified.
                                if type_name is None:
                                    raise ValueError("{} docstring must specify a type for return value".format(
                                        func_name))
                                    
                                # Check type name is not 'None'.
                                if type_name == 'None':
                                    raise ValueError("{} docstring specifies 'None' for return value".format(
                                        func_name))
                                    
                                # Check return value type is supported.
                                if type_name not in _gtypes:
                                    raise ValueError("{} docstring specifies unsupported type {!r} for return value".format(
                                        func_name, type_name ))
                                
                            # ..otherwise if return value type already
                            # identified, append line to description..
                            elif type_name is not None:
                                
                                description.append(line)
                                
                            # ..otherwise this is not a valid docstring return value.
                            else:
                                raise ValueError("failed to parse docstring for function ~ {!r}".format(
                                    func_name))
                    
                    # Set parsed return value info for docstring.
                    doc_info[h] = {
                        'type': type_name,
                        'description': ' '.join(description)
                    }
                    
                else:
                    
                    # Strip leading/trailing blank lines.
                    lines = raw_info[h]
                    for i in (0, -1):
                        while len(lines) > 0 and lines[i].strip() == '':
                            lines.pop(i)
                    doc_info[h] = '\n'.join(lines)
           
        return doc_info
    
    @staticmethod
    def _parse_function_name(function):
        """Parse gactfunc name."""
        
        # Get function name.
        func_name = function.__name__
        
        # Check gactfunc is indeed a function.
        if not isfunction(function):
            return TypeError("object is not a function ~ {!r}".format(func_name))
        
        # Try to match function name to expected gactfunc pattern.
        m = _info['regex']['gactfunc'].match(func_name)
        
        try: # Split gactfunc name into commands.
            assert m is not None
            commands = tuple( func_name.split('_') )
            assert len(commands) >= 2
            assert len(set(commands)) == len(commands)
        except AssertionError:
            raise ValueError("function {!r} does not follow gactfunc naming convention".format(func_name))
        
        return commands
    
    @staticmethod
    def _validate_param_type(x, type_name=None):
        """Validate parameter object type."""
        
        t = _get_type_name(x)
        
        if type_name is not None and t != type_name:
            raise TypeError("parameter type ({}) differs from that expected ({})".format(
                t, type_name))
        
        if t == 'string':
            
            _validate_ductile(x)
            
        elif t == 'dict':
            
            for key, value in x.items():
                _validate_ductile(key)
                _validate_ductile(value)
            
        elif t == 'list':
            
            for element in x:
                _validate_ductile(element)
            
        elif not _gtypes[t].is_ductile:
            raise TypeError("{} is not a valid parameter object ~ {!r}".format(t, x))
    
    @staticmethod
    def _validate_return_type(x, type_name=None):
        """Validate return value type."""
        
        t = _get_type_name(x)
        
        if type_name is not None and t != type_name:
            raise TypeError("return value type ({}) differs from that expected ({})".format(
                t, type_name))
        
        if t == 'string':
            
            _validate_ductile(x)
            
        elif t == 'dict':
            
            for key, value in x.items():
                _validate_ductile(key)
                _validate_ductile(value)
            
        elif t == 'list':
            
            try:
                for element in x:
                    _validate_ductile(element)
            except (TypeError, ValueError):
                for element in x:
                    _validate_delimitable(element)
        
        elif t not in _gtypes:
            raise TypeError("{} is not a valid return value object ~ {!r}".format(t, x))
    
    def __init__(self, function):
        """Init gactfunc wrapper from wrapped function."""
        
        # Get function name.
        func_name = function.__name__
        
        # Init argparser spec.
        self.ap_spec = OrderedDict()
        
        # Get commands from function name.
        self.commands = self._parse_function_name(function)
        
        # Get function parameter spec.
        param_spec = getargspec(function)
        
        # Check that there are no unenumerated arguments.
        if param_spec.varargs is not None or param_spec.keywords is not None:
            raise ValueError("{} cannot have unenumerated arguments".format(
                self.__class__.__name__))
        
        # Get enumerated parameter names.
        param_names = param_spec.args
        
        # Map formal keyword parameters to their defaults.
        if param_spec.defaults is not None:
            i = len(param_spec.defaults)
            spec_def_info = { k: x for k, x in
                zip(param_names[-i:], param_spec.defaults) }
        else:
            spec_def_info = None
        
        # Set docstring info from gactfunc docstring.
        doc_info = self._parse_function_docstring(function)
        
        # Check that gactfunc has been documented.
        if doc_info is None:
            raise ValueError("{} {!r} is not documented".format(
                self.__class__.__name__, func_name))
        
        self.ap_spec['summary'] = doc_info['Summary']
        
        if 'Description' in doc_info:
            self.ap_spec['description'] = doc_info['Description']
        
        # Init flag set to check for conflicting option strings.
        flag2param = dict()
        
        # If parameters documented, validate them..
        if 'Args' in doc_info:
            
            # Set gactfunc parameter info from parsed docstring.
            self.ap_spec['params'] = doc_info['Args']
            
            # Get set of documented parameters.
            doc_param_set = set(self.ap_spec['params'])
            
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
                    
                    self.ap_spec['params'][param_name]['default'] = default
                    
                    # Skip unspecified defaults as we cannot validate them.
                    if default is None:
                        continue
                        
                    # Get parameter type.
                    type_name = self.ap_spec['params'][param_name]['type']
                    
                    # Check that the defined default value is of the
                    # type specified in the function documentation.
                    try:
                        _Gactfunc._validate_param_type(default, type_name)
                    except (TypeError, ValueError):
                        raise TypeError("{} definition has default type mismatch for parameter {!r}".format(
                            func_name, param_name))
                    
                    # Skip undocumented defaults.
                    if 'docstring_default' not in self.ap_spec['params'][param_name]:
                        continue
                        
                    # Get string representation of docstring default.
                    docstring_default = self.ap_spec['params'][param_name]['docstring_default']
                    
                    try: # Coerce documented default from string.
                        coerced_default = _object_from_string(docstring_default, type_name)
                    except (TypeError, ValueError):
                        raise TypeError("{} docstring has default type mismatch for parameter {!r}".format(
                            func_name, param_name))
                        
                    # Check that documented default matches actual default.
                    if coerced_default != default:
                        raise ValueError("{} has default value mismatch for parameter {!r}".format(
                            func_name, param_name))
            
            # Init input/output parameter set info.
            self.ap_spec['iop'] = { k: None for k in _info['iop'] }
            iop_info = { k: {} for k in _info['iop'] }
            param2channel = dict()
            param2iop = dict()
            
            # Get info on gactfunc input/output (IO) patterns.
            for channel in _info['iop']:
                
                # Check for each IO pattern, store info on matching pattern.
                for iop in _info['iop'][channel]:
                    
                    # Get info on this IO pattern.
                    regex, metavar, flag = [ _info['iop'][channel][iop][k]
                        for k in ('regex', 'metavar', 'flag') ]
                    
                    # If return-value IO pattern, check function for explicit return..
                    if iop == 'returned':
                        
                        # Set special parameter name for return value.
                        param_name = 'retfile'
                        
                        # Check if function contains explicit return.
                        explicit_return = any( token == 'return' for token in
                            reversed( _tokenise_source( getsource(function) ) ) )
                        
                        # If no explicit return in gactfunc source, check return
                        # value not documented, then skip to next parameter set.
                        if not explicit_return:
                            if 'Returns' in doc_info:
                                raise ValueError("{} return value documented but not defined".format(
                                    func_name))
                            continue
                    
                    # ..otherwise check parameter names.
                    else:
                        
                        # Try to match parameter names to those expected for this parameter set.
                        matches = [ regex.match(param_name) for param_name in param_names ]
                        
                        # Get mapping of params to matches for this parameter set.
                        param2match = { p: m for p, m in zip(param_names, matches)
                            if m is not None }
                        
                        # If no parameters matched, skip to next parameter set.
                        if len(param2match) == 0:
                            continue
                        
                    # Whether function parameter or return value, store
                    # matching parameter set, checking for any conflicts.
                    if self.ap_spec['iop'][channel] is not None:
                        raise ValueError("{} has conflicting {} parameter sets ~ {!r}".format(
                            func_name, channel, (self.ap_spec['iop'][channel], iop)))
                    self.ap_spec['iop'][channel] = iop
                    
                    if iop == 'returned':
                        
                        # Check return value documented.
                        if not 'Returns' in doc_info:
                            raise ValueError("{} return value defined but not documented".format(
                                func_name))
                        
                        # Check for conflicting function parameter.
                        if param_name in self.ap_spec['params']:
                            raise ValueError("{} return value command-line parameter conflicts with function parameter {!r}".format(
                                func_name, param_name))
                        
                        # Store parameter info for return value parameter.
                        iop_info[channel][param_name] = {
                            'metavar': metavar,
                            'flag': flag
                        }
                        
                        # Update parameter info with special return value option.
                        self.ap_spec['params'][param_name] = {
                            'default': '-',
                            'description': doc_info['Returns']['description'],
                            'type': doc_info['Returns']['type']
                        }
                        
                    else:
                        
                        # Store parameter info for each parameter in this set.
                        for param_name in param2match:
                            
                            iop_info[channel][param_name] = {
                                'metavar': regex.sub(metavar, param_name),
                                'flag': regex.sub(flag, param_name)
                            }
                            
                            # If these are indexed input/output files, get
                            # parameter index, as an integer if possible.
                            if iop == 'indexed':
                                i = param2match[param_name].group('index')
                                try:
                                    iop_info[channel][param_name]['index'] = int(i)
                                except ValueError:
                                    iop_info[channel][param_name]['index'] = i
                            
                            # Check parameter type is as expected.
                            type_name = self.ap_spec['params'][param_name]['type']
                            if type_name != 'string':
                                raise TypeError("{} {} parameter must be of type string, not {} ~ {!r}".format(
                                    func_name, channel, type_name, param_name))
                        
                        if iop == 'indexed':
                            
                            # Check indexed parameters are as expected:
                            # * numbered indices start at 1, increment by 1
                            # * unindexed parameter not present without indexed parameters
                            indices = [ iop_info[channel][p]['index'] for p in iop_info[channel] ]
                            numbers = sorted( i for i in indices if i != 'U' )
                            if numbers[0] != 1 or any( j - i != 1
                                for i, j in zip(numbers[:-1], numbers[1:]) ):
                                raise ValueError("sparse indices in {} parameters of {}".format(
                                    channel, func_name))
                            if 'U' in indices and len(indices) == 1:
                                raise ValueError("{} defines unindexed {1} parameter but not indexed {1} parameters".format(
                                    func_name, channel))
                            
                            # Check required indexed parameters are as expected:
                            # * numbered indices start at 1, increment by 1
                            # * unindexed parameter not present without indexed parameters
                            indices = [ iop_info[channel][p]['index'] for p in iop_info[channel]
                                if 'default' not in self.ap_spec['params'][p] ]
                            numbers = sorted( i for i in indices if i != 'U' )
                            if numbers[0] != 1 or any( j - i != 1
                                for i, j in zip(numbers[:-1], numbers[1:]) ):
                                raise ValueError("sparse indices in required {} parameters of {}".format(
                                    channel, func_name))
                            if 'U' in indices and len(indices) == 1:
                                raise ValueError("{} requires unindexed {1} parameter but not indexed {1} parameters".format(
                                    func_name, channel))
                    
                    # Map parameter to IO channel and IO pattern.
                    for param_name in iop_info[channel]:
                        param2channel[param_name] = channel
                        param2iop[param_name] = iop
                    
            # Prepare parameters for argument parser.
            for param_name in self.ap_spec['params']:
                
                # Get info for this parameter.
                param_info = self.ap_spec['params'][param_name]
                
                # Set parameter name to be used in argument parser.
                param_info['dest'] = param_name
                
                # If parameter has a default value, set as option or switch..
                if 'default' in param_info:
                    
                    param_info['required'] = False
                    
                    # If default value is False, assign to switches..
                    if param_info['type'] == 'bool' and param_info['default'] is False:
                        param_info['group'] = 'switch'
                    # ..otherwise assign to optionals.
                    else:
                        param_info['group'] = 'optional'
                    
                # ..otherwise, assign to positional parameters.
                else:
                    param_info['group'] = 'positional'
                
                # If this for input/output, change to IO parameter..
                if param_name in param2channel:
                    
                    channel = param2channel[param_name]
                    
                    # Set parameter metavariable and flag.
                    param_info['metavar'] = iop_info[channel][param_name]['metavar']
                    param_info['flag'] = iop_info[channel][param_name]['flag']
                    
                    # Input/output parameters are treated as optionals. If
                    # parameter was positional, set default value, using
                    # standard input or output where appropriate.
                    if param_info['group'] == 'positional':
                        
                        iop = param2iop[param_name]
                        
                        if iop == 'indexed' and iop_info[channel][param_name]['index'] == 1:
                            param_info['required'] = False
                            param_info['default'] = '-'
                        elif iop == 'listed':
                            param_info['required'] = False
                            param_info['default'] = ['-']
                        elif iop == 'single':
                            param_info['required'] = False
                            param_info['default'] = '-'
                        else:
                            param_info['required'] = True
                            param_info['default'] = None
                    
                    # Mark as IO parameter.
                    param_info['group'] = 'IO'
                    
                # ..otherwise if parameter has a short form, convert to short form..
                elif param_name in _info['short_params']:
                    
                    # Check that this is not a compound type.
                    if _gtypes[ param_info['type'] ].is_compound:
                        raise TypeError("cannot create short-form parameter {!r} of type {}".format(
                            param_name, param_info['type']))
                    
                    # Set flag to short form.
                    param_info['flag'] = _info['short_params'][param_name]['flag']
                    
                    # Check parameter type matches that of short-form.
                    if param_info['type'] != _info['short_params'][param_name]['type']:
                        raise TypeError("{} has type mismatch for short-form parameter {!r}".format(
                            func_name, param_name))
                    
                    # Short form parameters are treated as optionals.
                    # If parameter was positional, set as required.
                    if param_info['group'] == 'positional':
                        param_info['required'] = True
                        param_info['default'] = None
                        
                    try: # Check parameter default matches that of short-form.
                        assert param_info['default'] == _info['short_params'][param_name]['default']
                    except AssertionError:
                        raise ValueError("{} has default value mismatch for short-form parameter {!r}".format(
                            func_name, param_name))
                    except KeyError:
                        pass
                    
                    try: # Check parameter requirement matches that of short-form.
                        assert param_info['required'] == _info['short_params'][param_name]['required']
                    except AssertionError:
                        raise ValueError("{} has requirement mismatch for short-form parameter {!r}".format(
                            func_name, param_name))
                    except KeyError:
                        pass
                    
                    # Mark as short form optional.
                    param_info['group'] = 'short'
                
                # ..otherwise if parameter is of a compound type, create
                # two (mutually exclusive) parameters: one to accept argument
                # as a string, the other to load it from a file..
                elif _gtypes[ param_info['type'] ].is_compound:
                    
                    # Compound parameters are treated as optionals.
                    # If parameter was positional, set as required.
                    if param_info['group'] == 'positional':
                        param_info['required'] = True
                        param_info['default'] = None
                    
                    # Mark as 'compound'.
                    param_info['group'] = 'compound'
                    
                    # Set compound parameter title.
                    param_info['title'] = '{} argument'.format( param_name.replace('_', '-') )
                    
                    # If parameter is of a ductile type, set flag for
                    # it to be passed directly on the command line.
                    if _gtypes[ param_info['type'] ].is_ductile:
                        param_info['flag'] = '--{}'.format( param_name.replace('_', '-') )
                    
                    # Set file parameter name.
                    param_info['file_dest'] = '{}_file'.format(param_name)
                    
                    # Set flag for parameter to be passed as a file.
                    param_info['file_flag'] = file_flag = '--{}-file'.format( param_name.replace('_', '-') )
                    
                    # Check that file option string does
                    # not conflict with existing options.
                    if file_flag in flag2param:
                        raise ValueError("file flag of {} parameter {!r} conflicts with {!r}".format(
                            func_name, param_name, flag2param[file_flag]))
                    flag2param[file_flag] = '{} file flag'.format(param_name)
                    
                # ..otherwise if option or switch,
                # create flag from parameter name.
                elif param_info['group'] in ('optional', 'switch'):
                    
                    if len(param_name) > 1:
                        param_info['flag'] = '--{}'.format( param_name.replace('_', '-') )
                    else:
                        param_info['flag'] = '-{}'.format(param_name)
                 
                # Append info to argument description as appropriate.
                if param_info['group'] != 'positional':
                    if param_info['default'] is not None:
                        if param_info['group'] != 'switch' and not 'docstring_default' in param_info:
                            param_info['description'] = '{} [default: {!r}]'.format(
                                param_info['description'], param_info['default'])
                    elif param_info['required']:
                        param_info['description'] = '{} [required]'.format(
                            param_info['description'])
                
                try: # Delete docstring default - no longer needed.
                    del param_info['docstring_default']
                except KeyError:
                    pass
                
                # Check for conflicting option strings.
                if 'flag' in param_info:
                    flag = param_info['flag']
                    if flag in flag2param:
                        raise ValueError("flag of {} parameter {!r} conflicts with {!r}".format(
                            func_name, param_name, flag2param[flag]))
                    flag2param[flag] = param_name
                
                # Update parameter info.
                self.ap_spec['params'][param_name] = param_info
                
        # ..otherwise, check that no parameters were defined.
        elif len(param_names) > 0:
            raise ValueError("{} parameters defined but not documented ~ {!r}".format(
                func_name, param_names))
        
        self.__name__ = function.__name__
        self.function = function
        
    def __call__(self, *args, **kwargs):
        """Call gactfunc wrapper."""
        return self.function(*args, **kwargs)

class _GactfuncCollection(object):
    """A gactfunc collection class."""
    
    def __init__(self):
        """Init gactfunc collection."""
        self._data = dict()
        
    def dump(self):
        """Dump gactfunc collection info."""
        
        # Ensure data directory exists.
        data_dir = os.path.join('gactutil', 'data')
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)
        
        # Dump gactfunc collection info.
        gaction_file = os.path.join(data_dir, 'gaction.yaml')
        with open(gaction_file, 'w') as fh:
            dump(self._data, fh)
        
    def load(self):
        """Load gactfunc collection info."""
        gaction_file = os.path.join('data', 'gaction.yaml')
        gaction_path = resource_filename('gactutil', gaction_file)
        with open(gaction_path, 'r') as fh:
            self._data = load(fh)
    
    def populate(self):
        """Populate gactfunc collection from GACTutil package modules.
        
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
        
        # With module info, create listing of all _Gactfunc instances (i.e. any
        # functions with the @gactfunc decorator). Collect module and function
        # name for each gactfunc instance. Check for conflicting gactfunc names.
        gactfuncs = list()
        func_spec_info = dict()
        for mod_name, mod_path in mod_info.items():
            module = load_source(mod_name, mod_path)
            for k, x in getmembers(module):
                if isinstance(x, _Gactfunc):
                    func_name = x.__name__
                    if func_name in func_spec_info:
                        raise RuntimeError("conflicting gactfunc name ~ {!r}".format(func_name))
                    func_spec_info[func_name] = _GactfuncSpec(mod_name, func_name)
                    gactfuncs.append(x)
        
        # Populate multi-level dictionary of gactfunc specs, so that
        # these can be accessed by the relevant sequence of commands,
        # while checking for conflicting command sequences.
        for function in gactfuncs:
            try:
                d = self._data
                for cmd in function.commands[:-1]:
                    if cmd in d:
                        assert isinstance(d[cmd], dict)
                    else:
                        d[cmd] = dict()
                    d = d[cmd]
                assert function.commands[-1] not in d
                d[ function.commands[-1] ] = func_spec_info[function.__name__]
            except AssertionError:
                raise RuntimeError("{} has conflicting gactfunc commands ~ {!r}".format(
                    function.__name__, ' '.join(function.commands)))
    
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
        if len(self._data) == 0:
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
                
                # Get function definition.
                module = import_module(func_spec.module)
                function = getattr(module, func_spec.function)
                
                # Set gactfunc summary.
                cap.summary = function.ap_spec['summary']
                
                # Set gactfunc description, if present.
                try:
                    cap.description = '\n\n{}'.format(function.ap_spec['description'])
                except KeyError:
                    pass
                
                # If gactfunc has parameters..
                if 'params' in function.ap_spec:
                    
                    # ..add each parameter to the argument parser.
                    for param_name in function.ap_spec['params']:
                        
                        # Get info for this parameter.
                        param_info = function.ap_spec['params'][param_name]
                        
                        if param_info['group'] == 'positional':
                        
                            cap.add_argument(param_info['dest'],
                                help = param_info['description'])
                                
                        elif param_info['group'] == 'optional':
                        
                            cap.add_argument(param_info['flag'],
                                dest     = param_info['dest'],
                                metavar  = param_info['type'].upper(),
                                default  = param_info['default'],
                                required = param_info['required'],
                                help     = param_info['description'])
                        
                        elif param_info['group'] == 'short':
                            
                            cap.add_argument(param_info['flag'],
                                dest     = param_info['dest'],
                                default  = param_info['default'],
                                required = param_info['required'],
                                help     = param_info['description'])
                            
                        elif param_info['group'] == 'switch':
                        
                            cap.add_argument(param_info['flag'],
                                dest   = param_info['dest'],
                                action = 'store_true',
                                help   = param_info['description'])
                                
                        elif param_info['group'] == 'compound':
                            
                            # If compound object parameter is of a parameter type,
                            # prepare to read from command line or load from file..
                            if _gtypes[ param_info['type'] ].is_ductile:
                            
                                # Set info for pair of alternative parameters.
                                item_help = 'Set {} from string.'.format(param_info['type'])
                                file_help = 'Load {} from file.'.format(param_info['type'])
                                
                                # Add (mutually exclusive) pair of alternative parameters.
                                ag = cap.add_argument_group(
                                    title       = param_info['title'],
                                    description = param_info['description'])
                                mxg = ag.add_mutually_exclusive_group(
                                    required    = param_info['required'])
                                mxg.add_argument(param_info['flag'],
                                    dest        = param_info['dest'],
                                    metavar     = 'STR',
                                    default     = param_info['default'],
                                    help        = item_help)
                                mxg.add_argument(param_info['file_flag'],
                                    dest        = param_info['file_dest'],
                                    metavar     = 'PATH',
                                    help        = file_help)
                            
                            # ..otherwise prepare to load it from file.
                            else:
                                
                                cap.add_argument(param_info['file_flag'],
                                    dest     = param_info['file_dest'],
                                    metavar  = 'PATH',
                                    default  = param_info['default'],
                                    required = param_info['required'],
                                    help     = param_info['description'])
                        
                        elif param_info['group'] == 'IO':
                            
                            cap.add_argument(param_info['flag'],
                                dest     = param_info['dest'],
                                metavar  = param_info['metavar'],
                                default  = param_info['default'],
                                required = param_info['required'],
                                help     = param_info['description'])
                
                # Set function definition for this gactfunc.
                cap.set_defaults(function=function)
        
        return ap
    
    def proc_args(self, args):
        """Process parsed command-line arguments."""
        
        # Pop return-value output file, if present.
        retfile = args.__dict__.pop('retfile', None)
        
        # Pop gactfunc definition.
        try:
            function = args.__dict__.pop('function')
        except KeyError:
            raise RuntimeError("cannot run command - no function available")
        
        # Get parameter info for this gactfunc.
        param_info = function.ap_spec['params']
        
        # Process each argument.
        for param_name in args.__dict__.keys():
            
            # Assume argument is not to be loaded from file.
            filebound = False
            
            # Get expected argument type.
            type_name = param_info[param_name]['type']
            
            # Get argument value.
            try:
                arg = args.__dict__[param_name]
            except KeyError: # Filebound compound type.
                arg = args.__dict__[param_name] = None
            
            # If parameter is in compound group,
            # check both alternative arguments.
            if param_info[param_name]['group'] == 'compound':
                
                # Get file argument value.
                file_arg = args.__dict__[ param_info[param_name]['file_dest'] ]
                
                # If file argument specified, set argument value from file
                # argument, indicate argument value is to be loaded from file..
                if file_arg is not None:
                    arg = file_arg
                    filebound = True
                # ..otherwise check argument specified (if required).
                elif arg is None and param_info[param_name]['required']:
                    raise ArgumentError("{} is required".format(param_info['title']))
                
                # Remove file parameter from parsed arguments.
                del args.__dict__[ param_info[param_name]['file_dest'] ]
            
            # If argument specified, get from file or string.
            if arg is not None:
                if filebound:
                    args.__dict__[param_name] = _object_from_file(arg, type_name)
                else:
                    args.__dict__[param_name] = _object_from_string(arg, type_name)
        
        return function, args, retfile
    
    def walk(self):
        """Generate nodes of gactfunc command tree.
        
        Yields:
            tuple: Contains three elements: `commands` are the keys specifying
                   the current node in the gactfunc command tree, and matching
                   the commands input in the terminal; `subcommands` are the
                   subcommands available at this node; and `func_spec` gives
                   information about a gactfunc, if accessible at the current
                   node.
        """
        
        # Init command stack from root of gactfunc command tree.
        command_stack = [ (list(), self._data) ]
        
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
            elif isinstance(x, dict):
                
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

def _bool_from_file(f):
    """Get bool from file."""
    with TextReader(f) as fh:
        s = fh.read().strip()
    return _bool_from_string(s)

def _bool_from_string(s):
    """Get bool from string."""
    if not isinstance(s, basestring):
        raise TypeError("object is not of type string ~ {!r}".format(s))
    try:
        x = safe_load(s)
        assert isinstance(x, bool)
    except (AssertionError, YAMLError):
        raise ValueError("failed to parse Boolean string ~ {!r}".format(s))
    return x

def _bool_to_file(x, f):
    """Output bool to file."""
    s = _bool_to_string(x)
    with TextWriter(f) as fh:
        fh.write('{}\n'.format(s))

def _bool_to_string(x):
    """Convert bool to string."""
    return 'true' if x else 'false'

def _DataFrame_from_file(f):
    """Get Pandas DataFrame from file."""
    
    try:
        with TextReader(f) as reader:
            x = read_csv(reader, sep=',', header=0, mangle_dupe_cols=False,
                skipinitialspace=True, true_values=_info['true_values'],
                false_values=_info['false_values'], keep_default_na=False,
                na_values=_info['na_values'])
    except (IOError, OSError):
        raise RuntimeError("failed to get DataFrame from file ~ {!r}".format(f))
    
    return x

def _DataFrame_from_string(s):
    """Get Pandas DataFrame from string."""
    
    if not isinstance(s, basestring):
        raise TypeError("object is not of string type ~ {!r}".format(s))
    
    try:
        with BytesIO(s) as fh:
            x = read_csv(fh, sep=',', header=0, mangle_dupe_cols=False,
                skipinitialspace=True, true_values=_info['true_values'],
                false_values=_info['false_values'], keep_default_na=False,
                na_values=_info['na_values'])
    except (IOError, OSError):
        raise RuntimeError("failed to get DataFrame from string ~ {!r}".format(f))
    
    return x

def _DataFrame_to_file(x, f):
    """Output Pandas DataFrame to file."""
    try:
        with TextWriter(f) as writer:
            x.to_csv(writer, sep=',', na_rep=_info['na_values'][0], index=False)
    except (IOError, OSError):
        raise ValueError("failed to output DataFrame to file ~ {!r}".format(x))

def _DataFrame_to_string(x):
    """Convert Pandas DataFrame to string."""
    try:
        with BytesIO() as fh:
            x.to_csv(fh, sep=',', na_rep=_info['na_values'][0], index=False)
            s = fh.getvalue()
    except (IOError, OSError):
        raise ValueError("failed to output DataFrame to string ~ {!r}".format(x))
    
    return s

def _dict_from_file(f):
    """Get dictionary from file."""
    
    try:
        with TextReader(f) as reader:
            x = safe_load(reader)
        assert isinstance(x, dict)
    except (AssertionError, IOError, YAMLError):
        raise ValueError("failed to load dictionary from file ~ {!r}".format(f))
    
    _Gactfunc._validate_param_type(x)
    
    return x

def _dict_from_string(s):
    """Get dictionary from string."""
    
    if not isinstance(s, basestring):
        raise TypeError("object is not of string type ~ {!r}".format(s))
    
    if not ( s.startswith('{') and s.endswith('}') ):
        s = '{' + s + '}'
    
    try:
        x = safe_load(s)
        assert isinstance(x, dict)
    except (AssertionError, YAMLError):
        raise ValueError("failed to parse dict from string ~ {!r}".format(s))
    
    _Gactfunc._validate_param_type(x)
    
    return x

def _dict_to_file(x, f):
    """Output dictionary to file."""
    try:
        with TextWriter(f) as writer:
            safe_dump(x, writer, default_flow_style=False, width=sys.maxint)
    except (IOError, YAMLError):
        raise ValueError("failed to output dictionary to file ~ {!r}".format(x))
    

def _dict_to_string(x):
    """Convert dictionary to string."""
    
    try:
        s = safe_dump(x, default_flow_style=True, width=sys.maxint)
        assert isinstance(s, basestring)
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert dict to string ~ {!r}".format(x))
    
    s = s.rstrip('\n')
    
    return s

def _float_from_file(f):
    """Get float from file."""
    with TextReader(f) as fh:
        s = fh.read().strip()
    return float(s)

def _float_to_file(x, f):
    """Output float to file."""
    s = str(x)
    with TextWriter(f) as fh:
        fh.write('{}\n'.format(s))

def _get_type_name(x):
    """Get type name of object."""
    
    for t in _gtypes:
        if _gtypes[t].match(x):
            return t
    
    raise TypeError("unknown gactfunc parameter/return type ~ {!r}".format(type(x).__name__))

def _int_from_file(f):
    """Get integer from file."""
    with TextReader(f) as fh:
        s = fh.read().strip()
    return int(s)

def _int_to_file(x, f):
    """Output integer to file."""
    s = str(x)
    with TextWriter(f) as fh:
        fh.write('{}\n'.format(s))

def _list_from_file(f):
    """Get list from file."""
    
    with TextReader(f) as reader:
        
        document_ended = False
        x = list()
        
        for line in reader:
            
            # Strip comments.
            try:
                i = line.index('#')
            except ValueError:
                pass
            else:
                line = line[:i]
            
            # Strip leading/trailing whitespace.
            line = line.strip()
            
            # Skip lines after explicit document end.
            if document_ended:
                if line != '':
                    raise RuntimeError("list elements found after document end")
                continue
            
            # Check for document separator.
            if line == '---':
                raise RuntimeError("expected a single document in list stream")
            
            try:
                element = safe_load(line)
            except YAMLError as e:
                # If explicit document end, flag and continue to next line..
                if e.problem == "expected the node content, but found '<document end>'":
                    document_ended = True
                    element = None
                    continue
                else:
                    raise e
            
            # Append line.
            x.append(element)
        
        # Strip trailing null values.
        while len(x) > 0 and x[-1] is None:
            x.pop()
    
    _Gactfunc._validate_param_type(x)
    
    return x

def _list_from_string(s):
    """Get list from string."""
    
    if not isinstance(s, basestring):
        raise TypeError("object is not of string type ~ {!r}".format(s))
    
    if not ( s.startswith('[') and s.endswith(']') ):
        s = '[' + s + ']'
    
    try:
        x = safe_load(s)
        assert isinstance(x, list)
    except (AssertionError, YAMLError):
        raise ValueError("failed to parse list from string ~ {!r}".format(s))
    
    _Gactfunc._validate_param_type(x)
    
    return x

def _list_to_file(x, f):
    """Output list to file."""
    
    with TextWriter(f) as writer:
        for element in x:
            try:
                line = _object_to_string(element)
                writer.write( '{}\n'.format( line.rstrip('\n') ) )
            except (IOError, ValueError):
                raise ValueError("failed to output list to file ~ {!r}".format(x))
    
def _list_to_string(x):
    """Convert list to string."""
    
    try:
        s = safe_dump(x, default_flow_style=True, width=sys.maxint)
        assert isinstance(s, basestring)
    except (AssertionError, YAMLError):
        raise ValueError("failed to convert list to string ~ {!r}".format(x))
    
    s = s.rstrip('\n')
    
    return s

def _None_from_file(f):
    """Get None from file."""
    with TextReader(f) as fh:
        s = fh.read().strip()
    return _None_from_string(s)

def _None_from_string(s):
    """Get None from string."""
    if not isinstance(s, basestring):
        raise TypeError("object is not of type string ~ {!r}".format(s))
    if s != 'null':
        raise ValueError("failed to create 'NoneType' from string ~ {!r}".format(s))
    return None

def _None_to_file(x, f):
    """Output None to file."""
    s = _None_to_string(x)
    with TextWriter(f) as fh:
        fh.write('{}\n'.format(s))

def _None_to_string(x):
    """Convert None to string."""
    return 'null'

def _object_from_file(f, object_type):
    """Get object from file."""
    if object_type == 'NoneType':
        x = _None_from_file(f)
    elif object_type == 'bool':
        x = _bool_from_file(f)
    elif object_type == 'float':
        x = _float_from_file(f)
    elif object_type == 'int':
        x = _int_from_file(f)
    elif object_type == 'dict':
        x = _dict_from_file(f)
    elif object_type == 'list':
        x = _list_from_file(f)
    elif object_type == 'DataFrame':
        x = _DataFrame_from_file(f)
    elif object_type == 'string':
        x = _string_from_file(f)
    else:
        raise ValueError("failed to get unsupported type ({!r}) from file".format(object_type))
    return x

def _object_from_string(s, object_type):
    """Get object from string."""
    if object_type == 'NoneType':
        x = _None_from_string(s)
    elif object_type == 'bool':
        x = _bool_from_string(s)
    elif object_type == 'float':
        x = float(s)
    elif object_type == 'int':
        x = int(s)
    elif object_type == 'dict':
        x = _dict_from_string(s)
    elif object_type == 'list':
        x = _list_from_string(s)
    elif object_type == 'DataFrame':
        x = _DataFrame_from_string(s)
    elif object_type == 'string':
        x = s
    else:
        raise ValueError("failed to get unsupported type ({!r}) from string".format(object_type))
    return x

def _object_to_file(x, f):
    """Output object to file."""
    if _gtypes['NoneType'].match(x):
        _None_to_file(x, f)
    elif _gtypes['bool'].match(x):
        _bool_to_file(x, f)
    elif _gtypes['float'].match(x):
        _float_to_file(x, f)
    elif _gtypes['int'].match(x):
        _int_to_file(x, f)
    elif _gtypes['dict'].match(x):
        _dict_to_file(x, f)
    elif _gtypes['list'].match(x):
        _list_to_file(x, f)
    elif _gtypes['DataFrame'].match(x):
        _DataFrame_to_file(x, f)
    elif _gtypes['string'].match(x):
        _string_to_file(x, f)
    else:
        raise ValueError("failed to output object of unsupported type ({!r}) to file".format(type(x).__name__))

def _object_to_string(x):
    """Convert object to string."""
    if _gtypes['NoneType'].match(x):
        s = _None_to_string(x)
    elif _gtypes['bool'].match(x):
        s = _bool_to_string(x)
    elif _gtypes['float'].match(x):
        s = str(x)
    elif _gtypes['int'].match(x):
        s = str(x)
    elif _gtypes['dict'].match(x):
        s = _dict_to_string(x)
    elif _gtypes['list'].match(x):
        s = _list_to_string(x)
    elif _gtypes['DataFrame'].match(x):
        s = _DataFrame_to_string(x)
    elif _gtypes['string'].match(x):
        s = x
    else:
        raise ValueError("failed to convert object of unsupported type ({!r}) to string".format(type(x).__name__))
    return s

def _string_from_file(f):
    """Get string from file."""
    with TextReader(f) as fh:
        s = fh.read().rstrip()
    _Gactfunc._validate_param_type(s)
    return s

def _string_to_file(s, f):
    """Output string to file."""
    with TextWriter(f) as fh:
        fh.write('{}\n'.format(s))

def _validate_delimitable(x):
    """Validate delimitable object type."""
    
    t = _get_type_name(x)
    
    if t == 'string':
        
        _validate_ductile(x)
        
    elif t == 'dict':
        
        for key, value in x.items():
            _validate_ductile(key)
            _validate_ductile(value)
        
    elif t == 'list':
        
        for element in x:
            _validate_ductile(element)
    
    elif not _gtypes[t].is_delimitable:
        raise TypeError("{} is not delimitable ~ {!r}".format(t, x))

def _validate_ductile(x):
    """Validate ductile object type."""
    
    t = _get_type_name(x)
    
    if t == 'string':
        
        if '\n' in x:
            raise ValueError("string is not ductile ~ {!r}".format(x))
        
    elif t == 'dict':
        
        for key, value in x.items():
            _validate_ductile(key)
            _validate_ductile(value)
        
    elif t == 'list':
        
        for element in x:
            _validate_ductile(element)
    
    elif not _gtypes[t].is_ductile:
        raise TypeError("{} is not ductile ~ {!r}".format(t, x))

################################################################################

def gactfunc(function):
    """Gactfunc decorator function."""
    return _Gactfunc(function)

def gaction(argv=None):
    """Run gaction command."""

    if argv is None:
        argv = sys.argv[1:]
    
    gf = _GactfuncCollection()
    
    ap = gf.prep_argparser()
    
    args = ap.parse_args(argv)
    
    function, args, retfile = gf.proc_args(args)
    
    result = function( **vars(args) )
    
    if result is not None:
        _object_to_file(result, retfile)

def main():
    gaction()

################################################################################

if __name__ == '__main__':
    main()

################################################################################
