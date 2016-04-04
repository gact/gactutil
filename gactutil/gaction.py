#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""GACTutil command-line interface."""

from argparse import ArgumentError
from argparse import ArgumentParser
from argparse import FileType
from argparse import RawDescriptionHelpFormatter
from argparse import REMAINDER
from collections import deque
from collections import OrderedDict
from imp import load_source
from importlib import import_module
from inspect import getargspec
from inspect import getmembers
from inspect import getmodule
from inspect import isfunction
from inspect import stack
import os
from pydoc import locate
import re
import sys
from textwrap import dedent

from yaml import dump
from yaml import safe_load
from yaml import YAMLError

from gactutil import _read_about
from gactutil import _read_command_info

################################################################################

# Supported commands. 
_commands = ('filter', 'index', 'prep', 'setrg')

_info = {
    
    # Supported parameter types. These must be suitable for use both  
    # as command-line arguments and Python function arguments. Any data type 
    # added here must be explicitly handled in the function '_proc_args'.
    'types': { 
        bool, 
        float, 
        int, 
        str,
        dict, 
        list
     },
     
     # Scalar data types.
     'scalars': (bool, float, int, str),
     
     # Collection data types. It should be possible for these to be passed as a 
     # filepath on the command-line, and the process of reading the object from 
     # that file should be handled within the function '_proc_args'.
    'collections': (dict, list),
    
    # Alias parameters: mappings of Python function parameters to command-line 
    # flags. These make it possible for common parameters to take a short form 
    # on the command line. If a command function uses an alias parameter, this 
    # is automatically converted to the corresponding flag by '_prep_argparser'.
    'alias-params': { 
        'indir': '-i',
        'infile': '-i', 
        'infile0': '-0',
        'infile1': '-1',
        'infile2': '-2',
        'outdir': '-o',
        'outfile': '-o', 
        'outfile0': '--0',
        'outfile1': '--1',
        'outfile2': '--2'
    },
    
    # Command-function docstring headers.
    'docstring-headers': {
        
        'known': ('Args', 'Arguments', 'Attributes', 'Example', 'Examples',
                  'Keyword Args', 'Keyword Arguments', 'Methods', 'Note', 
                  'Notes', 'Other Parameters', 'Parameters', 'Return', 
                  'Returns', 'Raises', 'References', 'See Also', 'Warning', 
                  'Warnings', 'Warns', 'Yield', 'Yields'),
            
        'supported': ('Args', 'Arguments', 'Note', 'Notes', 'Parameters', 
                      'Return', 'Returns', 'References', 'See Also'),
        
        'alias-mapping': { 'Arguments': 'Args', 'Parameters': 'Args', 
                           'Return': 'Returns' }
    },
    
    'pattern': {
        'command-function': re.compile( '^({})_(\w+)$'.format('|'.join(_commands))),
        'docstring-header': re.compile('^(\w+):\s*$'),
        'docstring-param': re.compile('^([*]{0,2}\w+)\s*(?:\((\w+)\))?:\s+(.+)$'),
        'docstring-return': re.compile('^(\w+):\s+(.+)$'),
        'docstring-default': re.compile('[[(]default:\s+(.+?)\s*[])]', re.IGNORECASE)
    }
}

################################################################################

def _bool_from_string(s):
    """Get bool from string."""
    if not isinstance(s, basestring):
        raise TypeError("object is not of type string ~ {!r}".format(s))
    if s == 'True':
        value = True
    elif s == 'False':
        value = False
    else:
        raise ValueError("failed to create 'bool' from string ~ {!r}".format(s))
    return value

def _coerce_from_string(s, return_type):
    """Coerce string to specified type."""
    if return_type is None:
        s = _None_from_string(s)
    elif return_type == bool:
        s = _bool_from_string(s)
    elif return_type == float:
        s = float(s)
    elif return_type == int:
        s = int(s)
    elif return_type == dict:
        s = _dict_from_string(s)
    elif return_type == list:
        s = _list_from_string(s)
    else:
        if not isinstance(s, basestring):
            raise TypeError("object is not of type string ~ {!r}".format(s))
        if return_type != str:
            raise ValueError("failed to coerce string to unsupported type ~ {!r}".format(return_type.__name__))
    return s

def _dict_from_file(dict_file):
    """Get dictionary from file."""
    
    if not isinstance(dict_file, basestring):
        raise TypeError("dictionary filepath is not of type string ~ {!r}".format(dict_file))
    
    try:
        with open(dict_file) as fh:
            result = safe_load(fh)
        assert isinstance(result, dict)
    except (AssertionError, IOError, YAMLError):
        raise ValueError("failed to load dictionary from file ~ {!r}".format(dict_file))
     
    return result

def _dict_from_string(dict_string):
    """Get dictionary from string."""
    
    if not isinstance(dict_string, basestring):
        raise TypeError("object is not of type string ~ {!r}".format(dict_string))
    
    if not ( dict_string.startswith('{') and dict_string.endswith('}') ):
        dict_string = '{' + dict_string + '}'
    
    try:
        result = safe_load(dict_string)
        assert isinstance(result, dict)
    except (AssertionError, YAMLError):
        raise ValueError("failed to parse dictionary string ~ {!r}".format( 
            dict_string))
    return result

def _list_from_file(list_file):
    """Get list of scalars from file."""
    
    if not isinstance(list_file, basestring):
        raise TypeError("list filepath is not of type string ~ {!r}".format(list_file))
    
    try:
        with open(list_file) as fh:
            elements = [ line.rstrip() for line in fh.readlines() ]
    except IOError:
        raise ValueError("failed to load list from file ~ {!r}".format(list_file))
    
    while len(elements) > 0 and elements[-1] == '':
        elements.pop()
    
    try:
        result = [ safe_load(x) for x in elements ]
    except YAMLError:
        raise ValueError("failed to parse list elements in file ~ {!r}".format(list_file))
    
    for element in result:
        if element is not None and type(element) not in _info['scalars']:
            raise ValueError("failed to parse element of type {!r} in list file ~ {!r}".format(
                type(element).__name__, list_file))
    
    return result

def _list_from_string(list_string):
    """Get list of scalars from string."""
    
    if not isinstance(list_string, basestring):
        raise TypeError("object is not of type string ~ {!r}".format(list_string))
    
    if not ( list_string.startswith('[') and list_string.endswith(']') ):
        list_string = '[' + list_string + ']'
    
    try:
        result = safe_load(list_string)
        assert isinstance(result, list)
    except (AssertionError, YAMLError):
        raise ValueError("failed to parse list string ~ {!r}".format(list_string))
    
    for element in result:
        if element is not None and type(element) not in _info['scalars']:
            raise ValueError("failed to parse element of type {!r} in list string ~ {!r}".format(
                type(element).__name__, list_string))
    
    return result

def _None_from_string(s):
    """Get None from string."""
    if not isinstance(s, basestring):
        raise TypeError("object is not of type string ~ {!r}".format(s))
    if s != 'None':
        raise ValueError("failed to create 'NoneType' from string ~ {!r}".format(s))
    return None

def _parse_cmdfunc_docstring(function):
    """Parse command-function docstring.
    
    This function parses a command function docstring and returns an ordered 
    dictionary mapping headers to documentation. Command-function docstrings 
    must be in Google-style format. The keys of the returned dictionary will
    correspond to the docstring headers (e.g. 'Args'), in addition to two 
    special headers: 'Summary', which will contain the docstring summary line;
    and 'Description', which will contain the docstring description, if present.
    """
    
    # Get function name.
    func_name = function.__name__
    
    # Check command function is indeed a function.
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
            m = _info['pattern']['docstring-header'].match(line)
            
            # If matches, set header of new section..
            if m is not None:
                
                # Set current header.
                h = m.group(1)
                
                # Map header to alias, if relevant.
                if h in _info['docstring-headers']['alias-mapping']:
                    h = _info['docstring-headers']['alias-mapping'][h]
                
                # Check header is known.
                if h not in _info['docstring-headers']['known']:
                    raise ValueError("unknown docstring header ~ {!r}".format(h))
                
                # Check header is supported.
                if h not in _info['docstring-headers']['supported']:
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
                        m = _info['pattern']['docstring-param'].match(line)
                    
                        # If this is a parameter definition line, get parameter info..
                        if m is not None:
                            
                            param_name, type_name, param_desc = m.groups()
                            type_value = locate(type_name)
                            
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
                            
                            # Check parameter is of known type.
                            if type_value is None or type_value not in _info['types']:
                                raise ValueError("{} docstring specifies unsupported type {!r} for parameter {!r}".format(
                                    func_name, type_name, param_name))
                            
                            # Check for duplicate parameters.
                            if param_name in param_info:
                                raise ValueError("{} docstring contains duplicate parameter ~ {!r}".format(
                                    func_name, param_name))
                            
                            param_info[param_name] = { 
                                'type': type_value, 
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
                    defaults = _info['pattern']['docstring-default'].findall(
                        param_info[param_name]['description'])
                    
                    # If a default definition matched, keep 
                    # string representation of default value..
                    if len(defaults) == 1:
                        param_info[param_name]['docstring-default'] = defaults[0]
                    # ..otherwise the description has ambiguous default info.
                    elif len(defaults) > 1:
                        raise ValueError("{} docstring has multiple defaults for parameter {!r}".format(
                            func_name, param_name))
                
                # Set parsed parameter info for docstring.
                doc_info[h] = param_info
                
            elif h == 'Returns':
                
                # Init parsed return value info.
                ret_info = OrderedDict()
                
                type_value = None
                description = list()
                
                # Process each line of return value section.
                for line in raw_info[h]:
                    
                    line = line.strip()
                    
                    # Skip blank lines.
                    if line != '':
                        
                        # If this is the first line, try to get type info.
                        if len(ret_info) == 0:
                            
                            # Try to match line to expected pattern of return value.
                            m = _info['pattern']['docstring-return'].match(line)
                            
                            # If return value type info is present, 
                            # get type info and initial description..
                            if m is not None:
                                
                                type_name = m.group(1)
                                description.append( m.group(2) )
                                type_value = locate(type_name)
                                
                                # Check type name is not 'None'.
                                if type_name == 'None':
                                    raise ValueError("{} docstring specifies 'NoneType' for return value".format(
                                        func_name))
                                
                                # Check return value type is supported.
                                if type_value not in _info['types']:
                                    raise ValueError("{} docstring specifies unsupported type {!r} for return value".format(
                                        func_name, type_name ))
                            
                            # ..otherwise set description from first line.
                            else:
                                description.append(line)
                        
                        # ..otherwise append to description.
                        else:
                            description.append(line)
                
                # Set return value info.
                ret_info = { 
                    'type': type_value,
                    'description': ' '.join(description)
                }
                
                # Set parsed return value info for docstring.
                doc_info[h] = ret_info
                
            else:
                
                # Strip leading/trailing blank lines.
                lines = raw_info[h]
                for i in (0, -1):
                    while len(lines) > 0 and lines[i].strip() == '':
                        lines.pop(i)
                doc_info[h] = '\n'.join(lines)
       
    return doc_info

def _parse_cmdfunc_name(function):
    """Parse command-function name."""
    
    # Get function name.
    func_name = function.__name__
    
    # Check command function is indeed a function.
    if not isfunction(function):
        return TypeError("object is not a function ~ {!r}".format(func_name))
    
    # Try to match function name to command function
    m = _info['pattern']['command-function'].match(func_name)
    
    # Parse command function.
    try:
        command, qualifier = m.groups()
    except AttributeError:
        raise ValueError("command function does not follow GACTutil naming convention ~ {!r}".format(func_name))
        
    return command, qualifier

def _prep_argparser():
    """Prep command-line argument parser."""
    
    # Validate caller.
    caller_file, caller_func = [ (stack()[1])[i] for i in (1, 3) ]
    if os.path.basename(caller_file) != 'gaction.py' or caller_func != 'gaction':
        raise RuntimeError("function {!r} should only be called by GACTutil function 'gaction'".format(stack()[0][3]))
    
    # Set version string.
    prog = os.path.splitext( os.path.basename(__file__) )[0]
    about = _read_about()
    version = '{}-{}'.format(prog, about['version'])
    
    # Read package command info.
    cmd_info = _read_command_info()
    
    # Init argument parser.
    ap = ArgumentParser(description='\n{}\n\n{}\n'.format(version, __doc__))
    
    # Add version parameter.
    ap.add_argument('-v', '--version', action='version', version=version)
    
    # Add command subparser.
    sp = ap.add_subparsers(title='commands')
    
    # Populate argument parser for each command and function.
    for c in sorted( cmd_info.keys() ):
        
        # Add parser for this command.
        apc = sp.add_parser(c)
        
        # Add qualifier subparser for this command.
        spc = apc.add_subparsers(title='qualifiers')
        
        # Populate argument parser for every qualifier of this command.
        for q in sorted( cmd_info[c].keys() ):
            
            # Get module of the given command function.
            module = import_module(cmd_info[c][q]['module'])
            
            # Get command function definition.
            function = getattr(module, cmd_info[c][q]['name'])
            
            # Get command function summary.
            summary = cmd_info[c][q]['summary']
            
            # Get command function description, if present.
            if 'description' in cmd_info[c][q]:
                description = '\n\n{}'.format(cmd_info[c][q]['description'])
            else:
                description = None
            
            # Add parser for this command function.
            apq = spc.add_parser(q, help=summary, description=description)
            
            # If command function has parameters..
            if 'params' in cmd_info[c][q]:
                
                # ..add each parameter to the argument parser.
                for param_name in cmd_info[c][q]['params']:
                    
                    # Get info for this parameter.
                    param_info = cmd_info[c][q]['params'][param_name]
                    
                    # Add parameter to argparser.
                    if param_info['group'] == 'positional':
                    
                        apq.add_argument(param_name, 
                            help = param_info['description'])
                            
                    elif param_info['group'] == 'optional':
                    
                        apq.add_argument(param_info['flag'], 
                            dest     = param_name, 
                            metavar  = param_info['type'].__name__.upper(),
                            default  = param_info['default'], 
                            required = param_info['required'], 
                            help     = param_info['description'])
                    
                    elif param_info['group'] == 'short':
                        
                        apq.add_argument(param_info['flag'], 
                            dest     = param_name, 
                            default  = param_info['default'], 
                            required = param_info['required'], 
                            help     = param_info['description'])
                        
                    elif param_info['group'] == 'switch':
                    
                        apq.add_argument(param_info['flag'], 
                            dest   = param_name, 
                            action = 'store_true', 
                            help   = param_info['description'])
                            
                    elif param_info['group'] == 'collection':
                        
                        # Set info for pair of alternative parameters.
                        title = '{} argument'.format( param_name.replace('_', '-') )
                        file_param_name = '{}_file'.format(param_name)
                        file_flag = '{}-file'.format(param_info['flag'])
                        item_help = 'Set {} from string.'.format(
                            param_info['type'].__name__)
                        file_help = 'Load {} from file.'.format(
                            param_info['type'].__name__)
                        
                        # Add (mutually exclusive) pair of alternative parameters.
                        ag = apq.add_argument_group(
                            title       = title, 
                            description = param_info['description'])
                        mxg = ag.add_mutually_exclusive_group(
                            required    = param_info['required'])
                        mxg.add_argument(param_info['flag'], 
                            dest        = param_name, 
                            metavar     = 'STR',
                            default     = param_info['default'], 
                            help        = item_help)
                        mxg.add_argument(file_flag, 
                            dest        = file_param_name, 
                            metavar     = 'PATH',
                            help        = file_help)
            
            # Set function for this commmand.
            apq.set_defaults(function=function)
    
    return ap

def _proc_args(args):
    """Process parsed command-line arguments."""
    
    # Validate caller.
    caller_file, caller_func = [ (stack()[1])[i] for i in (1, 3) ]
    if os.path.basename(caller_file) != 'gaction.py' or caller_func != 'gaction':
        raise RuntimeError("function {!r} should only be called by GACTutil function 'gaction'".format(stack()[0][3]))
    
    # Read package command info.
    cmd_info = _read_command_info()
        
    # Get command function definition.
    try:
        function = args.__dict__.pop('function')
    except KeyError:
        raise RuntimeError("cannot run command - no function available")
    
    # Get parameter info for this command function.
    command, qualifier = _parse_cmdfunc_name(function)
    param_info = cmd_info[command][qualifier]['params']
    
    # Process each argument.
    for param_name in param_info:
        
        # Get expected argument type.
        param_type = param_info[param_name]['type']
        
        # Get argument value.
        arg = args.__dict__[param_name]
        
        # Assume argument is not to be loaded from file.
        filebound = False
        
        # If paremeter is of a collection type, 
        # check both alternative arguments.
        if param_type in _info['collections']:
            
            # Get file parameter name and argument value.
            file_param_name = '{}_file'.format(param_name)
            file_arg = args.__dict__[file_param_name]
            
            # If file argument specified, set argument value from file 
            # argument, indicate argument value is to be loaded from file..
            if file_arg is not None:
                arg = file_arg
                filebound = True
            # ..otherwise check argument specified (if required).
            elif arg is None and param_info[param_name]['required']:
                raise ArgumentError("option {!r} is required".format(
                    param_info[param_name]['flag']))
            
            # Remove file parameter from parsed arguments.
            del args.__dict__[file_param_name]
        
        # If argument specified, process it.
        if arg is not None:
            try:
                if param_type == bool:
                    args.__dict__[param_name] = _bool_from_string(arg)
                elif param_type == float:
                    args.__dict__[param_name] = float(arg)
                elif param_type == int:
                    args.__dict__[param_name] = int(arg)
                elif param_type == dict:
                    if filebound:
                        args.__dict__[param_name] = _dict_from_file(arg)
                    elif isinstance(args.__dict__[param_name], basestring):
                        args.__dict__[param_name] = _dict_from_string(arg)
                elif param_type == list:
                    if filebound:
                        args.__dict__[param_name] = _list_from_file(arg)
                    elif isinstance(args.__dict__[param_name], basestring):
                        args.__dict__[param_name] = _list_from_string(arg)
                elif param_type != str:
                    raise ArgumentError("failed to process argument {!r} of unsupported type {!r}".format(
                        param_name, param_type.__name__))
            except IOError:
                raise ArgumentError("failed to process argument {!r} of type {!r}".format(
                    param_name, param_type.__name__))
    
    return function, args

def _setup_commands():
    """Setup package commands.
    
    Outputs a package data file in YAML format with command function information.
    
    NB: this function should only be called during package setup.
    """
    
    # Validate caller.
    caller_file, caller_func = [ (stack()[1])[i] for i in (1, 3) ]
    if caller_file != 'setup.py' or caller_func != '<module>':
        raise RuntimeError("function {!r} should only be called during GACTutil package setup".format(stack()[0][3]))
    
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
    
    # With module info, create listing of info for every package function. Each 
    # element will be a tuple containing respectively the name of the function's
    # module, the name of the function itself, and the function definition.
    func_tuples = list()
    for mod_name, mod_path in mod_info.items():
        module = load_source(mod_name, mod_path)
        func_tuples += [ (mod_name, func_name, function) 
            for func_name, function in getmembers(module, isfunction) 
            if getmodule(function) == module ]
    
    # Init command info from those functions that follow the package  
    # command function naming convention (i.e. <command>_<qualifier>). 
    cmd_info = dict()
    for mod_name, func_name, function in func_tuples:
        try:
            command, qualifier = _parse_cmdfunc_name(function)
        except (TypeError, ValueError):
            continue
        if command in cmd_info and qualifier in cmd_info[command]:
            raise ValueError("duplicated command function "
                "- {!r}".format(func_name))
        cmd_info.setdefault(command, dict())
        cmd_info[command][qualifier] = OrderedDict([ ('module', mod_name),
            ('name', func_name), ('function', function) ])
    
    # Process each command function for a given command and qualifier.
    for command in cmd_info:
        
        for qualifier in cmd_info[command]:
            
            # Get existing command info for this function.
            func_info = cmd_info[command][qualifier]
            
            # Get function definition.
            function = func_info.pop('function')
            
            # Get function parameter spec.
            param_spec = getargspec(function)
            
            # Check that there are no unenumerated arguments.
            if param_spec.varargs is not None or param_spec.keywords is not None:
                raise ValueError("{} cannot have unenumerated arguments".format(func_info['name']))
            
            # Get specified parameter names.
            spec_params = param_spec.args
            
            # Map formal keyword parameters to their defaults.
            if param_spec.defaults is not None:
                i = len(param_spec.defaults)
                spec_def_info = { k: x for k, x in 
                    zip(spec_params[-i:], param_spec.defaults) }
            else:
                spec_def_info = None
            
            # Set docstring info from command-function docstring.
            doc_info = _parse_cmdfunc_docstring(function)
            
            # Check that command function has been documented.
            if doc_info is None:
                raise ValueError("GACTutil command function {!r} is not documented".format(func_info['name']))
            
            func_info['summary'] = doc_info['Summary']
            
            if 'Description' in doc_info:
                func_info['description'] = doc_info['Description']
            
            # If parameters documented, validate them..
            if 'Args' in doc_info:
                
                # Set command-function parameter info from parsed docstring.
                func_info['params'] = doc_info['Args']
                
                # Get set of documented parameters.
                doc_param_set = set(func_info['params'])
                
                # Get set of parameters specified in function definition.
                spec_param_set = set(spec_params)
                
                # Check for parameters in docstring but not in function definition.
                undef_params = list(doc_param_set - spec_param_set)
                if len(undef_params) > 0:
                    raise ValueError("{} parameters documented but not defined ~ {!r}".format(
                        func_info['name'], undef_params))
                        
                # Check for parameters in function definition but not in docstring.
                undoc_params = list(spec_param_set - doc_param_set)
                if len(undoc_params) > 0:
                    raise ValueError("{} parameters defined but not documented ~ {!r}".format(
                        func_info['name'], undoc_params))
                
                # Validate any formal keyword parameters.
                if spec_def_info is not None:
                    
                    for param_name, default in spec_def_info.items():
                        
                        func_info['params'][param_name]['default'] = default
                        
                        # Skip unspecified defaults as we cannot validate them.
                        if default is None:
                            continue
                            
                        # Get parameter type.
                        param_type = func_info['params'][param_name]['type']
                        
                        # Check that the defined default value is of the 
                        # type specified in the function documentation.
                        if not isinstance(default, param_type):
                            raise TypeError("{} definition has default type mismatch for parameter {!r}".format(
                                func_info['name'], param_name))
                        
                        # Skip undocumented defaults.
                        if 'docstring-default' not in func_info['params'][param_name]:
                            continue
                            
                        # Get string representation of docstring default.
                        docstring_default = func_info['params'][param_name]['docstring-default']
                        
                        try: # Coerce documented default from string.
                            coerced_default = _coerce_from_string(docstring_default, param_type)
                        except (TypeError, ValueError):
                            raise TypeError("{} docstring has default type mismatch for parameter {!r}".format(
                                func_info['name'], param_name))
                            
                        # Check that documented default matches actual default.
                        if coerced_default != default:
                            raise ValueError("{} has default value mismatch for parameter {!r}".format(
                                func_info['name'], param_name))
                
                # Init flag set to check for conflicting option strings.
                flag_set = set()
                
                # Prepare parameters for argument parser.
                for param_name in func_info['params']:
                    
                    # Get info for this parameter.
                    param_info = func_info['params'][param_name]
                    
                    # If parameter has a default value, set as option or switch..
                    if 'default' in param_info:
                        
                        param_info['required'] = False
                        
                        # If default value is False, assign to switches..
                        if param_info['type'] == bool and param_info['default'] is False:
                            param_info['group'] = 'switch'
                        # ..otherwise assign to optionals.
                        else:
                            param_info['group'] = 'optional'
                        
                    # ..otherwise, assign to positional parameters.
                    else:
                        param_info['group'] = 'positional'
                        
                    # If parameter has a short form, convert to short form..
                    if param_name in _info['alias-params']:
                        
                        # Check that this is not a collection type.
                        if param_info['type'] in _info['collections']:
                            raise TypeError("cannot alias parameter {!r} of type {!r}".format(
                                param_name, param_info['type'].__name__))
                        
                        # Set flag to short form.
                        param_info['flag'] = _info['alias-params'][param_name]
                        
                        # Short form parameters are treated as optionals.
                        # If parameter was positional, set as required.
                        if param_info['group'] == 'positional':
                            param_info['required'] = True
                            param_info['default'] = None
                    
                        # Mark as short form optional.
                        param_info['group'] = 'short'
                    
                    # ..otherwise if parameter is of a collection type, create
                    # two (mutually exclusive) parameters: one to accept argument
                    # as a string, the other to load it from a file..
                    elif param_info['type'] in _info['collections']:
                        
                        # Collection parameters are treated as optionals.
                        # If parameter was positional, set as required.
                        if param_info['group'] == 'positional':
                            param_info['required'] = True
                            param_info['default'] = None
                        
                        # Mark as 'collection'.
                        param_info['group'] = 'collection'
                        
                        # Set parameter flag.
                        param_info['flag'] = '--{}'.format( param_name.replace('_', '-') )
                        
                        # Check that file option string does 
                        # not conflict with existing options.
                        file_flag = '{}-file'.format(param_info['flag'])
                        if file_flag in flag_set:
                            raise ValueError("{} parameter {!r} has conflicting alternative option {!r}".format(
                                func_info['name'], param_name, file_flag))
                        flag_set.add(file_flag)
                        
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
                            if param_info['group'] != 'switch' and not 'docstring-default' in param_info:
                                param_info['description'] = '{} [default: {}]'.format(
                                    param_info['description'], str(param_info['default']))
                        elif param_info['required']:
                            param_info['description'] = '{} [required]'.format(
                                param_info['description'])
                    
                    try: # Delete docstring default - no longer needed.
                        del param_info['docstring-default']
                    except KeyError:
                        pass
                    
                    # Check for conflicting option strings.
                    if 'flag' in param_info:
                        if param_info['flag'] in flag_set:
                            raise ValueError("{} parameter {!r} has conflicting option {!r}".format(
                                func_info['name'], param_name, param_info['flag']))
                        flag_set.add(param_info['flag'])
                    
                    # Update parameter info.
                    func_info['params'][param_name] = param_info
                    
            # ..otherwise, check that no parameters were defined.
            elif len(spec_params) > 0:
                raise ValueError("{} parameters defined but not documented ~ {!r}".format(
                    func_info['name'], spec_params))
            
            # Update command info.
            cmd_info[command][qualifier] = func_info
    
    # Ensure data directory exists.
    data_dir = os.path.join('gactutil', 'data')
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
    
    # Write command info.
    cmd_file = os.path.join(data_dir, 'gaction.yaml')
    with open(cmd_file, 'w') as fh:
        dump(cmd_info, fh, default_flow_style=False)

################################################################################

def gaction(argv=None):
    """Run gaction command."""

    if argv is None:
        argv = sys.argv[1:]
    
    ap = _prep_argparser()
    
    args = ap.parse_args(argv)
    
    function, args = _proc_args(args)
    
    function( **vars(args) )

def main():
    gaction()

################################################################################

if __name__ == '__main__':
    main()

################################################################################
