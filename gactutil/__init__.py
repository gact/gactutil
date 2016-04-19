#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
"""GACTutil module."""

from abc import ABCMeta
from binascii import hexlify
from collections import Iterable
from collections import Mapping
from collections import Sequence
from collections import Set
from contextlib import contextmanager
import errno
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
from sys import stdin
from sys import stdout
from tempfile import mkdtemp
from tempfile import NamedTemporaryFile
from tokenize import generate_tokens
from tokenize import TokenError
from yaml import dump
from yaml import load
from yaml import safe_dump
from yaml import safe_load
from yaml import YAMLError

#from gactutil.gaction import gactfunc

################################################################################

_info = {
    
    # Data types that are considered valid as keys or valuse in a mapping object.
    'mapping_types': (None, bool, int, float, str),
    
    # Config settings filename.
    'settings-file': 'settings.yaml'
}

################################################################################

class FrozenDict(Mapping):
    """Hashable dictionary class.
    
    A FrozenDict can be created and accessed in the same way as a builtin dict.
    The values of a FrozenDict are recursively frozen when the FrozenDict is
    created, and the resulting object cannot be subsequently modified through
    its public interface.
    """
    
    @classmethod
    def fromkeys(cls, keys, value=None):
        return cls( dict.fromkeys(keys, value=value) )
    
    def __init__(self, *args, **kwargs):
        
        try: # Init set of checked object IDs.
            checked = kwargs.pop('memo_set')
        except KeyError:
            checked = set()
        
        temp_dict = dict(*args, **kwargs)
        
        # Recursively coerce each container of
        # known type to its frozen counterpart.
        for k, x in temp_dict.items():
            
            # Skip if previously checked.
            xid = id(x)
            if xid in checked:
                continue
            checked.add(xid)
            
            # Skip if hashable type.
            # NB: must handle strings before Iterable.
            if isinstance(x, (bool, float, frozenset, int, long, basestring)):
                continue
            
            # If this is a Mapping, coerce to FrozenDict..
            if isinstance(x, Mapping):
                
                if isinstance(x, FrozenDict):
                    continue
                temp_dict[k] = x = FrozenDict(x, memo_set=checked)
                checked.add( id(x) )
                
            # ..otherwise if Set, coerce to frozenset..
            # NB: must handle sets before Iterable.
            elif isinstance(x, Set):
                
                temp_dict[k] = x = frozenset(x)
                checked.add( id(x) )
                for element in x:
                    checked.add( id(element) )
                
            # ..otherwise if Iterable, coerce to FrozenList..
            elif isinstance(x, Iterable):
                
                if isinstance(x, FrozenList):
                    continue
                temp_dict[k] = x = FrozenList(x, memo_set=checked)
                checked.add( id(x) )
                
            # ..otherwise check if hashable.
            else:
                try:
                    hash(x)
                except TypeError:
                    raise TypeError("unhashable type: {!r}".format(type(x).__name__))
        
        # Set dict attribute.
        self._data = temp_dict
    
    def __delattr__(self, key):
        raise TypeError("{!r} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __getitem__(self, key):
        return self._data.__getitem__(key)
    
    def __hash__(self):
        return hash( frozenset( self._data.items() ) )
    
    def __iter__(self):
        return self._data.__iter__()
    
    def __len__(self):
        return self._data.__len__()
        
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._data.__repr__()[1:-1])
    
    def __setattr__(self, key, value):
        if hasattr(self, '_dict'):
            raise TypeError("{!r} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[key] = value
    
    def copy(self):
        return self
    
    def clear(self):
        raise TypeError("{!r} object does not support the clear operation".format(
            self.__class__.__name__))
    
    def get(self, key):
        return self._data.get(key)
        
    def has_key(self, key):
        return self._data.has_key(key)
        
    def iteritems(self):
        return self._data.iteritems()
    
    def iterkeys(self):
        return self._data.iterkeys()
    
    def itervalues(self):
        return self._data.itervalues()
    
    def pop(self, *args, **kwargs):
        raise TypeError("{!r} object does not support the pop operation".format(
            self.__class__.__name__))
    
    def popitem(self):
        raise TypeError("{!r} object does not support the popitem operation".format(
            self.__class__.__name__))
    
    def setdefault(self, *args, **kwargs):
        raise TypeError("{!r} object does not support the setdefault operation".format(
            self.__class__.__name__))
    
    def to_dict(self, **kwargs):
        """Return object as a mutable dict."""
        
        try: # Init set of checked object IDs.
            checked = kwargs.pop('memo_set')
        except KeyError:
            checked = set()
        
        # Create dict from object.
        result = dict(self)
        
        # Recursively coerce each frozen container to its mutable counterpart.
        for k, x in result.items():
            
            # Skip if previously checked.
            xid = id(x)
            if xid in checked:
                continue
            checked.add(xid)
            
            if isinstance(x, FrozenDict):
                result[k] = x = x.to_dict(memo_set=checked)
                checked.add( id(x) )
            elif isinstance(x, frozenset):
                result[k] = x = set(x)
                checked.add( id(x) )
            elif isinstance(x, FrozenList):
                result[k] = x = x.to_list(memo_set=checked)
                checked.add( id(x) )
        
        return result
    
    def update(self, *args, **kwargs):
        raise TypeError("{!r} object does not support the update operation".format(
            self.__class__.__name__))
    
    def viewitems(self):
        return self._data.viewitems()
    
    def viewkeys(self):
        return self._data.viewkeys()
    
    def viewvalues(self):
        return self._data.viewvalues()
    
class FrozenList(Sequence):
    """Hashable list class.
    
    A FrozenList can be created and accessed in the same way as a builtin list.
    The values of a FrozenList are recursively frozen when the FrozenList is
    created, and the resulting object cannot be subsequently modified through
    its public interface.
    """
    
    def __init__(self, *args, **kwargs):
        
        try: # Init set of checked object IDs.
            checked = kwargs.pop('memo_set')
        except KeyError:
            checked = set()
        
        temp_list = list(*args, **kwargs)
        
        # Recursively coerce each container of
        # known type to its frozen counterpart.
        for i, x in enumerate(temp_list):
            
            # Skip if previously checked.
            xid = id(x)
            if xid in checked:
                continue
            checked.add(xid)
            
            # Skip if hashable type.
            # NB: must handle strings before Iterable.
            if isinstance(x, (bool, float, frozenset, int, long, basestring)):
                continue
            
            # If this is a Mapping, coerce to FrozenDict..
            if isinstance(x, Mapping):
                
                if isinstance(x, FrozenDict):
                    continue
                temp_list[i] = FrozenDict(x, memo_set=checked)
                checked.add( id(temp_list[i]) )
            
            # ..otherwise if Set, coerce to frozenset..
            # NB: must handle sets before Iterable.
            elif isinstance(x, Set):
                
                temp_dict[k] = x = frozenset(x)
                checked.add( id(x) )
                for element in x:
                    checked.add( id(element) )
            
            # ..otherwise if Iterable, coerce to FrozenList..
            elif isinstance(x, Iterable):
                
                if isinstance(x, FrozenList):
                    continue
                temp_list[i] = FrozenList(x, memo_set=checked)
                checked.add( id(temp_list[i]) )
                
            # ..otherwise check if hashable.
            else:
                try:
                    hash(x)
                except TypeError:
                    raise TypeError("unhashable type: {!r}".format(type(x).__name__))
        
        # Set tuple attribute.
        self._data = tuple(temp_list)
    
    def __delattr__(self, *args, **kwargs):
        raise TypeError("{!r} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __getitem__(self, index):
        return self._data[index]
    
    def __hash__(self):
        return hash(self._data)
    
    def __len__(self):
        return len(self._data)
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._data.__repr__()[1:-1])
    
    def __setattr__(self, index, value):
        if hasattr(self, '_tuple'):
            raise TypeError("{!r} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[index] = value
    
    def append(self, *args, **kwargs):
        raise TypeError("{!r} object does not support the append operation".format(
            self.__class__.__name__))
    
    def count(self, x):
        return self._data.count(x)
    
    def extend(self, *args, **kwargs):
        raise TypeError("{!r} object does not support extension".format(
                self.__class__.__name__))
    
    def index(self, x):
        try:
            return self._data.index(x)
        except ValueError:
            raise ValueError("{!r} is not in {}".format(x, self.__class__.__name__))
    
    def insert(self, *args, **kwargs):
        raise TypeError("{!r} object does not support item insertion".format(
                self.__class__.__name__))
    
    def pop(self, *args, **kwargs):
        raise TypeError("{!r} object does not support the pop operation".format(
                self.__class__.__name__))
    
    def remove(self, *args, **kwargs):
        raise TypeError("{!r} object does not support item removal".format(
                self.__class__.__name__))
    
    def reverse(self):
        raise TypeError("{!r} object does not support in-place reversal".format(
                self.__class__.__name__))
    
    def sort(self, *args, **kwargs):
        raise TypeError("{!r} object does not support in-place sorting".format(
                self.__class__.__name__))
    
    def to_list(self, **kwargs):
        """Return object as a mutable list."""
        
        try: # Init set of checked object IDs.
            checked = kwargs.pop('memo_set')
        except KeyError:
            checked = set()
        
        # Init list from object.
        result = list(self)
        
        # Recursively coerce each frozen container to its mutable counterpart.
        for i, x in enumerate(result):
            
            # Skip if previously checked.
            xid = id(x)
            if xid in checked:
                continue
            checked.add(xid)
            
            if isinstance(x, FrozenDict):
                result[i] = x = x.to_dict(memo_set=checked)
                checked.add( id(x) )
            elif isinstance(x, frozenset):
                result[k] = x = set(x)
                checked.add( id(x) )
            elif isinstance(x, FrozenList):
                result[i] = x = x.to_list(memo_set=checked)
                checked.add( id(x) )
        
        return result

class TextRW(object):  
    """Abstract text reader/writer base class."""
    __metaclass__ = ABCMeta
    
    @property
    def closed(self):
        """bool: True if file is closed; False otherwise."""
        try:
            return self._handle.closed
        except AttributeError:
            return None
    
    @property
    def name(self):
        """str: Name of specified file or standard file object."""
        return self._name

    @property
    def newlines(self):
        """str or tuple: Observed newlines."""
        try:
            return self._handle.newlines
        except AttributeError:
            return None
    
    def __init__(self):
        """Init text reader/writer."""
        self._closable = False
        self._handle = None
        self._name = None
    
    def __enter__(self):
        """TextRW: Get reader/writer on entry to a context block."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close reader/writer on exit from a context block."""
        self.close()
        
    def close(self):
        """Close reader/writer."""
        if self._closable:
            self._handle.close()

class TextReader(TextRW):
    """Text reader class."""
    
    def __init__(self, filepath):
        """Init text reader.
         
         If the input `filepath` is `-`, the new object will read from standard
         input. Otherwise, the specified filepath is opened for reading. Input
         that is GZIP-compressed is identified and extracted to a temporary file
         before reading.
        
        Args:
            filepath (str): Path of input file.
        """
        
        super(TextReader, self).__init__()
        
        if not isinstance(filepath, basestring):
            raise TypeError("cannot open filepath of type {!r}".format(type(filepath).__name__))
        
        # If filepath indicates standard input, 
        # prepare to read from standard input..
        if filepath == '-':
            
            self._name = stdin.name
            self._handle = io.open(stdin.fileno(), mode='rb')
            
        # ..otherwise resolve, validate, and open the specified filepath.
        else:
            
            self._closable = True
            
            filepath = resolve_path(filepath)
            self._name = relpath(filepath)
            
            if not os.path.exists(filepath):
                raise IOError("file not found ~ {!r}".format(self._name))
            elif not os.path.isfile(filepath):
                raise IOError("not a file ~ {!r}".format(self._name))
            
            self._handle = io.open(filepath, mode='rb')
        
        # Assume input is text.
        format = 'text'
        
        # Sample first three bytes.
        sample = self._handle.read(3)
        
        # If sample returned successfully, check if
        # it indicates content is GZIP-compressed.
        if len(sample) == 3:
            magic_number = hexlify(sample[:2])
            method = ord(sample[2:])
            if magic_number == '1f8b': # NB: magic number for GZIP content.
                if method != 8:
                    raise ValueError("input compressed with unknown GZIP method")
                format = 'gzip'
        
        # If input is GZIP-compressed, extract and then read decompressed text..
        if format == 'gzip':
            
            gzipfile, textfile = [None] * 2
            
            try:
                # Write GZIP temp file from input stream.
                with NamedTemporaryFile(mode='wb', delete=False) as gtemp:
                    
                    # Get path of GZIP temp file.
                    gzipfile = gtemp.name
                    
                    # Init data from sample.
                    data = sample
                    
                    # While data in input stream, write data to GZIP temp file.
                    while data:
                        gtemp.write(data)
                        data = self._handle.read(1048576)
                
                # Extract GZIP temp file to text temp file.
                with NamedTemporaryFile(mode='w', delete=False) as ftemp:
                    textfile = ftemp.name
                    with GzipFile(gzipfile) as gtemp:
                        for line in gtemp:
                            ftemp.write(line)
            
            except (EOFError, IOError, OSError, ValueError) as e:
                _remove_tempfile(textfile)
                raise e
            finally:
                _remove_tempfile(gzipfile)
            
            # Keep temp file path.
            self._temp = textfile
            
            # Set handle from text temp file.
            self._handle = io.open(textfile)
            
            # Start with empty buffer; sampled bytes
            # already passed to GZIP temp file.
            self._buffer = ''
        
        # ..otherwise read input as text.
        else:
            
            # Keep null temp path; not used for text file input.
            self._temp = None
            
            # Set text handle from input stream.
            self._handle = io.TextIOWrapper(self._handle)
            
            # Extend buffer until the next line separator,
            # so buffer contains a set of complete lines.
            try:
                sample += next(self._handle)
            except StopIteration: # NB: for very short input.
                pass
            
            # Init buffer from sample.
            self._buffer = sample
    
    def __iter__(self):
        """Get iterator for reader."""
        return iter(self.__next__, None)
    
    def __next__(self):
        """Get next line from reader."""
        
        # EOF
        if self._buffer is None:
            raise StopIteration
        
        try: 
            # Get next line from buffer.
            line, self._buffer = self._buffer.split('\n', 1)
            
        except ValueError: # Buffer lacks newline.
                
            try:
                # Read line from input stream into buffer.
                self._buffer = '{}{}'.format(self._buffer, next(self._handle))
                
                # Get next line from buffer.
                line, self._buffer = self._buffer.split('\n', 1)
                
            except (StopIteration, ValueError): # EOF
                
                # Get last line, flag EOF.
                line, self._buffer = self._buffer, None
        
        return line
    
    def close(self):
        """Close reader."""
        super(TextReader, self).close()
        _remove_tempfile(self._temp)
    
    def next(self):
        """Get next line from reader."""
        return self.__next__()
    
    def read(self, size=None):
        """Read bytes from file."""
        
        if size is not None and not isinstance(size, int):
            raise TypeError("size is not of integer type ~ {!r}".format(size))
        
        # EOF
        if self._buffer is None:
            return ''
        
        # Read from file while size limit not reached.
        while size is None or len(self._buffer) < size:
            try:
                self._buffer += next(self._handle)
            except StopIteration:
                break
        
        # If size is specified and within the extent of the
        # buffer, take chunk of specified size from buffer..
        if size is not None and size <= len(self._buffer):
            chunk, self._buffer = self._buffer[:size], self._buffer[size:]
        # ..otherwise take entire buffer, flag EOF.
        else:
            chunk, self._buffer = self._buffer, None
        
        return chunk
    
    def readline(self, size=None):
        """Read next line from file."""
        
        if size is not None and not isinstance(size, int):
            raise TypeError("size is not of integer type ~ {!r}".format(size))
        
        # EOF
        if self._buffer is None:
            return ''
        
        try:
            # Get next line from buffer.
            line, self._buffer = self._buffer.split('\n', 1)
            
        except ValueError: # Buffer lacks newline.
                
            try:
                # Read line from input stream into buffer.
                self._buffer += '{}{}'.format(self._buffer, next(self._handle))
                
                # Get next line from buffer.
                line, self._buffer = self._buffer.split('\n', 1)
                
            except (StopIteration, ValueError): # EOF
                
                # Get last line, flag EOF.
                line, self._buffer = self._buffer, None
        
        # If applicable, truncate line to specified
        # length, then push excess back to buffer.
        if size is not None and len(line) > size:
            line, excess = line[:size], line[size:]
            self._buffer = '{}{}'.format(excess, self._buffer)
        
        return line
    
    def readlines(self, sizehint=None):
        """Read lines from file."""
        
        if sizehint is not None and not isinstance(sizehint, int):
            raise TypeError("sizehint is not of integer type ~ {!r}".format(sizehint))
        
        # EOF
        if self._buffer is None:
            return []
        
        # Read from file while size hint limit not reached.
        while sizehint is None or len(self._buffer) < sizehint:
            try:
                self._buffer += next(self._handle)
            except StopIteration:
                break
        
        # Split buffer into lines.
        lines = self._buffer.splitlines(keepends=True)
        
        # If size hint is specified and within the extent of the buffer,
        # set buffer to empty string to prepare for any subsequent lines..
        if sizehint is not None and sizehint <= len(self._buffer):
            self._buffer = ''
        # ..otherwise flag EOF.
        else:
            self._buffer = None
         
        return lines

class TextWriter(TextRW):
    """Text writer class."""
    
    def __init__(self, filepath):
        """Init text writer.
         
         If output `filepath` is set to `-`, the new object will write to
         standard output. Otherwise, the specified filepath is opened for
         writing. Output is GZIP-compressed if the specified filepath ends
         with the extension `.gz`.
        
        Args:
            filepath (str): Path of output file.
        """
        
        super(TextWriter, self).__init__()
        
        if not isinstance(filepath, basestring):
            raise TypeError("cannot open filepath of type {!r}".format(type(filepath).__name__))
        
        # Assume uncompressed output.
        compress_output = False
        
        # If filepath indicates standard output,
        # prepare to write to standard output..
        if filepath == '-':
            
            self._name = stdout.name
            self._handle = stdout
            
        # ..otherwise resolve, validate, and open the specified filepath.
        else:
            
            self._closable = True
            
            filepath = resolve_path(filepath)
            self._name = relpath(filepath)
            
            if filepath.endswith('.gz'):
                 compress_output = True
            
            self._handle = io.open(filepath, mode='wb')
        
        if compress_output:
            self._handle = GzipFile(fileobj=self._handle)
    
    def write(self, x):
        """Write string."""
        self._handle.write(x)
    
    def writelines(self, sequence):
        """Write lines."""
        self._handle.writelines(sequence)

################################################################################

def _read_about():
    """Read information about this package."""
    about_file = os.path.join('data', 'about.yaml')
    about_path = resource_filename('gactutil', about_file)
    with open(about_path, 'r') as fh:
        about_info = load(fh)
    return about_info

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

def _remove_tempfile(filepath):
    """Remove the specified temporary file."""
    
    if filepath is not None:
        try:
            if os.path.isfile(filepath):
                os.remove(filepath)
        except OSError:
            warn("failed to remove temp file ~ {!r}".format(filepath), RuntimeWarning)
        except TypeError:
            raise TypeError("failed to remove temp file of type {} ~ {!r}".format(
                type(filepath).__name__, filepath))

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

def _tokenise_source(source):
    """Tokenise source code into token strings."""
    buf = io.BytesIO(source)
    try:
        token_strings = [ x[1] for x in generate_tokens(buf.readline) ]
    except TokenError:
        raise RuntimeError("failed to tokenise source")
    return token_strings

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
        
        if not isinstance(k, _info['mapping_types']):
            raise TypeError("mapping key {!r} has invalid type ~ {!r}".format(str(k), type(k).__name__))
        
        try:
            x = mapping[k]
        except KeyError:
            raise TypeError("mapping has invalid type ~ {!r}".format(type(mapping).__name__))
        
        if not isinstance(x, _info['mapping_types']):
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
def temporary_directory(suffix='', prefix='tmp', name=None, dir=None,
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
        
        # Ensure temp directory exists, and that a
        # pre-existing directory isn't marked for deletion.
        try:
            os.makedirs(twd)
        except OSError as e:
            if e.errno == errno.EEXIST:
                if delete:
                    raise RuntimeError("cannot mark pre-existing temp directory for deletion ~ {!r}".format(twd))
            else:
                raise e
    
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
                warn("failed to delete temp directory ~ {!r}".format(twd),
                    RuntimeWarning)

################################################################################
