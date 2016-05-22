#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil module."""

from __future__ import absolute_import
from abc import ABCMeta
from binascii import hexlify
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
import pickle
from pkg_resources import resource_filename
from platform import system
from shutil import rmtree
import sys
from tempfile import mkdtemp
from tempfile import NamedTemporaryFile
from tokenize import generate_tokens
from tokenize import TokenError

################################################################################

_info = {
    
    # Data types that are considered valid as keys or values in a mapping object.
    u'mapping_types': (None, bool, int, float, unicode),
    
    # Config settings filename.
    u'settings-file': 'settings.yaml'
}

################################################################################

class TextRW(object):  
    u"""Abstract text reader/writer base class."""
    __metaclass__ = ABCMeta
    
    @property
    def closed(self):
        u"""bool: True if file is closed; False otherwise."""
        try:
            return self._handle.closed
        except AttributeError:
            return None
    
    @property
    def name(self):
        u"""unicode: Name of specified file or standard file object."""
        return self._name

    @property
    def newlines(self):
        u"""unicode or tuple: Observed newlines."""
        try:
            return self._handle.newlines
        except AttributeError:
            return None
    
    def __init__(self):
        u"""Init text reader/writer."""
        self._closable = False
        self._encoding = None
        self._handle = None
        self._name = None
    
    def __enter__(self):
        u"""TextRW: Get reader/writer on entry to a context block."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        u"""Close reader/writer on exit from a context block."""
        self.close()
        
    def close(self):
        u"""Close reader/writer."""
        if self._closable:
            self._handle.close()

class TextReader(TextRW):
    u"""Text reader class."""
    
    def __init__(self, filepath):
        u"""Init text reader.
         
         If the input `filepath` is `-`, the new object will read from standard
         input. Otherwise, the specified filepath is opened for reading. Input
         that is GZIP-compressed is identified and extracted to a temporary file
         before reading.
        
        Args:
            filepath (unicode): Path of input file.
        """
        
        super(TextReader, self).__init__()
        
        if not isinstance(filepath, unicode):
            raise TypeError("filepath must be of type unicode, not {!r}".format(
                type(filepath).__name__))
        
        # If filepath indicates standard input, 
        # prepare to read from standard input..
        if filepath == u'-':
            
            self._name = sys.stdin.name
            self._handle = io.open(sys.stdin.fileno(), mode='rb')
            
            if sys.stdin.isatty():
                self._encoding = sys.getfilesystemencoding()
            else:
                self._encoding = 'utf_8'
            
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
            
            self._encoding = 'utf_8'
        
        # Assume input is text.
        format = u'text'
        
        # Sample first three bytes.
        sample = self._handle.read(3)
        
        # If sample returned successfully, check if
        # it indicates content is GZIP-compressed.
        if len(sample) == 3:
            magic_bytes = hexlify(sample[:2])
            method = ord(sample[2:])
            if magic_bytes == '1f8b': # NB: magic number for GZIP content.
                if method != 8:
                    raise ValueError("input compressed with unknown GZIP method")
                format = u'gzip'
        
        # If input is GZIP-compressed, extract and then read decompressed text..
        if format == u'gzip':
            
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
            self._handle = io.open(textfile, encoding=self._encoding)
            
            # Start with empty buffer; sampled bytes
            # already passed to GZIP temp file.
            self._buffer = u''
        
        # ..otherwise read input as text.
        else:
            
            # Keep null temp path; not used for text file input.
            self._temp = None
            
            # Set text handle from input stream.
            self._handle = io.TextIOWrapper(self._handle, encoding=self._encoding)
            
            # Extend buffer until the next line separator,
            # so buffer contains a set of complete lines.
            try:
                sample += next(self._handle)
            except StopIteration: # NB: for very short input.
                pass
            
            # Init buffer from sample.
            self._buffer = sample
    
    def __iter__(self):
        u"""Get iterator for reader."""
        return iter(self.__next__, None)
    
    def __next__(self):
        u"""Get next line from reader."""
        
        # EOF
        if self._buffer is None:
            raise StopIteration
        
        try: 
            
            # Get EOL index in buffer.
            i = self._buffer.index('\n') + 1
            
            # Get next line from buffer.
            line, self._buffer = self._buffer[:i], self._buffer[i:]
            
        except ValueError: # Buffer lacks newline.
                
            try:
                # Read line from input stream into buffer.
                self._buffer = u'{}{}'.format(self._buffer, next(self._handle))
                
                # Get EOL index in buffer.
                i = self._buffer.index('\n') + 1
                
                # Get next line from buffer.
                line, self._buffer = self._buffer[:i], self._buffer[i:]
                
            except (StopIteration, ValueError): # EOF
                
                # Get last line, flag EOF.
                line, self._buffer = self._buffer, None
                
                if line == u'':
                    raise StopIteration
        
        return line
    
    def close(self):
        u"""Close reader."""
        super(TextReader, self).close()
        _remove_tempfile(self._temp)
    
    def next(self):
        u"""Get next line from reader."""
        return self.__next__()
    
    def read(self, size=None):
        u"""Read bytes from file."""
        
        if size is not None and not isinstance(size, int):
            raise TypeError("size is not of integer type ~ {!r}".format(size))
        
        # EOF
        if self._buffer is None:
            return u''
        
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
        u"""Read next line from file."""
        
        if size is not None and not isinstance(size, int):
            raise TypeError("size is not of integer type ~ {!r}".format(size))
        
        # EOF
        if self._buffer is None:
            return u''
        
        try:
            # Get next line from buffer.
            line, self._buffer = self._buffer.split(u'\n', 1)
            
        except ValueError: # Buffer lacks newline.
                
            try:
                # Read line from input stream into buffer.
                self._buffer += u'{}{}'.format(self._buffer, next(self._handle))
                
                # Get next line from buffer.
                line, self._buffer = self._buffer.split(u'\n', 1)
                
            except (StopIteration, ValueError): # EOF
                
                # Get last line, flag EOF.
                line, self._buffer = self._buffer, None
        
        # If applicable, truncate line to specified
        # length, then push excess back to buffer.
        if size is not None and len(line) > size:
            line, excess = line[:size], line[size:]
            self._buffer = u'{}{}'.format(excess, self._buffer)
        
        return line
    
    def readlines(self, sizehint=None):
        u"""Read lines from file."""
        
        if sizehint is not None and not isinstance(sizehint, int):
            raise TypeError("sizehint is not of integer type ~ {!r}".format(sizehint))
        
        # EOF
        if self._buffer is None:
            return []
        
        # Ensure buffer ends with complete line.
        self._buffer += next(self._handle)
        
        # Split buffer into lines.
        # NB: buffer is bypassed for remainder of method.
        lines = self._buffer.splitlines(True)
        
        # Get sum of line lengths.
        length_of_lines = sum( len(line) for line in lines )
        
        # Read from file while size hint limit not reached.
        while sizehint is None or len(length_of_lines) < sizehint:
            try:
                line = next(self._handle)
                length_of_lines += len(line)
                lines.append(line)
            except StopIteration:
                break
        
        # If size hint is specified and within the extent of the buffer,
        # set buffer to empty string to prepare for any subsequent lines..
        if sizehint is not None and sizehint <= len(self._buffer):
            self._buffer = u''
        # ..otherwise flag EOF.
        else:
            self._buffer = None
        
        return lines

class TextWriter(TextRW):
    u"""Text writer class."""
    
    def __init__(self, filepath):
        u"""Init text writer.
         
         If output `filepath` is set to `-`, the new object will write to
         standard output. Otherwise, the specified filepath is opened for
         writing. Output is GZIP-compressed if the specified filepath ends
         with the extension `.gz`.
        
        Args:
            filepath (unicode): Path of output file.
        """
        
        super(TextWriter, self).__init__()
        
        if not isinstance(filepath, unicode):
            raise TypeError("filepath must be of type unicode, not {!r}".format(
                type(filepath).__name__))
        
        # Assume uncompressed output.
        compress_output = False
        
        # If filepath indicates standard output,
        # prepare to write to standard output..
        if filepath == u'-':
            
            self._name = sys.stdout.name
            self._handle = sys.stdout
            
            if sys.stdout.isatty():
                self._encoding = sys.getfilesystemencoding()
            else:
                self._encoding = 'utf_8'
            
        # ..otherwise resolve, validate, and open the specified filepath.
        else:
            
            self._closable = True
            
            filepath = resolve_path(filepath)
            self._name = relpath(filepath)
            
            if filepath.endswith(u'.gz'):
                 compress_output = True
            
            self._handle = io.open(filepath, mode='wb')
            
            self._encoding = 'utf_8'
        
        if compress_output:
            self._handle = GzipFile(fileobj=self._handle)
    
    def write(self, x):
        u"""Write string."""
        self._handle.write( x.encode(self._encoding) )
    
    def writelines(self, sequence):
        u"""Write lines."""
        self._handle.writelines([ x.encode(self._encoding) for x in sequence ])

################################################################################

def _read_about():
    u"""Read information about this package."""
    about_file = os.path.join(u'data', u'about.p')
    about_path = resource_filename(u'gactutil', about_file)
    with open(about_path, 'r') as fh:
        about_info = pickle.load(fh)
    return about_info

def _read_setting(key):
    u"""Read a single package setting."""
    info = _read_settings()
    return info[key]

def _read_settings():
    u"""Read package settings file."""
    about = _read_about()
    settings_file = _info[u'settings-file']
    settings_path = os.path.join(about[u'config_dir'], settings_file)
    
    if os.path.isfile(settings_path):
        try:
            with TextReader(settings_path) as fh:
                config_info = uniload(fh)
        except (IOError, YAMLError):
            raise RuntimeError("failed to read package settings file ~ {!r}".format(settings_file))
    else:
        config_info = dict()
    return(config_info)

def _remove_tempfile(filepath):
    u"""Remove the specified temporary file."""
    
    if filepath is not None:
        try:
            if os.path.isfile(filepath):
                os.remove(filepath)
        except OSError:
            warn("failed to remove temp file ~ {!r}".format(filepath), RuntimeWarning)
        except TypeError:
            raise TypeError("failed to remove temp file of type {!r} ~ {!r}".format(
                type(filepath).__name__, filepath))

def _setup_about(setup_info):
    u"""Setup info about package.
    
    Outputs a package data file in YAML format with information about package.
    
    NB: this function should only be called during package setup.
    """
    
    # Validate caller.
    caller_file, caller_func = [ (inspect.stack()[1])[i] for i in (1, 3) ]
    if caller_file != u'setup.py' or caller_func != u'<module>':
        raise RuntimeError("{!r} should only be called during GACTutil "
            "package setup".format(inspect.stack()[0][3]))
    
    # Set keys common to setup info and about info.
    common_keys = (u'name', u'version')
    
    # Set common info from setup info.
    about_info = { k: setup_info[k] for k in common_keys }
    
    # Set config directory path for this platform.
    platform_system = system()
    if platform_system in ('Linux', 'Darwin'):
        home = expanduser(u'~')
        if platform_system == 'Linux':
            about_info['config_dir'] = os.path.join(home, u'.config', u'gactutil')
        elif platform_system == 'Darwin':
            about_info['config_dir'] = os.path.join(home, u'Library',
            u'Application Support', u'GACTutil')
    elif platform_system == 'Windows':
        appdata = os.getenv('APPDATA')
        about_info[u'config_dir'] = os.path.join(appdata, u'GACTutil')
        if appdata is None or not os.path.isdir(appdata):
            raise RuntimeError("valid %APPDATA% not found")
    else:
        raise RuntimeError("unrecognised platform ~ {!r}".format(platform_system))
    
    # Ensure data directory exists.
    data_dir = os.path.join(u'gactutil', u'data')
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)
    
    # Write info about package.
    about_file = os.path.join(data_dir, u'about.p')
    with open(about_file, 'w') as fh:
        pickle.dump(about_info, fh)

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
        raise TypeError("truncation object must be of type 'string'")
    if not isinstance(length, int):
        raise TypeError("truncation length must be of type 'int'")
    if length < 0:
        raise ValueError("invalid truncation length ~ {!r}".format(length))
    
    template = u'{}...' if isinstance(s, unicode) else '{}...'
    return s if len(s) <= length else template.format(s[:(length-3)])

def _validate_mapping(mapping):
    u"""Validate a mapping object."""
    for k in mapping:
        
        if not isinstance(k, _info[u'mapping_types']):
            raise TypeError("mapping key {!r} has invalid type ~ {!r}".format(
                unicode(k), type(k).__name__))
        
        try:
            x = mapping[k]
        except KeyError:
            raise TypeError("mapping has invalid type ~ {!r}".format(
                type(mapping).__name__))
        
        if not isinstance(x, _info[u'mapping_types']):
            raise TypeError("mapping value {!r} has invalid type ~ {!r}".format(
                unicode(x), type(x).__name__))

def _write_setting(key, value):
    u"""Write a single package setting."""
    info = _read_settings_file()
    info[key] = value    
    _write_settings_file(info, path)

def _write_settings(config_info):
    u"""Write package settings file."""
    about = _read_about()
    settings_file = _info[u'settings-file']
    settings_path = os.path.join(about[u'config_dir'], settings_file)
    try:
        with TextWriter(settings_path) as fh:
            unidump(config_info, fh, default_flow_style=False, width=sys.maxint)
    except (IOError, YAMLError):
        raise RuntimeError("failed to write package settings file ~ {!r}".format(settings_file))

################################################################################

def fsdecode(s):
    """Decode byte strings to unicode with file system encoding.
    
    This function is modelled after its namesake in the Python 3 os.path module.
    """
    if isinstance(s, str):
        return s.decode( sys.getfilesystemencoding() )
    elif isinstance(s, unicode):
        return s
    else:
        raise TypeError("argument is not of string type ~ {!r}".format(s))

def fsencode(s):
    """Encode byte strings from unicode with file system encoding.
    
    This function is modelled after its namesake in the Python 3 os.path module.
    """
    if isinstance(s, unicode):
        return s.encode( sys.getfilesystemencoding() )
    elif isinstance(s, str):
        return s
    else:
        raise TypeError("argument is not of string type ~ {!r}".format(s))

def resolve_path(path, start=None):
    u"""Resolve the specified path.
    
    By default, the specified path is modified by expanding the home directory
    and any environment variables, resolving symbolic links, and returning the
    resulting absolute path. If a `start` path is specified, the resolved path
    is given relative to `start`.
    
    Args:
        path (unicode): A system path.
        start (unicode): Optional starting point for the resolved path.
    
    Returns:
        unicode: Resolved system path.
    """
    if not isinstance(path, unicode):
        raise TypeError("path must be of type unicode, not {!r}".format(
            type(path).__name__))
    resolved_path = realpath( expandvars( expanduser(path) ) )
    if start is not None:
        start = realpath( expandvars( expanduser(start) ) )
        resolved_path = relpath(resolved_path, start)
    return resolved_path

def resolve_paths(paths, start=None):
    u"""Resolve the specified paths.
    
    By default, the specified paths are modified by expanding the home directory
    and any environment variables, resolving symbolic links, and returning the
    resulting absolute path for each input path. If a `start` path is specified,
    the resolved paths are given relative to `start`.
    
    Args:
        paths (list): System paths.
        start (unicode): Optional starting point for the resolved paths.
    
    Returns:
        dict: Mapping of input paths to their resolved form.
    """
    resolved_paths = dict()
    for path in paths:
        resolved_paths[path] = resolve_path(path, start=start)
    return resolved_paths

@contextmanager
def temporary_directory(suffix=u'', prefix=u'tmp', name=None, dir=None,
    delete=True):
    u"""Create temporary directory."""
    
    # If a temp directory name was specified, ensure it exists..
    if name is not None:
        
        # Verify temp directory name is a valid pathname component.
        if os.path.split(name)[0] != u'':
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
