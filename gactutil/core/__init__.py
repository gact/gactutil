#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil core module."""

from __future__ import division as _division
from collections import Container as _Container
from collections import Iterator as _Iterator
from collections import Sequence as _Sequence
import contextlib
import datetime
import errno
import functools
import inspect
import operator
import os
import random
import shutil
import string
import sys
import tempfile

################################################################################

# Known immutable scalar types.
_ImmutableScalarTypes = (basestring, bool, float, int, long, type(None),
    datetime.datetime, datetime.date)

# Standard newlines are those most commonly used and supported.
_standard_newlines = (
    u'\r\n',   # CR+LF
    u'\r',     # CR: carriage return
    u'\n'      # LF: line feed
)

# The full set of newlines listed here is taken from the Python function
# "_PyUnicode_IsLinebreak" in file "Python-2.7.11/Objects/unicodetype_db.h".
# Available from: <https://www.python.org/downloads/release/python-2711/>.
_newlines = _standard_newlines + (
    u'\x0b',   # VT: vertical tab
    u'\x0c',   # FF: form feed
    u'\x1c',   # FS: file separator
    u'\x1d',   # GS: group separator
    u'\x1e',   # RS: record separator
    u'\x85',   # NEL: next line
    u'\u2028', # LS: line separator
    u'\u2029'  # PS: paragraph separator
)

_newline_charset = frozenset( char for newline in _newlines for char in newline )

################################################################################

def _flatten(sequence, ndims):
    u"""Recursively flatten regular sequence."""
    
    for _ in range( ndims - 1 ):
        sequence = [ x for child in sequence for x in child ]
    
    return sequence

def _getshape(sequence, memo=None):
    u"""Recursively get shape of regular sequence."""
    
    if isinstance(sequence, basestring):
        return None
    
    if not isinstance(sequence, _Sequence):
        if isinstance(sequence, (_Container, _Iterator)):
            raise TypeError("data is not sequential")
        return None
    
    if memo is None:
        memo = list()
    
    sid = id(sequence)
    if sid in memo:
        raise ValueError("data contains a reference cycle")
    memo.append(sid)
    
    shapes = set([ _getshape(x, memo=memo) for x in sequence ])
    
    memo.pop()
    
    if len(shapes) > 1:
        raise ValueError("data has irregular shape")
    
    try:
        shape = shapes.pop()
    except KeyError: # empty sequence
        shape = (0,)
    else:
        try:
            shape = (len(sequence),) + shape
        except TypeError:
            shape = (len(sequence),)
    
    return shape
    
def _reshape(sequence, shape):
    u"""Recursively reshape flattened sequence."""
    
    reshaped = list()
    
    size = len(sequence)
    step = size // shape[0]
    
    for i in xrange(0, size, step):
        j = i + step
        reshaped.append(sequence[i:j])
    
    subshape = shape[1:]
    
    if len(subshape) > 1:
        reshaped = [ _reshape(x, subshape) for x in reshaped ]
    
    return reshaped

def _reshape_empty(shape):
    u"""Recursively reshape flattened empty sequence."""
    
    if len(shape) >= 2 and shape[1] > 0:
        reshaped = [ _reshape_empty(shape[1:]) for _ in xrange(shape[0]) ]
    else:
        reshaped = [ [] for _ in xrange(shape[0]) ]
    
    return reshaped

################################################################################

def contains_newline(string):
    return isinstance(string, basestring) and set(string) & _newline_charset

@contextlib.contextmanager
def dropped_tempfile():
    u"""Yield a temporary filepath for use in the given context."""
    
    rng = random.SystemRandom()
    
    # Generate random suffixes for temp directory and file.
    dir_suffix = ''.join( rng.choice(string.ascii_letters) for _ in range(16) )
    file_suffix = ''.join( rng.choice(string.ascii_letters) for _ in range(16) )
    
    # Create temp directory into which the temp file will be dropped.
    temp_dir = tempfile.mkdtemp(suffix=dir_suffix)
    
    try:
        # Create dropped temp file in temp directory.
        with tempfile.NamedTemporaryFile(suffix=file_suffix,
            dir=temp_dir, delete=False) as temp:
            filename = temp.name
        
        yield filename
        
    except OSError:
        raise RuntimeError("failed to create dropped tempfile: {!r}".format(filename))
    finally:
        try: # Remove temp directory and dropped temp file.
            shutil.rmtree(temp_dir)
        except OSError:
            warn("failed to delete dropped tempfile: {!r}".format(filename), RuntimeWarning)

def duplicated(iterable):
    u"""Yield duplicate elements of iterable."""
    duplicated = set()
    found = set()
    for x in iterable:
        if x in found:
            duplicated.add(x)
            yield x
        else:
            found.add(x)

def ellipt(string, length, left=False, right=False):
    u"""Ellipt string to given length."""
    
    if not isinstance(string, basestring):
        raise TypeError("expected object of string type, not {!r}".format(
            type(string).__name__))
    
    if not isinstance(length, (int, long)):
        raise TypeError("ellipt length must be of integer type, not {!r}".format(
            type(length).__name__))
    
    ellipsis = '...' # coerced to unicode as necessary
    
    if left and right:
        ellipses_length = 2 * len(ellipsis)
    else:
        ellipses_length = len(ellipsis)
    
    # Get length of original string that
    # will remain after it is ellipted.
    l = length - ellipses_length
    
    if l <= 0:
        raise ValueError("cannot ellipt to string of length {} (min={})".format(
            length, ellipses_length + 1))
    
    # Ellipt string if longer than specified length.
    if len(string) > length:
        
        if left and right:
            
            m, rm = divmod(len(string), 2)
            h, rh = divmod(l, 2)
            i = (m + rm) - (h + rh)
            j = i + l
            
            string = ellipsis + string[i:j] + ellipsis
            
        elif left:
            
            string = ellipsis + string[-l:]
            
        elif right:
            
            string = string[:l] + ellipsis
            
        else:
            
            h, rh = divmod(l, 2)
            i = h + rh
            string = string[:i] + ellipsis + string[-h:]
    
    return string

def flatten(sequence):
    u"""Flatten regular sequence."""
    
    if not isinstance(sequence, _Sequence) or isinstance(sequence, basestring):
        raise TypeError("cannot flatten {} object".format(
            type(sequence).__name__))
    
    ndims = len( _getshape(sequence) )
    
    if ndims > 1:
        sequence = _flatten(sequence, ndims)
    
    return sequence

def fsdecode(string):
    u"""Decode byte strings to unicode with file system encoding.
    
    This function is modelled after its namesake in the Python 3 os.path module.
    """
    if isinstance(string, str):
        return string.decode( sys.getfilesystemencoding() )
    elif isinstance(string, unicode):
        return string
    else:
        raise TypeError("argument is not of string type: {!r}".format(string))

def fsencode(string):
    u"""Encode byte strings from unicode with file system encoding.
    
    This function is modelled after its namesake in the Python 3 os.path module.
    """
    if isinstance(string, unicode):
        return string.encode( sys.getfilesystemencoding() )
    elif isinstance(string, str):
        return string
    else:
        raise TypeError("argument is not of string type: {!r}".format(string))

def getshape(sequence):
    u"""Get shape of regular sequence."""
    
    if not isinstance(sequence, _Sequence) or isinstance(sequence, basestring):
        raise TypeError("cannot get shape of {} object".format(
            type(sequence).__name__))
    
    return _getshape(sequence)

def is_multiline_string(string):
    return isinstance(string, basestring) and len(string.splitlines()) > 1

def is_newline(char):
    return char in _newlines

lellipt = functools.partial(ellipt, left=True, right=False)
lellipt.__doc__ = u"""Left-ellipt string to given length."""

def ltrunc(string, length):
    u"""Left-truncate string to the given length."""
    if not isinstance(string, basestring):
        raise TypeError("expected object of string type, not {!r}".format(
            type(string).__name__))
    if not isinstance(length, (int, long)):
        raise TypeError("truncation length must be of integer type, not {!r}".format(
            type(length).__name__))
    return string[-length:]

def remove_existing(filepath):
    u"""Remove file if it exists."""
    try:
        os.remove(filepath)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise e

def reshape(sequence, shape):
    u"""Reshape regular sequence."""
    
    if not isinstance(sequence, _Sequence) or isinstance(sequence, basestring):
        raise TypeError("cannot reshape {} object".format(
            type(sequence).__name__))
    
    try:
        shape = tuple(shape)
        if len(shape) == 0 or any( not isinstance(x, (int, long)) for x in shape ):
            raise TypeError
        if any( x < 0 for x in shape ):
            raise ValueError
    except TypeError:
        raise TypeError("shape must be a sequence of integers: {!r}".format(shape))
    except ValueError:
        raise ValueError("cannot have negative shape: {!r}".format(shape))
    
    # Get current shape of sequence.
    sequence_shape = _getshape(sequence)
    
    # If sequence already has desired shape, return it.
    if sequence_shape == shape:
        return sequence
    
    # Flatten sequence if multi-dimensional.
    if len(sequence_shape) > 1:
        sequence = _flatten(sequence, len(sequence_shape))
    
    # NB: assumes shape of nonzero length.
    shape_size = functools.reduce(operator.mul, shape, 1)
    
    if len(sequence) != shape_size:
        raise ValueError("cannot reshape sequence of shape {!r} to sequence of shape {!r}".format(
            sequence_shape, shape))
    
    # If desired shape is one-dimensional, return flattened sequence.
    if len(shape) == 1:
        return sequence
    
    if shape_size > 0:
        reshaped = _reshape(sequence, shape)
    else:
        reshaped = _reshape_empty(shape)
    
    return reshaped

def respath(path, start=None):
    u"""Resolve the specified path."""
    b_path = fsencode(path)
    b_path = os.path.realpath( os.path.expandvars(
        os.path.expanduser(b_path) ) )
    if start is not None:
        b_start = fsencode(start)
        b_start = os.path.realpath( os.path.expandvars(
            os.path.expanduser(b_start) ) )
        b_path = os.path.relpath(b_path, b_start)
    return fsdecode(b_path)

rellipt = functools.partial(ellipt, left=False, right=True)
rellipt.__doc__ = u"""Right-ellipt string to given length."""

def rtrunc(string, length):
    u"""Right-truncate string to the given length."""
    if not isinstance(string, basestring):
        raise TypeError("expected object of string type, not {!r}".format(
            type(string).__name__))
    if not isinstance(length, (int, long)):
        raise TypeError("truncation length must be of integer type, not {!r}".format(
            type(length).__name__))
    return string[:length]

@contextlib.contextmanager
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
        twd = respath(twd)
        
        # Ensure temp directory exists, and that a
        # pre-existing directory isn't marked for deletion.
        try:
            os.makedirs(twd)
        except OSError as e:
            if e.errno == errno.EEXIST:
                if delete:
                    raise RuntimeError("cannot mark pre-existing temp directory for deletion: {!r}".format(twd))
            else:
                raise e
    
    # ..otherwise, create temp directory in usual way.
    else:
        twd = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    
    try:
        yield twd
    finally:
        if delete:
            try:
                shutil.rmtree(twd)
            except OSError:
                warn("failed to delete temp directory: {!r}".format(twd), RuntimeWarning)

################################################################################

__all__ = [ name for name, member in inspect.getmembers(sys.modules[__name__])
    if not inspect.ismodule(member) and not name.startswith('_') ] + ['const']

################################################################################
