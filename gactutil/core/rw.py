#!/usr/bin/python -tt
# -*- coding: utf-8 -*-
u"""GACTutil IO module."""

from __future__ import absolute_import
from abc import ABCMeta
from binascii import hexlify
from gzip import GzipFile
import io
import os
import re
import sys
from tempfile import NamedTemporaryFile

from gactutil.core import fsdecode
from gactutil.core import respath
from gactutil.core.config import config
from gactutil import _standard_newlines

class _TextRW(object):  
    u"""Abstract text reader/writer base class."""
    
    __metaclass__ = ABCMeta
    
    _newline_regex = {
        None:   re.compile(u'(\r\n|\r|\n)'),
        u'':     re.compile(u'(\r\n|\r|\n)'),
        u'\r\n': re.compile(u'(\r\n)'),
        u'\r':   re.compile(u'(\r)'),
        u'\n':   re.compile(u'(\n)')
    }
    
    @staticmethod
    def _remove_tempfile(filepath):
        u"""Remove the specified temporary file."""
        
        if filepath is not None:
            try:
                if os.path.isfile(filepath):
                    os.remove(filepath)
            except OSError:
                warn("failed to remove temp file: {!r}".format(filepath), RuntimeWarning)
            except TypeError:
                raise TypeError("failed to remove temp file: {!r}".format(filepath))
    
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
    
    def __init__(self, newline=None):
        u"""Init text reader/writer."""
        self._closable = False
        self._encoding = None
        self._handle = None
        self._name = None
        
        if newline is not None and newline not in ('',) + _standard_newlines:
            raise ValueError("invalid/unsupported newline: {!r}".format(newline))
        self._newline = newline
    
    def __enter__(self):
        u"""Get reader/writer on entry to a context block."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        u"""Close reader/writer on exit from a context block."""
        self.close()
        
    def close(self):
        u"""Close reader/writer."""
        if self._closable:
            self._handle.close()
    
    def fileno(self):
        return self._handle.fileno()

class TextReader(_TextRW):
    u"""Text reader class."""
    
    def __init__(self, filepath, newline=None):
        u"""Init text reader.
         
         If the input `filepath` is `-`, the new object will read from standard
         input. Otherwise, the specified filepath is opened for reading. Input
         that is GZIP-compressed is identified and extracted to a temporary file
         before reading.
        
        Args:
            filepath (unicode): Path of input file.
        """
        
        super(TextReader, self).__init__(newline=newline)
        
        filepath = fsdecode(filepath)
        
        # If filepath indicates standard input, 
        # prepare to read from standard input..
        if filepath == u'-':
            
            self._name = sys.stdin.name
            self._handle = io.open(sys.stdin.fileno(), mode='rb', closefd=False)
            
            if sys.stdin.isatty():
                self._encoding = sys.getfilesystemencoding()
            else:
                self._encoding = 'utf_8'
            
        # ..otherwise resolve, validate, and open the specified filepath.
        else:
            
            self._closable = True
            
            filepath = respath(filepath)
            self._name = os.path.relpath(filepath)
            
            if not os.path.exists(filepath):
                raise IOError("file not found: {!r}".format(self._name))
            elif not os.path.isfile(filepath):
                raise IOError("not a file: {!r}".format(self._name))
            
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
                _TextRW._remove_tempfile(textfile)
                raise e
            finally:
                _TextRW._remove_tempfile(gzipfile)
            
            # Keep temp file path.
            self._temp = textfile
            
            # Set handle from text temp file.
            self._handle = io.open(textfile, encoding=self._encoding,
                newline=self._newline)
            
            # Start with empty buffer; sampled bytes
            # already passed to GZIP temp file.
            self._buffer = u''
        
        # ..otherwise read input as text.
        else:
            
            # Keep null temp path; not used for text file input.
            self._temp = None
            
            # Set text handle from input stream.
            self._handle = io.TextIOWrapper(self._handle,
                encoding=self._encoding, newline=self._newline)
            
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
        
        newline_regex = self.__class__._newline_regex[self._newline]
        
        try: 
            # Search for EOL in buffer.
            m = newline_regex.search(self._buffer)
            
            # Get EOL index in buffer.
            i = m.end()
            
            # Get next line from buffer.
            line, self._buffer = self._buffer[:i], self._buffer[i:]
            
        except AttributeError: # Buffer lacks newline.
                
            try:
                # Read line from input stream into buffer.
                self._buffer = u'{}{}'.format(self._buffer, next(self._handle))
                
                # Search for EOL in buffer.
                m = newline_regex.search(self._buffer)
                
                # Get EOL index in buffer.
                i = m.end()
                
                # Get next line from buffer.
                line, self._buffer = self._buffer[:i], self._buffer[i:]
                
            except (StopIteration, AttributeError): # EOF
                
                # Get last line, flag EOF.
                line, self._buffer = self._buffer, None
                
                if line == u'':
                    raise StopIteration
        
        return line
    
    def close(self):
        u"""Close reader."""
        super(TextReader, self).close()
        _TextRW._remove_tempfile(self._temp)
    
    def next(self):
        u"""Get next line from reader."""
        return self.__next__()
    
    def read(self, size=None):
        u"""Read bytes from file."""
        
        if size is not None and not isinstance(size, int):
            raise TypeError("size is not of integer type: {!r}".format(size))
        
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
            raise TypeError("size is not of integer type: {!r}".format(size))
        
        # EOF
        if self._buffer is None:
            return u''
        
        newline_regex = self.__class__._newline_regex[self._newline]
        
        try:
            
            # Search for EOL in buffer.
            m = newline_regex.search(self._buffer)
            
            # Get EOL index in buffer.
            i = m.end()
            
            # Get next line from buffer.
            line, self._buffer = self._buffer[:i], self._buffer[i:]
            
        except AttributeError: # Buffer lacks newline.
                
            try:
                # Read line from input stream into buffer.
                self._buffer += u'{}{}'.format(self._buffer, next(self._handle))
                
                # Search for EOL in buffer.
                m = newline_regex.search(self._buffer)
                
                # Get EOL index in buffer.
                i = m.end()
                
                # Get next line from buffer.
                line, self._buffer = self._buffer[:i], self._buffer[i:]
                
            except (StopIteration, AttributeError): # EOF
                
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
            raise TypeError("sizehint is not of integer type: {!r}".format(sizehint))
        
        # EOF
        if self._buffer is None:
            return []
        
        # Ensure buffer ends with complete line.
        try:
            self._buffer += next(self._handle)
        except StopIteration:
            pass
        
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

class TextWriter(_TextRW):
    u"""Text writer class."""
    
    def __init__(self, filepath, newline=None):
        u"""Init text writer.
         
         If output `filepath` is set to `-`, the new object will write to
         standard output. Otherwise, the specified filepath is opened for
         writing. Output is GZIP-compressed if the specified filepath ends
         with the extension `.gz`.
        
        Args:
            filepath (unicode): Path of output file.
        """
        
        super(TextWriter, self).__init__(newline=newline)
        
        if self._newline is None:
            self._newline = config[u'default'][u'newline']
        
        filepath = fsdecode(filepath)
        
        # Assume uncompressed output.
        compress_output = False
        
        # If filepath indicates standard output,
        # prepare to write to standard output..
        if filepath == u'-':
            
            self._name = sys.stdout.name
            self._handle = io.open(sys.stdout.fileno(), mode='wb', closefd=False)
            
            if sys.stdout.isatty():
                self._encoding = sys.getfilesystemencoding()
            else:
                self._encoding = 'utf_8'
            
        # ..otherwise resolve, validate, and open the specified filepath.
        else:
            
            self._closable = True
            
            filepath = respath(filepath)
            self._name = os.path.relpath(filepath)
            
            if filepath.endswith(u'.gz'):
                 compress_output = True
            
            self._handle = io.open(filepath, mode='wb')
            
            self._encoding = 'utf_8'
        
        if compress_output:
            self._handle = GzipFile(fileobj=self._handle)
    
    def write(self, x):
        u"""Write string."""
        
        if self._newline != u'':
            x = x.replace(u'\n', self._newline)
        
        x = x.encode(self._encoding)
        
        self._handle.write(x)
    
    def writelines(self, sequence):
        u"""Write lines."""
        
        for line in sequence:
            
            if self._newline != u'':
                line = line.replace(u'\n', self._newline)
            
            line = line.encode(self._encoding)
            
            self._handle.write(line)

################################################################################

__all__ = ['TextReader', 'TextWriter']

################################################################################
