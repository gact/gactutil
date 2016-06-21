#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil table data module."""

from __future__ import absolute_import
from collections import Callable
from collections import Iterable
from collections import Mapping
from collections import MutableSequence
from collections import namedtuple
from collections import Sequence
import copy
import csv
from itertools import izip
from operator import itemgetter
from StringIO import StringIO

from gactutil.core import _ImmutableScalarTypes
from gactutil.core import _newline_charset
from gactutil.core import getshape
from gactutil.core import reshape

class table(csv.Dialect):
    delimiter = ','
    quotechar = '"'
    doublequote = False
    escapechar = '\\'
    skipinitialspace = False
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL

################################################################################

class _TableHeadings(MutableSequence):
    u"""Table headings class."""
    
    def __init__(self, headings=None, size=None):
        
        if headings is not None:
            
            try:
                headings = list(headings)
            except TypeError:
                raise TypeError("headings are not iterable")
        
        if size is not None:
            
            if not isinstance(size, (int, long)):
                raise TypeError("{} object size must be an integer, not {!r}".format(
                    self.__class__.__name__, size))
            if size < 0:
                raise ValueError("{} object size is a negative integer: {!r})".format(
                    self.__class__.__name__, size))
            if headings is not None and len(headings) != size:
                raise ValueError("number of headings ({}) does not match specified {} object size ({})".format(
                    len(headings), self.__class__.__name__, size))
        
        if headings is None and size is None:
            headings = ()
            size = 0
        elif headings is None:
            headings = [None] * size
        elif size is None:
            size = len(headings)
        
        # Set default headings to unicode string of the given column index.
        headings = [ heading if heading is not None else unicode(i)
            for i, heading in enumerate(headings) ]
        
        self._hmap = dict()
        self._data = [None] * size
        
        for i, heading in enumerate(headings):
            self._set_heading(i, heading)
  
    def __contains__(self, value):
        return value in self._hmap
  
    def __delitem__(self, key, value):       
        raise TypeError("headings cannot be resized")
        
    def __eq__(self, other):
        return type(self) is type(other) and self._data == other._data
        
    def __getitem__(self, key, value):
    
        if isinstance(key, (int, long)):
            
            if key < -len(self._data) or key >= len(self._data):
                raise IndexError("heading index ({!r}) out of range".format(
                    self.__class__.__name__, key))
            
            item = self._data[key]
        
        elif isinstance(key, basestring):
        
            try:
                i = self._hmap[key]
                item = self._data[i]
            except KeyError:
                raise KeyError("heading not found ({!r})".format(heading))
            
        elif isinstance(key, slice):
            
            indices = key.indices( len(self._data) )
            item = tuple( self._data[i] for i in indices )
            
        else:
            raise TypeError("heading index/key is of invalid type ({})".format(
                type(key).__name__))
    
        return item
        
    def __iter__(self):
        return iter(self._data)
    
    def __len__(self):
        return len(self._data)

    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._data)[1:-1])
    
    def __reversed__(self):
        return reversed(self._data)
    
    def __setitem__(self, key, value):
        
        if isinstance(key, (int, long)):
            
            if key < -len(self._data) or key >= len(self._data):
                raise IndexError("heading index ({!r}) out of range".format(key))
            
            self._set_heading(key, value)
            
        elif isinstance(key, basestring):
            
            i = self.index(key)
            self._set_heading(i, value)
            
        elif isinstance(key, slice):
            
            if not isinstance(value, Sequence) or isinstance(value, basestring):
                raise TypeError("headings slice assignment takes a sequence")

            indices = key.indices( len(self._data) )
            
            if len(value) != len(indices):
                raise TypeError("headings cannot be resized")
            
            for i, heading in zip(indices, value):
                self._set_heading(i, heading)
            
        else:
            raise TypeError("heading index/key is of invalid type ({})".format(
                type(key).__name__))
    
    def __str__(self):
        return unicode(self._data).encode('utf_8')
    
    def __unicode__(self):
        return unicode(self._data)
  
    def _set_heading(self, index, heading):
        
        if not isinstance(heading, basestring):
            raise TypeError("heading must be of string type, not {!r}".format(
                type(heading).__name__))
        
        if set(heading) & _newline_charset:
            raise ValueError("heading is invalid - contains newline(s): {!r}".format(
                heading))
        
        try:
            column_index = int(heading)
        except ValueError:
            pass
        else:
            if column_index != index:
                raise ValueError("numeric heading ({!r}) does not match column index ({})".format(
                    heading, index))
        
        if heading in self._hmap and index != self._hmap[heading]:
            raise ValueError("duplicate heading: {!r}".format(heading))
        
        if self._data[index] in self._hmap:
            del self._hmap[ self._data[index] ]
        
        self._data[index] = heading
        self._hmap[heading] = index
  
    def append(self, value):
        raise TypeError("headings cannot be resized")
  
    def extend(self, values):
        raise TypeError("headings cannot be resized")
 
    def index(self, heading):
        try:
            return self._hmap[heading]
        except KeyError:
            pass
        raise KeyError("heading not found ({!r})".format(heading))
 
    def insert(self, index, value):       
        raise TypeError("headings cannot be resized")

    def pop(self):
        raise TypeError("headings cannot be resized")

    def remove(self, label):
        raise TypeError("headings cannot be resized")

    def reverse(self):
        raise TypeError("headings cannot be reordered")

    def sort(self):
        raise TypeError("headings cannot be reordered")
        
    def to_list(self):
        return [ x if not x.isdigit() else None for x in self._data ]

class _TableIndexer(object):
    u"""Class for indexing into a Table object."""
    
    _dimnames = ('row', 'column')
    
    @property
    def column(self):
        return self._spec.column

    @property
    def row(self):
        return self._spec.row
    
    @classmethod
    def _get_ellipsis_spec(cls, table, d):
        
        length = len(table) if d == 0 else table.width
        
        _start = _min = 0
        _stop = _size = _span = length
        _step = 1
        _index = None
        _indices = xrange(_start, _stop, _step)
        _slc = slice(_start, _stop, _step)
        _last = _max = length - 1
        
        return _IndexerSpec(_start, _stop, _step, _index, _indices, _slc,
            _last, _max, _min, _size, _span)
    
    @classmethod
    def _get_index_spec(cls, table, d, key):
        
        if d == 0:
            index = table._resolve_row_index(key)
        elif d == 1:
            index = table._resolve_column_index(key)
        
        _start = _last = _min = _max = index
        _stop = index + 1
        _step = _size = _span = 1
        _indices = xrange(_start, _stop, _step)
        _index = index
        _slc = slice(_start, _stop, _step)
        
        return _IndexerSpec(_start, _stop, _step, index, _indices, _slc,
            _last, _max, _min, _size, _span)
    
    @classmethod
    def _get_slice_spec(cls, table, d, slc):
        
        start, stop, step = slc.start, slc.stop, slc.step
        if not all( isinstance(x, (int, long, type(None)) ) 
            for x in (start, stop, step) ):
            raise TypeError("{} {} slice attributes must be integer or None".format(
                table.__class__.__name__, cls._dimnames[d]))
        
        length = len(table) if d == 0 else table.width
        
        if step is not None:
            if step == 0:
                raise ValueError("{} {} slice step cannot be zero".format(
                    table.__class__.__name__, cls._dimnames[d]))
        else:
            step = 1
 
        if start is not None:
            if start < -length or start > length:
                raise IndexError("{} {} slice start ({}) out of range".format(
                    table.__class__.__name__, cls._dimnames[d], start))
            if start < 0:
                start += length
        else:
            start = 0 if step > 0 else length - 1
        
        if stop is not None:
            if stop < -length or stop > length:
                raise IndexError("{} {} slice stop ({}) out of range".format(
                    table.__class__.__name__, cls._dimnames[d], stop))
            if stop < 0:
                stop += length
            if step == 1 and start > stop:
                stop = start
        else:
            stop = length if step > 0 else -1
        
        step_quotient, step_remainder = divmod( abs(stop - start), abs(step) )
    
        if step_remainder:
            if step > 0:
                last = stop - step_remainder
            else:
                last = stop + step_remainder
            size = step_quotient + 1
        else:
            last = stop - step
            size = step_quotient
        
        _min, _max = sorted([start, last])
        _span = _max - _min + 1
        _index = None
        _indices = xrange(start, stop, step)
        
        return _IndexerSpec(start, stop, step, _index, _indices, slc,
            last, _max, _min, size, _span)
    
    def __init__(self, table, keys):
        
        if not isinstance(table, Table):
            raise TypeError("expected object of type {!r}, not {!r}".format(
                Table.__name__, table.__class__.__name__))
        
        try: # Split key into row and column keys.
            row_key, col_key = keys
        except ValueError:
            if len(keys) > 2:
                raise ValueError("too many {} indices/keys".format(
                    table.__class__.__name__))
            else:
                raise ValueError("{} object must be accessed by double-indexing".format(
                    table.__class__.__name__))
        except TypeError:
            if isinstance(keys, (basestring, int, long)):
                raise ValueError("{} object must be accessed by double-indexing".format(
                    table.__class__.__name__))
            else:
                raise TypeError("{} index/key is of invalid type ({})".format(
                    self.__class__.__name__, type(keys).__name__))
        
        info = [None] * 2
        
        for d, key in enumerate(keys):
            
            if isinstance(key, (int, long, basestring)):
                
                info[d] = self.__class__._get_index_spec(table, d, key)
                
            elif isinstance(key, slice):
                
                info[d] = self.__class__._get_slice_spec(table, d, key)
                
            elif isinstance(key, type(Ellipsis)):
                
                info[d] = self.__class__._get_ellipsis_spec(table, d)
                
            else:
                raise TypeError("invalid {} index/key ({})".format(
                    table.__class__.__name__, repr(key)))
        
        self._spec = _IndexerPair(*info)

_IndexerPair = namedtuple('IndexerPair', _TableIndexer._dimnames)
_IndexerSpec = namedtuple('IndexerSpec', ('start', 'stop', 'step', 'index',
    'indices', 'slice', 'last', 'max', 'min', 'size', 'span'))

################################################################################

class Table(MutableSequence):
    u"""Table class.
    
    A Table is a sequence of regular rows, which can be indexed by column
    heading in addition to row and column indices.
    """
    
    _hdg_type = _TableHeadings
    _seq_type = list
    
    @property
    def shape(self):
        return (len(self._data), len(self.headings))
    
    @property
    def width(self):
        return len(self.headings)
    
    @classmethod
    def from_dict(cls, data):
        u"""Get Table from dict."""
        
        if not isinstance(data, Mapping):
            raise TypeError("data must be of mapping type, not {!r}".format(
                type(data).__name__))
        
        headings = sorted( data.keys() )
        
        col_length = None
        
        for x in data.values():
            
            try:
                assert not isinstance(x, basestring)
                assert isinstance(x, Sequence)
                
                row_count = len(x)
                
            except (AssertionError, TypeError):
                raise TypeError("dict data column must be a sized sequence, not {!r}".format(
                    type(x).__name__))
            
            if col_length is None:
                col_length = row_count
            elif row_count != col_length:
                raise ValueError("dict data columns have inconsistent lengths")
        
        return cls(data=[ x for x in izip(*[ data[k]
            for k in headings ]) ], headings=headings)
    
    def __init__(self, data=None, headings=None):
        
        shape = getshape(data)
        
        if len(shape) not in (1, 2):
            raise ValueError("{} data must have 2 dimensions, not {}".format(
                self.__class__.__name__, len(shape)))
        
        if headings is not None:
            
            self.headings = self.__class__._hdg_type(headings=headings)
            
            num_headings = len(self.headings)
            
            if len(shape) == 2:
                
                row_count, row_width = shape
                
                if row_width != num_headings:
                    raise ValueError("{} data row width ({}) does not match number of headings ({})".format(
                        self.__class__.__name__, row_width, num_headings))
            else:
                
                try:
                    row_count, remainder = divmod(len(data), num_headings)
                except ZeroDivisionError:
                    remainder = len(data)
                    row_count = 0
                    
                if remainder:
                    raise ValueError("{} data size ({}) is not a multiple of number of headings ({})".format(
                        self.__class__.__name__, len(data), num_headings))
                
                row_width = num_headings
        else:
            
            if len(shape) == 2:
                row_count, row_width = shape
            else:
                row_count = 1 if len(data) > 0 else 0
                row_width = len(data)
            
            self.headings = self.__class__._hdg_type(size=row_width)
        
        if len(shape) == 1:
            data = reshape(data, (row_count, row_width))
        
        seq = self.__class__._seq_type
        
        table = seq( seq( x for x in row ) for row in data )
        
        self._validate_table(table)
        
        self._data = table
    
    def __contains__(self, item):
        return any( item in row for row in self._data )
    
    def __delitem__(self, keys):
        
        idx = _TableIndexer(self, keys)
        
        deletion_length = idx.row.size
        deletion_width = idx.column.size
        
        if deletion_length > 0 or deletion_width > 0:
            
            if deletion_width == len(self.headings):
                
                rows = range(idx.row.max, idx.row.min - 1, -abs(idx.row.step))
                
                for r in rows:
                    del self._data[r]
                
            elif deletion_length == len(self._data):
                
                raise TypeError("{} object cannot delete columns".format(
                    self.__class__.__name__))
                
            else:
                
                raise TypeError("{} object cannot delete partial rows/columns".format(
                    self.__class__.__name__))
    
    def __getitem__(self, keys):
        
        idx = _TableIndexer(self, keys)
        
        try:
            
            value = self._data[idx.row.index][idx.column.index]
            
        except TypeError:
            
            seq = self.__class__._seq_type
            column_index = idx.column.index
            row_index = idx.row.index
            
            if column_index is not None:
                
                row_indices = idx.row.indices
                value = seq( self._data[row_index][column_index]
                    for row_index in row_indices )
            
            elif row_index is not None:
                
                column_slice = idx.column.slice
                value = self._data[row_index][column_slice]
                
            else:
                
                column_slice = idx.column.slice
                row_indices = idx.row.indices
                value = seq( self._data[row_index][column_slice]
                    for row_index in row_indices )
        
        return value
    
    def __iter__(self):
        for row in self._data:
            yield row
    
    def __len__(self):
        return len(self._data)
    
    def __repr__(self):
        
        if len(self._data) > 0:
            return '{}()'.format(self.__class__.__name__)
        
        seq = self.__class__._seq_type
        rows = [ repr(seq(self.headings)) ] + [
            repr(row) for row in self._data ]
        
        return '{}( {} )'.format(self.__class__.__name__, ', '.join(rows))
    
    def __reversed__(self):
        for row in reversed(self._data):
            yield row
    
    def __setattr__(self, name, value):
        
        if name == 'headings':
            
            if type(value) != self.__class__._hdg_type:
                raise TypeError("{} headings must be of type {!r}, not {!r}".format(
                    self.__class__.__name__, self.__class__._hdg_type.__name__,
                    type(value).__name__))
            
            num_columns = self._get_data_width()
            num_headings = len(value)
            
            if hasattr(self, '_data') and num_headings != num_columns:
                raise ValueError("number of {} headings ({}) must match number of columns ({})".format(
                    self.__class__.__name__, num_headings, num_columns))
            
        elif hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        
        self.__dict__[name] = value
    
    def __setitem__(self, keys, value):
        
        idx = _TableIndexer(self, keys)
        
        column_index = idx.column.index
        row_index = idx.row.index
        
        if row_index is not None and column_index is not None:
            
            self._validate_element(value)
            self._data[row_index][column_index] = value
            
        else:
            
            seq = self.__class__._seq_type
            
            table_length, table_width = len(self._data), len(self.headings)
            indexed_length, indexed_width = idx.row.size, idx.column.size
            
            if isinstance(value, Table):
                shape = value.shape
            else:
                shape = getshape(value)
            
            if len(shape) not in (1, 2):
                raise ValueError("attempt to assign data of {} dimensions".format(
                    len(shape)))
            
            if len(shape) == 1:
                
                if len(value) == indexed_length * indexed_width:
                    shape = (indexed_length, indexed_width)
                    value = reshape(value, shape)
                else:
                    raise ValueError("attempt to assign data of size {!r} to slice of shape {!r}".format(
                        len(value), (indexed_length, indexed_width)))
            
            if not isinstance(value, Table):
                self._validate_table(value)
            
            value_length, value_width = shape
            
            if value_length == indexed_length and value_width == indexed_width:
                
                for r, i in izip(idx.row.indices, xrange(value_length)):
                    for c, j in izip(idx.column.indices, xrange(value_width)):
                        self._data[r][c] = value[i][j]
                
            elif value_width == indexed_width == table_width and idx.row.step == 1:
                
                self._data[idx.row.slice] = seq( x for x in value )
                
            elif value_length == indexed_length == table_length and idx.column.step == 1:
                
                for i, r in enumerate(idx.row.indices):
                    self._data[r][idx.column.slice] = seq(value[i])
                
                if isinstance(value, Table):
                    value_headings = value.headings
                else:
                    value_headings = [None] * value_width
                
                table_headings = self.headings.to_list()
                table_headings[idx.column.slice] = value_headings
                self.headings = self.__class__._hdg_type(table_headings)
                
            elif value_length == indexed_length:
                
                attempted = 'insert' if value_width > indexed_width else 'delete'
                raise TypeError("{} object cannot {} partial columns".format(
                    self.__class__.__name__, attempted))
                
            elif value_width == indexed_width:
                
                attempted = 'insert' if value_length > indexed_length else 'delete'
                raise TypeError("{} object cannot {} partial rows".format(
                    self.__class__.__name__, attempted))
                
            elif idx.row.step != 1 or idx.column.step != 1:
                
                raise ValueError("attempt to assign data of shape {!r} to "
                    "extended slice of shape {!r}".format(
                    shape, (indexed_length, indexed_width)))
                
            else:
                
                raise ValueError("attempt to assign data of shape {!r} to "
                    "slice of shape {!r}".format(
                    shape, (indexed_length, indexed_width)))
    
    def __str__(self):
        
        sh = StringIO()
        writer = csv.writer(sh, dialect=table)
        
        writer.writerow( list(self.headings) )
        
        for row in self._data:
            row = [ x.encode('utf_8') if isinstance(x, unicode)
                else str(x) for x in row ]
            writer.writerow(row)
        
        result = sh.getvalue()
        sh.close()
        
        return result
    
    def __unicode__(self):
        return str(self).decode('utf_8')
    
    def _get_data_width(self):
        u"""Get width of table data (i.e. length of rows)."""
        
        widths = set([ len(row) for row in self._data ])
        
        if len(widths) > 1:
            raise ValueError("{} object has irregular shape".format(
                self.__class__.__name__))
        
        try:
            width = widths.pop()
        except KeyError: # empty table
            width = 0
        
        return width
    
    def _resolve_column_index(self, key):
        
        if isinstance(key, basestring):
            
            index = self.headings.index(key)
            
        elif isinstance(key, (int, long)):
            
            length = len(self.headings)
            index = key
            
            if index < -length or index >= length:
                raise IndexError("{} column index ({!r}) out of range".format(
                    self.__class__.__name__, index))
            
            if index < 0:
                index += length
            
        else:
            raise TypeError("cannot resolve {} column index from object of type {!r}".format(
                self.__class__.__name__, type(key).__name__))
            
        return index
    
    def _resolve_row_index(self, key):
        
        if isinstance(key, (int, long)):
            
            length = len(self._data)
            index = key
            
            if index < -length or index >= length:
                raise IndexError("{} row index ({!r}) out of range".format(
                    self.__class__.__name__, index))
            
            if index < 0:
                index += length
            
        else:
            raise TypeError("cannot resolve {} row index from object of type {!r}".format(
                self.__class__.__name__, type(key).__name__))
            
        return index
    
    def _validate_element(self, x):
        
        if isinstance(x, basestring):
            if set(x) & _newline_charset:
                raise ValueError("{} element is invalid - contains newline(s): {!r}".format(
                    self.__class__.__name__, x))
        elif not isinstance(x, _ImmutableScalarTypes):
            raise TypeError("{} element is invalid - unknown or non-scalar type: {!r}".format(
                self.__class__.__name__, type(x).__name__))
    
    def _validate_table(self, data):
        
        data = set( x for row in data for x in row )
        
        for x in data:
            if isinstance(x, basestring):
                if set(x) & _newline_charset:
                    raise ValueError("{} element is invalid - contains newline(s): {!r}".format(
                        self.__class__.__name__, x))
            elif not isinstance(x, _ImmutableScalarTypes):
                raise TypeError("{} element is invalid - unknown or non-scalar type: {!r}".format(
                    self.__class__.__name__, type(x).__name__))
    
    def append(self, row):
        self[len(self._data):, ...] = [row]
    
    def columns(self):
        seq = self.__class__._seq_type
        for column in izip(*self._data):
            yield seq(column)
    
    def count(self, value):
        return sum( row.count(value) for row in self._data )
        
    def extend(self, rows):
        self[len(self._data):, ...] = rows
    
    def hindex(self, key):
        u"""Get column index corresponding to the given heading."""
        if not isinstance(key, basestring):
            raise TypeError("cannot resolve {} heading index from object of type {!r}".format(
                self.__class__.__name__, type(key).__name__))
        return self.headings.index(key)
    
    def index(self, value):
        for r, row in enumerate(self._data):
            for c, x in enumerate(row):
                if x == value:
                    return (r, c)
        raise ValueError("{!r} is not in {}".format(value, self.__class__.__name__))
    
    def insert(self, *args, **kwargs):
        raise TypeError("{} object does not support the insert operation".format(
            self.__class__.__name__))
        
    def pop(self):
        try:
            return self._data.pop()
        except IndexError:
            raise IndexError("pop from empty {}".format(self.__class__.__name__))
        
    def reverse(self):
        self._data.reverse()
    
    def rows(self):
        for row in self._data:
            yield row
    
    def sort(self, key=None, reverse=False):
        u"""Sort table rows.
        
        If no key is specified, the table is sorted as a list of lists. If a
        callable key is specified, this is applied to each row during sorting.
        If the specified key is a single column index/heading (or iterable of
        column indices/headings), then the table rows are sorted by these.
        """
        
        if key is None:
            
            self._data = sorted(self._data, reverse=reverse)
        
        elif isinstance(key, Callable):
            
            self._data = sorted(self._data, key=key, reverse=reverse)
            
        else:
            
            if not isinstance(key, Iterable) or isinstance(key, basestring):
                key = [key]
            
            try:
                keys = tuple(key)
            except TypeError:
                raise TypeError("{} object is not a valid {} sort key".format(
                    type(key).__name__, self.__class__.__name__))
            
            indices = [ self._resolve_column_index(k) for k in keys ]

            for i in reversed(indices):
                self._data = sorted(self._data, key=itemgetter(i), reverse=reverse)

    def to_dict(self):
        u"""Return as a dictionary mapping headings to columns."""
        return dict( izip(self.headings, seq( seq(column)
            for column in izip(*self._data) )) )
    
    def values(self):
        u"""Get table element values by row, then column."""
        for row in self._data:
            for x in row:
                yield x

################################################################################

__all__ = ['Table']

################################################################################
