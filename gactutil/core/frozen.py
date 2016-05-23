#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil frozen data module."""

from __future__ import absolute_import
from collections import Iterable
from collections import Mapping
from collections import Sequence
from collections import Set
import csv
from datetime import date
from datetime import datetime
from itertools import izip
from StringIO import StringIO
from types import NoneType

from gactutil.core.csv import csvtext

################################################################################

class FrozenDict(Mapping):
    u"""Hashable dictionary class.
    
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
        raise TypeError("{} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __eq__(self, other):
        return isinstance(other, FrozenDict) and self._data == other._data
    
    def __getitem__(self, key):
        return self._data.__getitem__(key)
    
    def __hash__(self):
        return hash( frozenset( self._data.items() ) )
    
    def __iter__(self):
        return self._data.__iter__()
    
    def __len__(self):
        return self._data.__len__()
    
    def __ne__(self, other):
        return not isinstance(other, FrozenDict) or self._data != other._data
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._data.__repr__()[1:-1])
    
    def __setattr__(self, key, value):
        if hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[key] = value
    
    def __str__(self):
        return self.__unicode__().encode('utf_8')
    
    def __unicode__(self):
        return unicode(self._data)
    
    def copy(self):
        return self
    
    def clear(self):
        raise TypeError("{} object does not support the clear operation".format(
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
        raise TypeError("{} object does not support the pop operation".format(
            self.__class__.__name__))
    
    def popitem(self):
        raise TypeError("{} object does not support the popitem operation".format(
            self.__class__.__name__))
    
    def setdefault(self, *args, **kwargs):
        raise TypeError("{} object does not support the setdefault operation".format(
            self.__class__.__name__))
    
    def to_dict(self, **kwargs):
        u"""Return object as a mutable dict."""
        
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
        raise TypeError("{} object does not support the update operation".format(
            self.__class__.__name__))
    
    def viewitems(self):
        return self._data.viewitems()
    
    def viewkeys(self):
        return self._data.viewkeys()
    
    def viewvalues(self):
        return self._data.viewvalues()
    
class FrozenList(Sequence):
    u"""Hashable list class.
    
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
        raise TypeError("{} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __eq__(self, other):
        return isinstance(other, FrozenList) and self._data == other._data
    
    def __getitem__(self, index):
        return self._data[index]
    
    def __hash__(self):
        return hash(self._data)
    
    def __len__(self):
        return len(self._data)
    
    def __ne__(self, other):
        return not isinstance(other, FrozenList) or self._data != other._data
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, self._data.__repr__()[1:-1])
    
    def __setattr__(self, index, value):
        if hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[index] = value
    
    def __str__(self):
        return self.__unicode__().encode('utf_8')
    
    def __unicode__(self):
        return unicode(self._data)
    
    def append(self, *args, **kwargs):
        raise TypeError("{} object does not support the append operation".format(
            self.__class__.__name__))
    
    def count(self, x):
        return self._data.count(x)
    
    def extend(self, *args, **kwargs):
        raise TypeError("{} object does not support extension".format(
                self.__class__.__name__))
    
    def index(self, x):
        try:
            return self._data.index(x)
        except ValueError:
            raise ValueError("{!r} is not in {}".format(x, self.__class__.__name__))
    
    def insert(self, *args, **kwargs):
        raise TypeError("{} object does not support item insertion".format(
                self.__class__.__name__))
    
    def pop(self, *args, **kwargs):
        raise TypeError("{} object does not support the pop operation".format(
                self.__class__.__name__))
    
    def remove(self, *args, **kwargs):
        raise TypeError("{} object does not support item removal".format(
                self.__class__.__name__))
    
    def reverse(self):
        raise TypeError("{} object does not support in-place reversal".format(
                self.__class__.__name__))
    
    def sort(self, *args, **kwargs):
        raise TypeError("{} object does not support in-place sorting".format(
                self.__class__.__name__))
    
    def to_list(self, **kwargs):
        u"""Return object as a mutable list."""
        
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

class FrozenTable(object):
    u"""Hashable table class.
    
    A FrozenTable can be created from either an iterable of values, or an
    iterable of iterables. The table is filled in row order, with row size
    determined by the number of fieldnames. Once created, the resulting
    object cannot be subsequently modified through its public interface.
    """
    
    # Supported FrozenTable cell value types. Cell values must be representable
    # in a single cell of a comma-delimited table, so in addition to being one
    # of the listed types, cell values must not contain any unescaped newlines.
    supported_types = (NoneType, bool, basestring, float, int, long,
        datetime, date)
    
    @property
    def fieldcount(self):
        u"""Number of fields in FrozenTable."""
        return len(self._field_list)
    
    @property
    def fieldnames(self):
        u"""Tuple of FrozenTable fieldnames."""
        return self._field_list
    
    @staticmethod
    def _resolve_index(index, length):
        u"""Resolve positive index for a sequence of the given length."""
        
        if not isinstance(index, int):
            raise TypeError("index must be integer, not {!r}".format(
                type(index).__name__))
        
        if not isinstance(length, int):
            raise TypeError("index resolution length must be integer, not {!r}".format(
                type(length).__name__))
        
        if index < -length or index >= length:
            raise IndexError("index ({!r}) out of range".format(index))
        
        if index < 0:
            index += length
        
        return index
    
    @classmethod
    def from_dict(cls, data):
        u"""Get FrozenTable from dict."""
        
        if not isinstance(data, Mapping):
            raise TypeError("data not of mapping type ~ {!r}".format(data))
        
        fieldnames = sorted( data.keys() )
        
        col_length = None
        
        for x in data.values():
            
            try:
                assert isinstance(x, Sequence)
                assert not isinstance(x, basestring)
                
                row_count = len(x)
                
            except (AssertionError, TypeError):
                raise TypeError("dict data column must be a sized sequence, not {!r}".format(
                    type(x).__name__))
            
            if col_length is None:
                col_length = row_count
            elif row_count != col_length:
                raise ValueError("dict data columns have inconsistent lengths")
        
        return FrozenTable(data=[ x for x in izip(*[ data[k]
            for k in fieldnames ]) ], fieldnames=fieldnames)
    
    def __init__(self, data=(), fieldnames=()):
        
        for fieldname in fieldnames:
            if not isinstance(fieldname, basestring):
                raise TypeError("{} fieldname must be of string type, not {!r}".format(
                    self.__class__.__name__, type(fieldname).__name__))
        
        # Set tuple of fieldnames.
        self._field_list = tuple(fieldnames)
        
        # Set mapping of fieldnames to column indices.
        self._field_dict = FrozenDict( (fieldname, i)
            for i, fieldname in enumerate(fieldnames) )
        
        if len(self._field_dict) != len(self._field_list):
            raise ValueError("duplicate {} fieldnames".format(
                self.__class__.__name__))
        
        # Check number of dimensions in input data.
        try:
            assert isinstance(data, Iterable)
            assert not isinstance(data, basestring)
            
            if ( len(data) > 0 and isinstance(data[0], Iterable) and
                not isinstance(data[0], basestring) ):
                ndim = 2
            else:
                ndim = 1
            
        except (AssertionError, TypeError):
            raise TypeError("{} data must be a sized iterable, not {!r}".format(
                self.__class__.__name__, type(data).__name__))
        
        # Get table row width from number of fieldnames.
        row_width = len(fieldnames)
        
        table = list()
        
        # If data is an iterable of iterables, append rows
        # to temp table, validating the width of each row..
        if ndim == 2:
            
            row_count = len(data)
            
            for row in data:
                if len(row) != row_width:
                    raise ValueError("{} data has inconsistent row widths".format(
                        self.__class__.__name__))
                table.append( tuple(row) )
            
        # ..otherwise iterate over input data with
        # step equal to the number of fieldnames.
        elif row_width > 0:
            
            row_count, remainder = divmod(len(data), row_width)
            
            if remainder:
                raise ValueError("{} data has inconsistent row widths".format(
                    self.__class__.__name__))
            
            for i in xrange(0, row_count * row_width, row_width):
                j = i + row_width
                table.append( tuple(data[i:j]) )
        
        # Validate table contents.
        for row in table:
            for x in row:
                if isinstance(x, basestring) and any( line_break in x
                    for line_break in (u'\r\n', u'\n', u'\r') ):
                    raise ValueError("{} is unrepresentable - contains multiline string value ~ {!r}".format(
                        self.__class__.__name__, x))
                elif not isinstance(x, FrozenTable.supported_types):
                    raise TypeError("{} is invalid - contains value of unknown or non-scalar type {!r}".format(
                        self.__class__.__name__, type(x).__name__))
        
        self._data = tuple(table)
    
    def __contains__(self, item):
        return any( item in row for row in self._data )
    
    def __delattr__(self, key):
        raise TypeError("{} does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __delitem__(self, key):
        raise TypeError("{} does not support item deletion".format(
            self.__class__.__name__))
    
    def __eq__(self, other):
        return ( isinstance(other, self.__class__) and
            self._field_list == other._field_list and
            self._data == other._data )
    
    def __getitem__(self, key):
        
        try: # Split key into row and column keys.
            row_key, col_key = key
        except ValueError:
            problem = 'too many' if len(key) > 2 else 'too few'
            raise ValueError("{} {} indices/keys".format(
                problem, self.__class__.__name__))
        except TypeError:
            raise TypeError("{} index/key is of invalid type ({})".format(
                self.__class__.__name__, type(key).__name__))
        
        # Check if row key is a single index.
        is_row_index = isinstance(row_key, int)
        
        # Check if column key is a single index or equivalent fieldname.
        is_col_index = isinstance(col_key, (int, basestring))
        
        # If both row and column index specified, get the specified value..
        if is_row_index and is_col_index:
            
            r = self._resolve_record_index(row_key)
            c = self._resolve_field_index(col_key)
            item = self._data[r][c]
            
        # ..otherwise if one row and multiple columns specified, get
        # the specified FrozenRecord subset as a FrozenRecord object..
        elif is_row_index:
            
            r = self._resolve_record_index(row_key)
            col_indices = self._resolve_field_indices(col_key)
            data = [ self._data[r][c] for c in col_indices ]
            fieldnames = [ self._field_list[c] for c in col_indices ]
            item = FrozenRecord(data, fieldnames)
            
        # ..otherwise if multiple rows and multiple columns specified,
        # get the specified FrozenTable subset as a FrozenTable object.
        else:
            
            row_indices = self._resolve_record_indices(row_key)
            if is_col_index:
                col_indices = [ self._resolve_field_index(col_key) ]
            else:
                col_indices = self._resolve_field_indices(col_key)
            data = [ [ self._data[r][c] for c in col_indices ]
                for r in row_indices ]
            fieldnames = [ self._field_list[c] for c in col_indices ]
            item = FrozenTable(data, fieldnames)
        
        return item
    
    def __hash__(self):
        return hash( (self._field_list, self._data) )
    
    def __iter__(self):
        for row in self._data:
            yield FrozenRecord(row, self._field_list)
    
    def __len__(self):
        return self._data.__len__()
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __repr__(self):
        
        if len(self._data) == 0:
            return 'FrozenTable()'
        
        fieldnames = [ repr(fieldname) for fieldname in self._field_list ]
        
        records = list()
        
        for row in self._data:
            
            fields = [ '{}={}'.format(fieldname, repr(value))
                for fieldname, value in zip(fieldnames, row) ]
            
            record = 'FrozenRecord({})'.format(', '.join(fields))
            
            records.append(record)
        
        return 'FrozenTable( {} )'.format(', '.join(records))
    
    def __reversed__(self):
        for row in reversed(self._data):
            yield FrozenRecord(row, self._field_list)
    
    def __setattr__(self, key, value):
        if hasattr(self, '_data'):
            raise TypeError("{} does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[key] = value
    
    def __setitem__(self, key, value):
        raise TypeError("{} does not support item assignment".format(
                self.__class__.__name__))
    
    def __str__(self):
        
        sh = StringIO()
        writer = csv.writer(sh, dialect=csvtext)
        
        for row in (self._field_list,) + self._data:
            row = [ x.encode('utf_8') if isinstance(x, unicode)
                else str(x) for x in row ]
            writer.writerow(row)
        
        result = sh.getvalue()
        sh.close()
        
        return result
    
    def __unicode__(self):
        return self.__str__().decode('utf_8')
    
    def _resolve_field_index(self, key):
        u"""Resolve field index from the given index or fieldname."""
        
        if isinstance(key, basestring):
            
            index = self.fieldindex(key)
            
        elif isinstance(key, int):
            
            index = key
            length = len(self._field_list)
            
            if index < -length or index >= length:
                raise IndexError("{} field index ({!r}) out of range".format(
                    self.__class__.__name__, index))
            
            if index < 0:
                index += length
            
        else:
            raise TypeError("cannot resolve field index from object of type {!r}".format(
                type(key).__name__))
        
        return index
    
    def _resolve_field_indices(self, key):
        u"""Resolve field indices from the given key."""
        
        if isinstance(key, slice):
            
            indices = tuple( i for i in
                xrange( *key.indices( len(self._field_list) ) ) )
            
        elif isinstance(key, Iterable) and not isinstance(key, basestring):
            
            indices = tuple( self._resolve_field_index(k) for k in key )
            
        elif key is Ellipsis:
            
            indices = tuple( i for i in xrange( len(self._field_list) ) )
            
        else:
            raise TypeError("cannot resolve field indices from object of type {!r}".format(
                type(key).__name__))
        
        return indices
    
    def _resolve_record_index(self, key):
        u"""Resolve record index from the given index."""
        
        if isinstance(key, int):
            
            length = len(self._data)
            index = key
            
            if index < -length or index >= length:
                raise IndexError("{} record index ({!r}) out of range".format(
                    self.__class__.__name__, index))
            
            if index < 0:
                index += length
            
        else:
            raise TypeError("cannot resolve record index from object of type {!r}".format(
                type(key).__name__))
        
        return index
    
    def _resolve_record_indices(self, key):
        u"""Resolve record indices from the given key."""
        
        if isinstance(key, slice):
            
            indices = tuple( i for i in
                xrange( *key.indices( len(self._data) ) ) )
            
        elif isinstance(key, Iterable) and not isinstance(key, basestring):
            
            indices = tuple( self._resolve_record_index(k) for k in key )
            
        elif key is Ellipsis:
            
            indices = tuple( i for i in xrange( len(self._data) ) )
            
        else:
            raise TypeError("cannot resolve record indices from object of type {!r}".format(
                type(key).__name__))
        
        return indices
    
    def fieldindex(self, fieldname):
        u"""Get field index corresponding to the specified fieldname."""
        
        if not isinstance(fieldname, basestring):
            raise TypeError("{} fieldname must be of string type, not {!r}".format(
                self.__class__.__name__, type(fieldname).__name__))
        
        try:
            index = self._field_dict[fieldname]
        except KeyError:
            raise KeyError("{} fieldname not found ({!r})".format(
                self.__class__.__name__, fieldname))
        
        return index
    
    def to_dict(self):
        u"""Return FrozenTable as a mutable dict."""
        return dict( izip(self._field_list, izip(*self._data)) )
        
    def to_list(self, flatten=False):
        u"""Return FrozenTable as a mutable list."""
        if flatten:
            return [ x for row in self._data for x in row ]
        else:
            return [ list(x) for x in self._data ]

class FrozenRecord(FrozenTable):
    u"""Hashable record class.
    
    A FrozenRecord is equivalent to a single record of a FrozenTable. Once
    created, the resulting object cannot be subsequently modified through
    its public interface.
    """
    
    @classmethod
    def from_dict(cls, data):
        u"""Get FrozenRecord from dict."""
        
        if not isinstance(data, Mapping):
            raise TypeError("data not of mapping type ~ {!r}".format(data))
        
        fieldnames = sorted( data.keys() )
        
        for x in data.values():
            if not isinstance(x, FrozenTable.supported_types):
                raise TypeError("dict data value is of unknown or non-scalar type {!r}".format(
                    type(x).__name__))
        
        return FrozenRecord(data=[ data[k] for k in fieldnames ],
            fieldnames=fieldnames)
    
    def __init__(self, data=(), fieldnames=()):
        
        super(FrozenRecord, self).__init__(data, fieldnames)
        
        if len(self._data) > 1:
            raise ValueError("invalid {} data".format(self.__class__.__name__))
    
    def __getitem__(self, key):
        
        # Check if column key is a single index or equivalent fieldname.
        is_col_index = isinstance(key, (int, basestring))
        
        # If a single column index specified, get the specified value..
        if is_col_index:
            
            c = self._resolve_field_index(key)
            item = self._data[0][c]
            
        # ..otherwise get specified FrozenRecord subset as a FrozenRecord object.
        else:
            
            col_indices = self._resolve_field_indices(key)
            data = [ self._data[0][c] for c in col_indices ]
            fieldnames = [ self._field_list[c] for c in col_indices ]
            item = FrozenRecord(data, fieldnames)
        
        return item
    
    def __iter__(self):
        for x in self._data[0]:
            yield x
    
    def __len__(self):
        return self._data[0].__len__()
    
    def __repr__(self):
        if len(self._data) == 0:
            return 'FrozenRecord()'
        fieldnames = [ repr(fieldname) for fieldname in self._field_list ]
        fields = [ '{}={}'.format(fieldname, repr(value))
            for fieldname, value in zip(fieldnames, self._data[0]) ]
        return 'FrozenRecord({})'.format(', '.join(fields))
    
    def __reversed__(self):
        for x in reversed(self._data[0]):
            yield x
    
    def to_dict(self):
        u"""Return FrozenRecord as a mutable dict."""
        return dict( izip(self._field_list, self._data[0]) )
        
    def to_list(self):
        u"""Return FrozenRecord as a mutable list."""
        return list(self._data[0])

################################################################################
