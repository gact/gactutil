#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil frozen data module."""

from collections import Container
from collections import Iterable
from collections import Mapping
from collections import Sequence
from collections import Set
import copy
import csv
import datetime
from itertools import izip
from StringIO import StringIO

from gactutil.core import _ImmutableScalarTypes
from gactutil.core.deep import DeepDict
from gactutil.core.table import _TableHeadings
from gactutil.core.table import Table

################################################################################

class FrozenObject(object):
    u"""Base class for a frozen object."""
    
    @classmethod
    def _freeze(cls, x, memo=set()):
        raise NotImplementedError("{} object does not support the freeze operation".format(
            self.__class__.__name__))
    
    @classmethod
    def freeze(cls, x):
        u"""Convert to a frozen object."""
        return cls._freeze(x)
    
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("FrozenObject is an abstract class")
    
    def __delattr__(self, name):
        raise TypeError("{} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __delitem__(self, key):
        raise TypeError("{} object does not support item deletion".format(
            self.__class__.__name__))
    
    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __setattr__(self, name, value):
        if hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[name] = value
    
    def __setitem__(self, key, value):
        raise TypeError("{} object does not support item assignment".format(
            self.__class__.__name__))
    
    def _thaw(self, memo=None):
        raise NotImplementedError("{} object does not support the thaw operation".format(
            self.__class__.__name__))
    
    def thaw(self):
        u"""Convert to a mutable object."""
        return self._thaw()

class FrozenNestable(FrozenObject):
    u"""Base class for a frozen nestable container."""
    
    @classmethod
    def _freeze(cls, x, memo=set()):
        return cls(x, memo=memo)
    
    def __init__(self, *args, **kwargs):
        raise NotImplementedError("FrozenNestable is an abstract class")
    
    def __contains__(self, item):
        return item in self._data
    
    def __copy__(self):
        return self.__class__( copy.copy(self._data) )
    
    def __deepcopy__(self):
        return self.__class__( copy.deepcopy(self._data) )
    
    def __eq__(self, other):
        return type(self) is type(other) and self._data == other._data
    
    def __iter__(self):
        return iter(self._data)
    
    def __len__(self):
        return len(self._data)
    
    def __str__(self):
        return unicode(self._data).encode('utf_8')
    
    def __unicode__(self):
        return unicode(self._data)

################################################################################

class FrozenDict(FrozenNestable, Mapping):
    u"""Frozen dictionary class.
    
    A FrozenDict can be created and accessed in the same way as a builtin dict,
    except that keyword arguments cannot be used in its constructor. The values
    of a FrozenDict are recursively frozen when the FrozenDict is created, and
    the resulting object cannot be modified through its public interface.
    """
    
    @classmethod
    def fromkeys(cls, keys, value=None):
        return cls( dict.fromkeys(keys, value) )
    
    def __init__(self, data, **kwargs):
        
        memo = kwargs.pop('memo', set())
        
        if len(kwargs) > 0:
            raise ValueError("{}() cannot take unenumerated keyword arguments".format(
                self.__class__.__name__))
        
        if isinstance(data, Mapping):
            data = [ [ k, data[k] ] for k in data ]
        elif not isinstance(data, Iterable):
            raise TypeError("{} data is not iterable".format(
                type(data).__name__))
        
        temp_dict = dict()
        
        for i, pair in enumerate(data):
            
            pair = list(pair)
            
            if len(pair) != 2:
                raise ValueError("{} sequence element #{} has length {}; 2 is required".format(
                    self.__class__.__name__, i, len(pair)))
            
            for j, x in enumerate(pair):
                
                xid = id(x)
                if xid in memo:
                    if isinstance(x, Container) and not isinstance(x, basestring):
                        raise ValueError("{} object cannot contain a circular "
                            "data structure".format(self.__class__.__name__))
                    continue
                memo.add(xid)
                
                if isinstance(x, _ImmutableScalarTypes + (FrozenObject, frozenset)):
                    continue
                
                if isinstance(x, tuple):
                    try:
                        hash(x)
                    except TypeError:
                        pass
                    else:
                        continue
                
                for mutable, frozen in _FrozenTypePairs:
                    if isinstance(x, mutable):
                        pair[j] = frozen._freeze(x, memo=memo)
                        break
                else:
                    try:
                        hash(x)
                    except TypeError:
                        raise TypeError("unhashable type: {!r}".format(
                            type(x).__name__))
            
            temp_dict[ pair[0] ] = pair[1]
        
        self._data = temp_dict
    
    def __getitem__(self, key):
        return self._data[key]
    
    def __hash__(self):
        return hash( frozenset( self._data.items() ) )
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._data)[1:-1])
    
    def _thaw(self, memo=None):
        
        # NB: default to None, create memo set explicitly
        if memo is None:
            memo = set()
        
        result = dict(self)
        
        for k, x in result.items():
            
            xid = id(x)
            if xid in memo: continue
            memo.add(xid)
            
            if isinstance(x, FrozenObject):
                result[k] = x._thaw(memo=memo)
        
        return result
    
    def clear(self):
        raise TypeError("{} object does not support the clear operation".format(
            self.__class__.__name__))
    
    def copy(self):
        return self.__class__( self._data.copy() )
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
    
    def has_key(self, key):
        return self._data.has_key(key)
        
    def items(self):
        return self._data.items()
        
    def iteritems(self):
        return self._data.iteritems()
    
    def iterkeys(self):
        return self._data.iterkeys()
    
    def itervalues(self):
        return self._data.itervalues()
    
    def keys(self):
        return self._data.keys()
    
    def pop(self, *args, **kwargs):
        raise TypeError("{} object does not support the pop operation".format(
            self.__class__.__name__))
    
    def popitem(self):
        raise TypeError("{} object does not support the popitem operation".format(
            self.__class__.__name__))
    
    def setdefault(self, *args, **kwargs):
        raise TypeError("{} object does not support the setdefault operation".format(
            self.__class__.__name__))
    
    def update(self, *args, **kwargs):
        raise TypeError("{!r} object does not support the update operation".format(
            self.__class__.__name__))
    
    def values(self):
        return self._data.values()
    
    def viewitems(self):
        return self._data.viewitems()
    
    def viewkeys(self):
        return self._data.viewkeys()
    
    def viewvalues(self):
        return self._data.viewvalues()

class FrozenDeepDict(FrozenDict, DeepDict):
    u"""Frozen nested dictionary class.
    
    A FrozenDeepDict can be created and accessed in the same way as a DeepDict,
    except that keyword arguments cannot be used in its constructor. The values
    of a FrozenDeepDict are recursively frozen when the FrozenDict is created,
    and the resulting object cannot be modified through its public interface.
    """
    
    @classmethod
    def fromkeys(cls, keys, value):
        return FrozenDict( DeepDict.fromkeys(keys, value) )
    
    def __init__(self, *args, **kwargs):
        
        memo = kwargs.pop('memo', set())
        
        if len(kwargs) > 0:
            raise ValueError("{}() cannot take unenumerated keyword arguments".format(
                self.__class__.__name__))
        
        FrozenDict.__init__(self, DeepDict(*args), memo=memo)
    
    def __getitem__(self, keys):
        return DeepDict.__getitem__(self, keys)
    
    def _thaw(self, memo=None):
        return DeepDict( FrozenDict._thaw(self, memo=memo) )

class FrozenList(FrozenNestable, Sequence):
    u"""Frozen list class.
    
    A FrozenList can be created and accessed in the same way as a builtin list.
    Its elements are recursively frozen when the FrozenList is created, and the
    resulting object cannot be modified through its public interface.
    """
    
    def __init__(self, data, **kwargs):
        
        memo = kwargs.pop('memo', set())
        
        if len(kwargs) > 0:
            raise ValueError("{}() cannot take unenumerated keyword arguments".format(
                self.__class__.__name__))
        
        data = list(data)
        
        for i, x in enumerate(data):
            
            xid = id(x)
            if xid in memo:
                if isinstance(x, Container) and not isinstance(x, basestring):
                    raise ValueError("{} object cannot contain a circular data structure".format(
                        self.__class__.__name__))
                continue
            memo.add(xid)
            
            if isinstance(x, _ImmutableScalarTypes + (FrozenObject, frozenset)):
                continue
            
            if isinstance(x, tuple):
                try:
                    hash(x)
                except TypeError:
                    pass
                else:
                    continue
            
            for mutable, frozen in _FrozenTypePairs:
                if isinstance(x, mutable):
                    data[i] = frozen._freeze(x, memo=memo)
                    break
            else:
                try:
                    hash(x)
                except TypeError:
                    raise TypeError("unhashable type: {!r}".format(
                        type(x).__name__))
        
        self._data = tuple(data)
    
    def __getitem__(self, index):
        return self._data[index]
    
    def __hash__(self):
        return hash(self._data)
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._data)[1:-1])
    
    def __reversed__(self):
        return reversed(self._data)
    
    def _thaw(self, memo=None):
        
        # NB: default to None, create memo set explicitly
        if memo is None:
            memo = set()
        
        result = list(self)
        
        for i, x in enumerate(result):
            
            xid = id(x)
            if xid in memo: continue
            memo.add(xid)
            
            if isinstance(x, _ImmutableScalarTypes):
                continue
            
            if isinstance(x, FrozenObject):
                result[i] = x._thaw(memo=memo)
        
        return result
    
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
            raise ValueError("{!r} is not in {}".format(
                x, self.__class__.__name__))
    
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

class FrozenSet(FrozenNestable, Set):
    u"""Frozen set class.
    
    A FrozenSet can be created and accessed in the same way as a set or frozenset.
    The values of a FrozenSet are recursively frozen when it is created, and the
    resulting object cannot be modified through its public interface.
    """
    
    def __init__(self, data, **kwargs):
        
        memo = kwargs.pop('memo', set())
            
        if len(kwargs) > 0:
            raise ValueError("{}() cannot take unenumerated keyword arguments".format(
                self.__class__.__name__))
        
        data = list(data)
        
        for i, x in enumerate(data):
            
            xid = id(x)
            if xid in memo:
                if isinstance(x, Container) and not isinstance(x, basestring):
                    raise ValueError("{} object cannot contain a circular data structure".format(
                        self.__class__.__name__))
                continue
            memo.add(xid)
            
            if isinstance(x, _ImmutableScalarTypes + (FrozenObject, frozenset)):
                continue
            
            if isinstance(x, tuple):
                try:
                    hash(x)
                except TypeError:
                    pass
                else:
                    continue
            
            for mutable, frozen in _FrozenTypePairs:
                if isinstance(x, mutable):
                    data[i] = frozen._freeze(x, memo=memo)
                    break
            else:
                try:
                    hash(x)
                except TypeError:
                    raise TypeError("unhashable type: {!r}".format(
                        type(x).__name__))
        
        self._data = frozenset(data)
    
    def __and__(self, *args, **kwargs):
        return FrozenSet( self._data.__and__(*args, **kwargs) )
    
    def __getitem__(self, key):
        raise TypeError("{} object does not support indexing".format(
            self.__class__.__name__))
    
    def __ge__(self, *args, **kwargs):
        return self._data.__ge__(*args, **kwargs)
    
    def __gt__(self, *args, **kwargs):
        return self._data.__gt__(*args, **kwargs)
    
    def __hash__(self):
        return hash(self._data)
    
    def __le__(self, *args, **kwargs):
        return self._data.__le__(*args, **kwargs)
    
    def __lt__(self, *args, **kwargs):
        return self._data.__lt__(*args, **kwargs)
    
    def __or__(self, *args, **kwargs):
        return FrozenSet( self._data.__or__(*args, **kwargs) )
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._data)[11:-2])
    
    def __sub__(self, *args, **kwargs):
        return FrozenSet( self._data.__sub__(*args, **kwargs) )
    
    def __xor__(self, *args, **kwargs):
        return FrozenSet( self._data.__xor__(*args, **kwargs) )
    
    def _thaw(self, memo=None):
        # NB: elements must remain hashable, so cannot be thawed
        return set(self)
    
    def add(self, *args, **kwargs):
        raise TypeError("{} object does not support element addition".format(
            self.__class__.__name__))
    
    def clear(self):
        raise TypeError("{} object does not support the clear operation".format(
            self.__class__.__name__))
    
    def copy(self):
        return self.__class__( self._data.copy() )
    
    def difference(self, *args, **kwargs):
        return FrozenSet( self._data.difference(*args, **kwargs) )
    
    def discard(self, *args, **kwargs):
        raise TypeError("{} object does not support the discard operation".format(
            self.__class__.__name__))
    
    def intersection(self, *args, **kwargs):
        return FrozenSet( self._data.intersection(*args, **kwargs) )

    def isdisjoint(self, *args, **kwargs):
        return self._data.isdisjoint(*args, **kwargs)
    
    def issubset(self, *args, **kwargs):
        return self._data.issubset(*args, **kwargs)
    
    def issuperset(self, *args, **kwargs):
        return self._data.issuperset(*args, **kwargs)
    
    def pop(self, *args, **kwargs):
        raise TypeError("{} object does not support the pop operation".format(
            self.__class__.__name__))
    
    def remove(self, *args, **kwargs):
        raise TypeError("{} object does not support element removal".format(
            self.__class__.__name__))
    
    def symmetric_difference(self, *args, **kwargs):
        return FrozenSet( self._data.symmetric_difference(*args, **kwargs) )
    
    def union(self, *args, **kwargs):
        return FrozenSet( self._data.union(*args, **kwargs) )

################################################################################

class _FrozenHeadings(FrozenObject, _TableHeadings):
    u"""Frozen table headings class."""

    @classmethod
    def _freeze(cls, x):
        return cls(x)
    
    def __init__(self, headings=None, size=None):
        _TableHeadings.__init__(self, headings=headings, size=size)
    
    def __hash__(self):
        return hash( tuple(self._data) )
        
    def _thaw(self, memo=None):
        return _TableHeadings(self._data)

################################################################################

class FrozenTable(FrozenObject, Table):
    u"""Frozen table class.
    
    A FrozenTable is a sequence of regular rows, which can be indexed by column
    heading in addition to row and column indices. Once created, the resulting
    object cannot be modified through its public interface.
    """
    
    _hdg_type = _FrozenHeadings
    _seq_type = tuple
    
    @classmethod
    def _freeze(cls, x, memo=set()):
        if not isinstance(x, Table):
            raise TypeError("expected object of type {!r}, not {!r}".format(
                Table.__name__, x.__class__.__name__))
        return cls(x._data, x.headings)
    
    def __init__(self, *args, **kwargs):
        Table.__init__(self, *args, **kwargs)
    
    def __hash__(self):
        return hash( (tuple(self.headings), self._data) )
        
    def _thaw(self, memo=None):
        return Table(self._data, self.headings)

################################################################################

def freeze(x):
    u"""Get frozen copy of object."""
    
    if isinstance(x, _ImmutableScalarTypes + (FrozenObject, frozenset)):
        return x
    
    if isinstance(x, tuple):
        try:
            hash(x)
        except TypeError:
            pass
        else:
            return x
    
    for mutable, frozen in _FrozenTypePairs:
        if isinstance(x, mutable):
            return frozen._freeze(x)
    else:
        try:
            hash(x)
        except TypeError:
            raise TypeError("unhashable type: {!r}".format(type(x).__name__))
    
    return x

################################################################################

# Sequence of pairs mapping (possibly abstract)
# mutable types to their frozen counterpart.
# NB: always iterate in order (e.g. Mapping before Iterable)
_FrozenTypePairs = (
    (DeepDict, FrozenDeepDict),
    (Mapping, FrozenDict),
    (Set, FrozenSet),
    (Table, FrozenTable),
    (Iterable, FrozenList)
)

__all__ = ['FrozenDeepDict', 'FrozenDict', 'FrozenList', 'FrozenSet', 'FrozenTable']

################################################################################
