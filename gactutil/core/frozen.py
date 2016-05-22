#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil frozen data module."""

from __future__ import absolute_import
from collections import Iterable
from collections import Mapping
from collections import Sequence
from collections import Set

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
        raise TypeError("{!r} object does not support attribute deletion".format(
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
            raise TypeError("{!r} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[key] = value
    
    def __str__(self):
        return str(self._data)
    
    def __unicode__(self):
        return unicode(self._data)
    
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
        raise TypeError("{!r} object does not support the update operation".format(
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
        raise TypeError("{!r} object does not support attribute deletion".format(
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
            raise TypeError("{!r} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[index] = value
    
    def __str__(self):
        return str(self._data)
    
    def __unicode__(self):
        return unicode(self._data)
    
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

################################################################################
