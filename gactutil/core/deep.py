#!/usr/bin/python -tt
# -*- coding: utf-8 -*-
u"""GACTutil deep data module."""

from collections import Iterable
from collections import Mapping
from collections import MutableMapping
import copy
from itertools import izip
from itertools import repeat

class DeepDict(MutableMapping):
    u"""Nested dictionary class."""
    
    @classmethod
    def _validate_keys(cls, keys):
        
        if not isinstance(keys, tuple):
            keys = (keys,)
        elif len(keys) == 0:
            raise KeyError("no {} keys specified".format(cls.__name__))
        
        # NB: this is necessary to avoid ambiguity between
        # a tuple of keys and a single key of type tuple.
        for key in keys:
            if isinstance(key, Iterable) and not isinstance(key, basestring):
                raise KeyError("{} object cannot take key of type {!r}: {!r}".format(
                    cls.__name__, type(key).__name__, key))
        
        return keys
    
    @classmethod
    def fromkeys(cls, keys, value):
        return cls( izip(keys, repeat( value, len(keys) ) ) )
    
    def __init__(self, *args, **kwargs):
        self._data = dict()
        self.update(*args, **kwargs)
    
    def __contains__(self, keys):
        return self.has_keys(keys)
    
    def __copy__(self):
        return self.__class__( copy.copy(self._data) )
    
    def __deepcopy__(self):
        return self.__class__( copy.deepcopy(self._data) )
    
    def __delattr__(self, name):
        raise TypeError("{} object does not support attribute deletion".format(
            self.__class__.__name__))
    
    def __delitem__(self, keys):
        
        keys = self.__class__._validate_keys(keys)
        
        stack = list()
        
        # Delete value indexed by the key tuple, 
        # remember ancestral mapping objects.
        try: 
            d = self._data
            for i, key in enumerate(keys[:-1], start=1):
                if key in d and not isinstance(d[key], Mapping):
                    raise TypeError("{} item accessed by keys {!r} is not of mapping type".format(
                        self.__class__.__name__, keys[:i]))
                stack.append( (d, key) )
                d = d[key]
            del d[ keys[-1] ]
        except KeyError:
            raise KeyError(keys)
        
        # Delete any newly-emptied ancestral mappings.
        while True:
            
            try:
                d, key = stack.pop()
            except IndexError:
                break # DeepDict is completely empty
            
            if len(d[key]) == 0:
                del d[key]
            else:
                break # mapping object is not empty
    
    def __eq__(self, other):
        return type(self) is type(other) and self._data == other._data
    
    def __getitem__(self, keys):
        
        keys = self.__class__._validate_keys(keys)
        
        try: # Get value of object indexed by the key tuple.
            d = self._data
            for i, key in enumerate(keys[:-1], start=1):
                if key in d and not isinstance(d[key], Mapping):
                    raise TypeError("{} item accessed by keys {!r} is not of mapping type".format(
                        self.__class__.__name__, keys[:i]))
                d = d[key]
            value = d[ keys[-1] ]
        except KeyError:
            raise KeyError(keys)
        
        return value
    
    def __iter__(self):
        return iter(self._data)
    
    def __len__(self):
        return len(self._data)
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, repr(self._data)[1:-1])
    
    def __setattr__(self, name, value):
        if hasattr(self, '_data'):
            raise TypeError("{} object does not support attribute assignment".format(
                self.__class__.__name__))
        self.__dict__[name] = value
    
    def __setitem__(self, keys, value):
        
        keys = self.__class__._validate_keys(keys)
        
        try: # Set value of object indexed by the tuple of keys.
            d = self._data
            for i, key in enumerate(keys[:-1], start=1):
                if key in d:
                    if not isinstance(d[key], Mapping):
                        raise TypeError("{} item accessed by keys {!r} is not of mapping type".format(
                            self.__class__.__name__, keys[:i]))
                else:
                    d[key] = dict()
                d = d[key]
            d[ keys[-1] ] = value
        except KeyError:
            raise KeyError(keys)
    
    def __str__(self):
        return unicode(self._data).encode('utf_8')
    
    def __unicode__(self):
        return unicode(self._data)
    
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default
        
    def has_key(self, key):
        return self._data.has_key(key)
    
    def has_keys(self, keys):
        try:
            self[keys]
        except KeyError:
            return False
        else:
            return True
    
    def leafitems(self):
        u"""Iterate over terminal items."""
        for keys, subkeys, value in self.walk():
            if not isinstance(value, Mapping) or len(value) == 0:
                yield (keys, value)
    
    def leafkeys(self):
        u"""Iterate over terminal keys."""
        for keys, subkeys, value in self.walk():
            if not isinstance(value, Mapping) or len(value) == 0:
                yield keys
        
    def leafvalues(self):
        u"""Iterate over terminal values."""
        for keys, subkeys, value in self.walk():
            if not isinstance(value, Mapping) or len(value) == 0:
                yield value
    
    def walk(self):
        u"""Iterate over members."""
        
        # Init stack from dict.
        stack = [ ((), self._data) ]
        
        # Init set of checked objects.
        checked = set()
        
        while True:
            
            try: # Pop next object from stack.
                (keys, x) = stack.pop()
            except IndexError:
                break
            
            # Skip previously-checked objects.
            xid = id(x)
            if xid in checked:
                continue
            checked.add(xid)
            
            # If mapping, get subkeys, then for each child,
            # push its key tuple and value onto the stack..
            if isinstance(x, Mapping):
                
                subkeys = tuple( sorted( _ for _ in x ) )
                
                for subkey in reversed(subkeys):
                    stack.append( (keys + (subkey,), x[subkey]) )
                
            # ..otherwise set empty tuple of subkeys.
            else:
                subkeys = ()
                
            yield keys, subkeys, x

################################################################################

__all__ = ['DeepDict']

################################################################################
