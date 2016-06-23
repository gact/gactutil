#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil constants module.

Solution from: Martelli and Asher (2002) Python Cookbook. O'Reilly. (p.193)
Module reference workaround from: http://stackoverflow.com/questions/5365562
"""

import sys

from gactutil.core.frozen import freeze

class _const(object):
    u"""Class for storing constant values."""
    
    def __init__(self):
        pass
    
    def __setattr__(self, name, value):
        
        if name in self.__dict__:
            raise TypeError("cannot rebind const: {!r}".format(name))
        
        try:
            value = freeze(value)
        except TypeError:
            raise TypeError("cannot bind unhashable object to const: {!r}".format(
                type(value).__name__, name))
        
        self.__dict__[name] = value
    
    def __delattr__(self, name):
        if name in self.__dict__:
            raise TypeError("cannot unbind const: {!r}".format(name))
        raise NameError(name)

################################################################################

# Create reference to module (workaround to prevent deletion).
ref = sys.modules[__name__]

# Assign module name to _const object.
sys.modules[__name__] = _const()

################################################################################