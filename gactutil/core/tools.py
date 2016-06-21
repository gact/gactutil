#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil external tools module."""

from collections import Iterable
from subprocess import CalledProcessError
from subprocess import Popen

from gactutil.core import fsencode
from gactutil.core.config import config

def run(tool, args=(), stdin=None, stdout=None, stderr=None, input=None):
    u"""Run external tool."""
    
    try:
        command = config[u'tools', tool]
    except KeyError:
        raise ValueError("unknown tool: {!r}".format(tool))
    
    if ( not isinstance(args, Iterable) or isinstance(args, basestring) or
        any( not isinstance(arg, basestring) for arg in args ) ):
        raise TypeError("tool arguments must be of string type")
        
    if input is not None and not isinstance(input, basestring):
        raise TypeError("any tool input must be of string type, not {}".format(
            type(input).__name__))
    
    argv = [command] + [ arg for arg in args ]
    
    argv = [ fsencode(arg) for arg in argv ]
    
    try:
        p = Popen(argv, stdin=stdin, stdout=stdout, stderr=stderr)
        out, err = p.communicate(input=input)
    except (CalledProcessError, IOError) as e:
        raise e
    except OSError:
        raise RuntimeError("command not found for {} - set tool command in {!r}".format(
            tool, config.filepath))
    
    return out, err

################################################################################
