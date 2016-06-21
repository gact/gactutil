#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil path utilities."""

from gactutil import FrozenDict
from gactutil import gactfunc
from gactutil.core import respath

################################################################################

@gactfunc
def resolve_path(path, start=None):
    u"""Resolve the given path.
    
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
    return respath(path)

@gactfunc
def resolve_paths(paths, start=None):
    u"""Resolve the given paths.
    
    By default, the specified paths are modified by expanding the home directory
    and any environment variables, resolving symbolic links, and returning the
    resulting absolute path for each input path. If a `start` path is specified,
    the resolved paths are given relative to `start`.
    
    Args:
        paths (FrozenList): System paths.
        start (unicode): Optional starting point for the resolved paths.
    
    Returns:
        FrozenDict: Mapping of input paths to their resolved form.
    """
    res = dict()
    for path in paths:
        res[path] = respath(path, start=start)
    return FrozenDict(res)

################################################################################
