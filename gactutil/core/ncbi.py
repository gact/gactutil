#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil NCBI module."""

import os
from socket import error as SocketError
from time import sleep
from urllib2 import HTTPError

from Bio import Entrez

from gactutil.core.about import about
from gactutil.core import const

################################################################################

Entrez.email = about[u'author_email']
Entrez.tool = about[u'name']

################################################################################

# Number of seconds to wait between NCBI queries
const.ncbi_query_beat = 1.0

# Number of seconds to wait if NCBI server appears to be busy.
const.ncbi_query_delay = 60.0

################################################################################

def check_efetch(**kwargs):
    u"""Wrapper function for Entrez efetch."""
    
    result = None
        
    # Set maximum number of efetch attempts.
    max_attempts = 10
    
    # Init actual number of efetch attempts.
    attempts = 0
    
    # Do NCBI efetch.
    while attempts < max_attempts:
        try:
            request = Entrez.efetch(**kwargs)
            result = Entrez.read(request)
        except (HTTPError, RuntimeError, SocketError, URLError):
            sleep( const.ncbi_query_delay )
        else:
            break
    
    # Verify result obtained.
    if result is None:
        raise RuntimeError("Entrez efetch failed after {!r} attempts".format(attempts))
    
    # Validate result.
    if any( fault in result for fault in ('WarningList', 'ErrorList') ):
        msg = 'check parameters'
        for fault in ('WarningList', 'ErrorList'):
            if 'OutputMessage' in result[fault]:
                msg = '\n'.join( result[fault]['OutputMessage'] )
                break
        raise RuntimeError("Entrez efetch failed - {}".format(msg))
    
    return result

def check_esearch(**kwargs):
    u"""Wrapper function for Entrez esearch."""
    
    result = None
    
    # Set maximum number of esearch attempts.
    max_attempts = 10
    
    # Init actual number of esearch attempts.
    attempts = 0
    
    # Do NCBI esearch.
    while attempts < max_attempts:
        try:
            request = Entrez.esearch(**kwargs)
            result = Entrez.read(request)
        except (HTTPError, RuntimeError, SocketError, URLError):
            sleep( const.ncbi_query_delay )
        else:
            break
    
    # Verify result obtained.
    if result is None:
        raise RuntimeError("Entrez esearch failed after {!r} attempts".format(attempts))
    
    # Validate result.
    if any( fault in result for fault in ('WarningList', 'ErrorList') ):
        msg = "check parameters"
        for fault in ('WarningList', 'ErrorList'):
            if 'OutputMessage' in result[fault]:
                msg = '\n'.join( result[fault]['OutputMessage'] )
                break
        raise RuntimeError("Entrez esearch failed - {}".format(msg))
    
    return result

################################################################################

__all__ = ['check_efetch', 'check_esearch']

################################################################################
