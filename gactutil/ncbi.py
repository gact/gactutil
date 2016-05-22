#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil NCBI utilities."""

from socket import error as SocketError
from time import sleep
from urllib2 import HTTPError

from Bio import Entrez

from gactutil import _read_setting

################################################################################

_info = {
    
    # Set list of known NCBI query faults.
    u'faults': ('WarningList', 'ErrorList'),
    
    # Number of seconds to wait if NCBI server appears to be busy.
    u'polite_delay': 60.0
}

################################################################################

def check_efetch(**kwargs):
    u"""Wrapper function for Entrez efetch."""
    
    result = None
        
    # Set maximum number of efetch attempts.
    max_attempts = 10
    
    # Init actual number of efetch attempts.
    attempts = 0
    
    # Set Entrez email attribute from argument or setting.
    try:
        email = kwargs.pop(u'email')
        assert isinstance(email, basestring)
    except (AssertionError, KeyError):
        try:
            email = _read_setting(u'email')
            assert isinstance(email, basestring)
        except (AssertionError, RuntimeError):
            raise RuntimeError("Entrez efetch failed - please provide an email address")
    Entrez.email = email
    
    # Do NCBI efetch.
    while attempts < max_attempts:
        try:
            request = Entrez.efetch(**kwargs)
            result = Entrez.read(request)
        except (HTTPError, RuntimeError, SocketError, URLError):
            sleep( _info(u'polite_delay') )
        else:
            break
    
    # Verify result obtained.
    if result is None:
        raise RuntimeError("Entrez efetch failed after {!r} attempts".format(attempts))
    
    # Validate result.
    if any( fault in result for fault in _info[u'faults'] ):
        msg = 'check parameters'
        for fault in _info[u'faults']:
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
    
    # Set Entrez email attribute from argument or setting.
    try:
        email = kwargs.pop(u'email')
        assert isinstance(email, basestring)
    except (AssertionError, KeyError):
        try: 
            email = _read_setting(u'email')
            assert isinstance(email, basestring)
        except (AssertionError, RuntimeError):
            raise RuntimeError("Entrez esearch failed - please provide an email address")
    Entrez.email = email
    
    # Do NCBI esearch.
    while attempts < max_attempts:
        try:
            request = Entrez.esearch(**kwargs)
            result = Entrez.read(request)
        except (HTTPError, RuntimeError, SocketError, URLError):
            sleep( _info(u'polite_delay') )
        else:
            break
    
    # Verify result obtained.
    if result is None:
        raise RuntimeError("Entrez esearch failed after {!r} attempts".format(attempts))
    
    # Validate result.
    if any( fault in result for fault in _info[u'faults'] ):
        msg = "check parameters"
        for fault in _info[u'faults']:
            if 'OutputMessage' in result[fault]:
                msg = '\n'.join( result[fault]['OutputMessage'] )
                break
        raise RuntimeError("Entrez esearch failed - {}".format(msg))
    
    return result

################################################################################
