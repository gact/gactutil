#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil VCF module.

This module contains functions and utilities for handling data in VCF format.

The VCF filter gactfunc is a wrapper for the PyVCF filter script, and 
the filter classes defined within this module are intended for use with that script.
"""

from __future__ import absolute_import
from argparse import ArgumentError
from subprocess import CalledProcessError
from subprocess import Popen
import sys

import vcf

from gactutil import fsencode
from gactutil.gaction import gactfunc

################################################################################

class CategoricalFilter(vcf.filters.Base):
    u"""Base class for categorical variant filters."""
    
    name = u'CategoricalFilter'
    
    def __init__(self, *args):
        u"""Create filter without arguments."""
        if self.__class__.__name__ == u'CategoricalFilter':
            raise NotImplementedError("CategoricalFilter is an abstract class")
    
    def filter_name(self):
        u"""Return variant-type filter name."""
        return self.name

class MnpOnly(CategoricalFilter):
    u"""Filter class for MNP variants."""
    
    name = u'mnp-only'
    
    def __call__(self, record):
        u"""Pass if MNP variant, 'FAIL' otherwise."""
        
        ref_allele  = record.REF
        alt_alleles = [ unicode(str(x), encoding='utf_8') for x in record.ALT ]
        
        if record.is_monomorphic:
            return u'FAIL'
        
        if record.is_sv:
            return u'FAIL'
        
        alleles = [ref_allele] + alt_alleles
        
        if any( c not in [u'A', u'C', u'G', u'T', u'N', u'*']
            for allele in alleles for c in allele.upper() ):
            return u'FAIL'
        
        if len(ref_allele) == 1:
            return u'FAIL'
        
        if any( len(a) != len(ref_allele) for a in alt_alleles ):
            return u'FAIL'
        
        return None

class SvOnly(CategoricalFilter):
    u"""Filter class for structural variants."""

    name = u'sv-only'

    def __call__(self, record):
        u"""Pass if structural variant, 'FAIL' otherwise."""
        
        if record.is_monomorphic:
            return u'FAIL'
        
        if not record.is_sv:
            return u'FAIL'
        
        return None

################################################################################

@gactfunc
def filter_vcf_variants(infile, outfile, filters, no_short_circuit=False, 
    no_filtered=False):
    u"""Filter VCF.
    
    Args:
        infile (unicode): Input VCF file.
        outfile (unicode): Output filtered VCF file.
        filters (FrozenList): Filter specifications.
        no_short_circuit (bool): Apply all filters to each site.
        no_filtered (bool): Output only sites passing all filters.
    """
    
    # Get core options of PyVCF filter script.
    known_core_options = (u'--output', u'--no-short-circuit', u'--no-filtered')
    
    # Assemble specified core options.
    core_options = list()
    if no_short_circuit:
        core_options.append(u'--no-short-circuit')
    if no_filtered:
        core_options.append(u'--no-filtered')
    
    if any( not isinstance(filter_spec, basestring) for filter_spec in filters ):
        raise TypeError("filters must be of type unicode, not {!r}".format(
            type(filter_spec).__name__))
    
    # Assemble filter specification from individual parts.
    filter_spec = [ token for filter_spec in filters 
        for token in filter_spec.split() ]
    
    if any( x in filter_spec for x in known_core_options ):
        raise ArgumentError("filter specification contains core options")
    
    # Assemble command for PyVCF filter script.
    command = [u'vcf_filter.py'] + core_options + [infile] + filter_spec
    command = [ fsencode(x) for x in command ]
    
    # Run PyVCF filter script.
    try:
        with TextWriter(outfile) as fh:
            process = Popen(command, stdout=fh)
            process.wait()
    except (CalledProcessError, IOError, OSError, RuntimeError, ValueError) as e:
        raise e

################################################################################
