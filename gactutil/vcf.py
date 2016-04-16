#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
"""GACTutil VCF module.

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

from gactutil.gaction import gactfunc

################################################################################

class CategoricalFilter(vcf.filters.Base):
    """Base class for categorical variant filters."""
    
    name = 'CategoricalFilter'
    
    def __init__(self, *args):
        """Create filter without arguments."""
        if self.__class__.__name__ == 'CategoricalFilter':
            raise NotImplementedError("CategoricalFilter is an abstract class")
    
    def filter_name(self):
        """Return variant-type filter name."""
        return self.name

class MnpOnly(CategoricalFilter):
    """Filter class for MNP variants."""
    
    name = 'mnp-only'
    
    def __call__(self, record):
        """Pass if MNP variant, 'FAIL' otherwise."""
        
        ref_allele  = record.REF
        alt_alleles = [ str(x) for x in record.ALT ]
        
        if record.is_monomorphic:
            return 'FAIL'
        
        if record.is_sv:
            return 'FAIL'
        
        alleles = [ref_allele] + alt_alleles
        
        if any( c not in ['A', 'C', 'G', 'T', 'N', '*'] 
            for allele in alleles for c in allele.upper() ):
            return 'FAIL'
        
        if len(ref_allele) == 1:
            return 'FAIL'
        
        if any( len(a) != len(ref_allele) for a in alt_alleles ):
            return 'FAIL'
        
        return None

class SvOnly(CategoricalFilter):
    """Filter class for structural variants."""

    name = 'sv-only'

    def __call__(self, record):
        """Pass if structural variant, 'FAIL' otherwise."""
        
        if record.is_monomorphic:
            return 'FAIL'
        
        if not record.is_sv:
            return 'FAIL'
        
        return None

################################################################################

@gactfunc
def filter_vcf(infile, outfile, filters, no_short_circuit=False, 
    no_filtered=False):
    """Filter VCF.
    
    Args:
        infile (string): Input VCF file.
        outfile (string): Output filtered VCF file.
        filters (list): Filter specifications.
        no_short_circuit (bool): Apply all filters to each site.
        no_filtered (bool): Output only sites passing all filters.
    """
    
    # Get core options of PyVCF filter script.
    known_core_options = ('--output', '--no-short-circuit', '--no-filtered')
    
    # Assemble specified core options.
    core_options = list()
    if no_short_circuit:
        core_options.append('--no-short-circuit')
    if no_filtered:
        core_options.append('--no-filtered')
    
    # Assemble filter specification from individual parts.
    filter_spec = [ token for filter_spec in filters 
        for token in str(filter_spec).split() ]
    
    if any( x in filter_spec for x in known_core_options ):
        raise ArgumentError("filter specification contains core options")
    
    # Assemble command for PyVCF filter script.
    command = ['vcf_filter.py'] + core_options + [infile] + filter_spec
    
    # Run PyVCF filter script.
    try:
        with open(outfile, 'w') as fh:
            process = Popen(command, stdout=fh)
            process.wait()
    except (CalledProcessError, IOError, OSError, RuntimeError, ValueError) as e:
        raise e

################################################################################
