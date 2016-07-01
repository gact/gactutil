#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil VCF utilities.

This module contains functions and utilities for handling data in VCF format.

The VCF filter function is a wrapper for the PyVCF filter script, and the filter
classes defined within this module are intended for use with that script.
"""

from __future__ import absolute_import
from argparse import ArgumentError
from subprocess import CalledProcessError
from subprocess import Popen
import sys

import numpy as np
from pysam import VariantFile
import vcf
from vcf.model import _Record

from gactutil import const
from gactutil import FrozenList
from gactutil import gactfunc
from gactutil import TextWriter
from gactutil.core import fsencode

################################################################################

const.vcf_fixed_headings = ('#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL',
    'FILTER', 'INFO')

################################################################################

class CallRate(vcf.filters.Base):
    """Threshold genotype call rate."""
    
    name = 'callrate'
    
    @classmethod
    def customize_parser(self, parser):
        parser.add_argument('callrate', type=float,
            help='Minimum required genotype call rate.')
    
    def __init__(self, args):
        self.threshold = args.callrate
    
    def __call__(self, record):
        """Pass if callrate meets threshold, 'FAIL' otherwise."""
        
        if record.call_rate < self.threshold:
            return 'FAIL'
        
        return None

class CategoricalFilter(vcf.filters.Base):
    """Base class for categorical variant filters."""
    
    name = 'CategoricalFilter'
    
    def __init__(self, *args):
        """Create filter without arguments."""
        if self.__class__.__name__ == 'CategoricalFilter':
            raise NotImplementedError("CategoricalFilter is an abstract class")
    
    def filter_name(self):
        """Return categorical filter name."""
        return self.name

class DiallelicObservations(CategoricalFilter):
    """Choose only variants with two observed alleles."""
    
    name = 'dao'
    
    def __call__(self, record):
        """Pass if diallelic observations, 'FAIL' otherwise.
        
        NB: upstream deletions are excluded from allele counts.
        """
        
        observed_allele_count = _get_observed_allele_count(record)
        
        if observed_allele_count != 2:
            return 'FAIL'
        
        return None

class DiallelicVariant(CategoricalFilter):
    """Choose only variants with two alleles."""
    
    name = 'dav'
    
    def __call__(self, record):
        """Pass if diallelic variant, 'FAIL' otherwise.
        
        NB: upstream deletions are excluded from allele counts.
        """
        
        allele_count = _get_variant_allele_count(record)
        
        if allele_count != 2:
            return 'FAIL'
        
        return None

class MnpOnly(CategoricalFilter):
    """Choose only MNP variants."""
    
    name = 'mnp-only'
    
    def __call__(self, record):
        """Pass if MNP variant, 'FAIL' otherwise."""
        
        ref_allele  = record.REF
        alt_alleles = [ unicode(str(x), encoding='utf_8') for x in record.ALT ]
        
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

class PolyallelicObservations(CategoricalFilter):
    """Choose only variants with more than two observed alleles."""

    name = 'pao'
    
    def __call__(self, record):
        """Pass if polyallelic observations, 'FAIL' otherwise.
        
        NB: upstream deletions are excluded from allele counts.
        """
        
        observed_allele_count = _get_observed_allele_count(record)
        
        if not observed_allele_count > 2:
            return 'FAIL'
        
        return None

class PolyallelicVariant(CategoricalFilter):
    """Choose only variants with more than two alleles."""

    name = 'pav'
    
    def __call__(self, record):
        """Pass if polyallelic variant, 'FAIL' otherwise.
        
        NB: upstream deletions are excluded from allele counts.
        """
        
        allele_count = _get_variant_allele_count(record)
        
        if not allele_count > 2:
            return 'FAIL'
        
        return None

class PolymorphicObservations(CategoricalFilter):
    """Choose only variants with two or more observed alleles."""
    
    name = 'pmo'
    
    def __call__(self, record):
        """Pass if polymorphic observations, 'FAIL' otherwise.
        
        NB: upstream deletions are excluded from allele counts.
        """
        
        observed_allele_count = _get_observed_allele_count(record)
        
        if not observed_allele_count > 1:
            return 'FAIL'
        
        return None

class PolymorphicVariant(CategoricalFilter):
    """Choose only variants with two or more alleles."""

    name = 'pmv'
    
    def __call__(self, record):
        """Pass if polymorphic variant, 'FAIL' otherwise.
        
        NB: upstream deletions are excluded from allele counts.
        """
        
        allele_count = _get_variant_allele_count(record)
        
        if not allele_count > 1:
            return 'FAIL'
        
        return None

class SvOnly(CategoricalFilter):
    """Choose only structural variants."""

    name = 'sv-only'

    def __call__(self, record):
        """Pass if structural variant, 'FAIL' otherwise."""
        
        if record.is_monomorphic:
            return 'FAIL'
        
        if not record.is_sv:
            return 'FAIL'
        
        return None

################################################################################

def _get_observed_allele_count(record, include_upstream_deletions=False):
    u"""Get number of observed alleles in VCF record."""
    
    if not isinstance(record, _Record):
        raise TypeError("expected a PyVCF record object, not {!r}".format(
            type(record).__name__))
    
    # Get alleles in VCF record.
    # NB: None corresponds to VCF missing value '.'
    alleles = [ str(a) for a in record.alleles if a is not None ]
    
    genotyped_alleles = set()
    
    for sample in record.samples:
        
        try:
            called_genotype = sample['GT']
        except AttributeError:
            raise ValueError("no genotype found for sample {!r} at position {} of {!r}".format(
                sample.sample, record.POS, record.CHROM))
        
        if genotype_string is not None:
            
            sep = '|' if sample.phased else '/'
            
            called_alleles = called_genotype.split(sep)
            
            genotyped_alleles = genotyped_alleles.union( allele
                for allele in called_alleles if allele != '.' )
    
    # Remove upstream deletions if appropriate.
    if not include_upstream_deletions:
        try:
            i = alleles.index('*')
        except ValueError: # upstream deletion not among variant alleles
            pass
        else:
            try:
                genotyped_alleles.remove(i)
            except KeyError: # upstream deletion not among sample genotypes
                pass
    
    return len(genotyped_alleles)

def _get_variant_allele_count(record, include_upstream_deletions=False):
    u"""Get number of alleles in VCF record."""
    
    if not isinstance(record, _Record):
        raise TypeError("expected a PyVCF record object, not {!r}".format(
            type(record).__name__))
    
    # Get alleles in VCF record.
    # NB: None corresponds to VCF missing value '.'
    alleles = set([ str(a) for a in record.alleles if a is not None ])
    
    # Remove upstream deletions if appropriate.
    if not include_upstream_deletions:
        try:
            alleles.remove('*')
        except KeyError:
            pass
    
    return len(alleles)

################################################################################

@gactfunc
def filter_vcf_variants(infile, outfile, filters, no_short_circuit=False, 
    no_filtered=False):
    u"""Filter variants in VCF file.
    
    Args:
        infile (unicode): Input VCF file.
        outfile (unicode): Output filtered VCF file.
        filters (FrozenList): Filter specifications.
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
    
    if any( not isinstance(filter_spec, basestring) for filter_spec in filters ):
        raise TypeError("filters must be of string type, not {!r}".format(
            type(filter_spec).__name__))
    
    # Assemble filter specification from individual parts.
    filter_spec = [ token for filter_spec in filters 
        for token in filter_spec.split() ]
    
    if any( x in filter_spec for x in known_core_options ):
        raise ArgumentError("filter specification contains core options")
    
    # Assemble command for PyVCF filter script.
    command = ['vcf_filter.py'] + core_options + [infile] + filter_spec
    command = [ fsencode(x) for x in command ]
    
    # Run PyVCF filter script.
    try:
        with TextWriter(outfile) as fh: # TODO: newline option
            process = Popen(command, stdout=fh)
            process.wait()
    except (CalledProcessError, IOError, OSError, RuntimeError, ValueError) as e:
        raise e

@gactfunc
def get_vcf_persample_depth(infile):
    u"""Get mean per-sample depth of VCF variants.
    
    Args:
        infile (unicode): Input VCF file.
    
    Returns:
        float: Mean per-sample depth of variants in VCF file.
    """
    return float( np.nanmean( get_vcf_persample_depths(infile) ) )

@gactfunc
def get_vcf_persample_depths(infile):
    u"""Get per-sample depths of VCF variants.
    
    Args:
        infile (unicode): Input VCF file.
    
    Returns:
        FrozenList: List of variant depths in VCF file.
    """
    
    # TODO: improve
    
    persample_depths = list()
    
    with VariantFile(infile) as reader:
        
        num_samples = len(reader.header.samples)
        
        for record in reader:
            
            record_sample_depths = [np.nan] * num_samples
            
            try:
                for i, sample in enumerate(record.samples):
                    
                    # NB: PySAM expects bytestring key
                    if record.samples[sample]['DP'] is not None:
                        record_sample_depths[i] = record.samples[sample]['DP']
                
            except KeyError:
                raise RuntimeError("depth not found for variant at position {} of {} in file {!r}".format(
                    record.pos, record.chrom, infile))
            
            persample_depths.append(record_sample_depths)
    
    return FrozenList(persample_depths)

################################################################################
