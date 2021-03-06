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
from pysam import AlignmentFile
from pysam import VariantFile
import vcf
from vcf.model import _Record
from vcf.parser import _Contig

from gactutil import const
from gactutil import FrozenDict
from gactutil import FrozenList
from gactutil import gactfunc
from gactutil import TextReader
from gactutil import TextWriter
from gactutil.core import dropped_tempfile
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
        
        if called_genotype is not None:
            
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
def count_vcf_variants(infile):
    u"""Get number of variants in VCF file.
    
    Args:
        infile (unicode): Input VCF file.
    
    Returns:
        int: Number of variants in VCF file.
    """
    num_variants = 0
    with TextReader(infile) as fh:
        reader = vcf.Reader(fh, compressed=False)
        for record in reader:
            num_variants += 1
    return num_variants

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

@gactfunc
def rename_vcf_contigs(infile, mapping, outfile):
    u"""Rename contigs in VCF file.
    
    Contigs are renamed in VCF header metainfo
    and in the 'CHROM' field of each VCF record.
    
    Args:
        infile (unicode): Input VCF file.
        mapping (FrozenDict): Contig ID mapping.
        outfile (unicode): Output VCF file.
    """
    
    for item in mapping.items():
        for x in item:
            if not isinstance(x, basestring):
                raise TypeError("contig IDs must be of string type, not {!r}".format(
                    type(x).__name__))
    
    contig_ids = list()
    with dropped_tempfile() as tempfile:
        
        # First pass: copy to temp file unmodified,
        # while getting list of existing contig IDs.
        with TextReader(infile) as fin:
            
            reader = vcf.Reader(fin, compressed=False)
            
            with TextWriter(tempfile) as ftmp:
                
                writer = vcf.Writer(ftmp, template=reader)
                
                for record in reader:
                    k = record.CHROM
                    if k not in contig_ids:
                        contig_ids.append(k)
                    writer.write_record(record)
        
        # Update contig ID mapping to include only input VCF contigs.
        mapping = mapping.thaw()
        for k in mapping.keys():
            if k not in contig_ids:
                del mapping[k]
        for k in contig_ids:
            if k not in mapping:
                mapping[k] = k
        
        # Check that the updated contig mapping is one-to-one.
        old_ids = mapping.keys()
        new_ids = mapping.values()
        if len(old_ids) != len(set(old_ids)) or len(new_ids) != len(set(new_ids)):
            raise RuntimeError("contig ID mapping is not one-to-one")
        
        # Second pass: copy temp file to output, while
        # renaming contigs in header metainfo and variant records.
        with TextReader(tempfile) as ftmp:
            
            reader = vcf.Reader(ftmp, compressed=False)
            
            if len(reader.contigs) > 0:
                
                contig_metainfo = dict()
                
                for k in contig_ids:
                    
                    try:
                        contig_record = reader.contigs[k]
                    except KeyError:
                        raise RuntimeError("metainfo not found for contig: {!r}".format(k))
                    
                    contig_metainfo[k] = _Contig(mapping[k], contig_record.length)
                
                reader.contigs.clear()
                
                for k in contig_ids:
                    reader.contigs[k] = contig_metainfo[k]
            
            with TextWriter(outfile) as fout:
                
                writer = vcf.Writer(fout, template=reader)
                
                for record in reader:
                    record.CHROM = mapping[ record.CHROM ]
                    writer.write_record(record)

@gactfunc
def set_vcf_contig_metainfo(infile, outfile, seq_dict=None):
    u"""Set contig metainfo in VCF file header.
    
    Reference sequence information is taken from the specified sequence
    dictionary SAM file, converted to equivalent VCF metainfo fields
    (e.g. 'SN:chr01' to 'ID=chr01'), and written to metainfo records
    in the header of the given VCF file.
    
    Args:
        infile (unicode): Input VCF file.
        seq_dict (unicode): Sequence dictionary SAM text file.
        outfile (unicode): Output VCF file.
    """
    
    if seq_dict is None:
        raise ValueError("no contig metainfo source specified")
    
    # Get contig metainfo from sequence dictionary file.
    contig_metainfo = dict()
    with AlignmentFile(seq_dict, 'r') as reader:
        
        for refseq_record in reader.header['SQ']:
            
            try:
                rid = refseq_record['SN']
                length = refseq_record['LN']
            except KeyError:
                raise KeyError("required 'SQ' field not found in SAM file: {!r}".format(seq_dict))
            
            contig_metainfo[rid] = _Contig(rid, length)
    
    contig_ids = list()
    with dropped_tempfile() as tempfile:
        
        # First pass: copy to temp file unmodified,
        # while getting list of contig IDs.
        with TextReader(infile) as fin:
            
            reader = vcf.Reader(fin, compressed=False)
            
            with TextWriter(tempfile) as ftmp:
                
                writer = vcf.Writer(ftmp, template=reader)
                
                for record in reader:
                    k = record.CHROM
                    if k not in contig_ids:
                        contig_ids.append(k)
                    writer.write_record(record)
        
        for k in contig_ids:
            if k not in contig_metainfo:
                raise RuntimeError("metainfo not found for contig: {!r}".format(k))
        
        # Second pass: copy temp file to output,
        # while setting contig metainfo in header.
        with TextReader(tempfile) as ftmp:
            
            reader = vcf.Reader(ftmp, compressed=False)
            
            for k in reader.contigs.keys():
                del reader.contigs[k]
            
            for k in contig_ids:
                reader.contigs[k] = contig_metainfo[k]
            
            with TextWriter(outfile) as fout:
                
                writer = vcf.Writer(fout, template=reader)
                
                for record in reader:
                    writer.write_record(record)

################################################################################
