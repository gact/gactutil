#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil chromosome utilities."""

import re

from gactutil import const

################################################################################

# This pattern is based on the method used by SnpEff to simplify chromosome
# names, as in the file 'ChromosomeSimpleName.java'. Prefixes of the form 
# 'chr', 'chromo', and 'chromosome' are removed, as well as leading zeroes
# in the chromosome number.
# (See https://github.com/pcingola/SnpEff [Accessed: 16 Feb 2016].)
_chrom_id_regex = re.compile(u'^(?:chr(?:omo(?:some)?)?)?0*(\S+)$')

# Normalised representations of yeast chromosomes. These
# were chosen to sort consistently in most circumstances.
_chrom_id_list = (
  u'chr01',
  u'chr02',
  u'chr03',
  u'chr04',
  u'chr05',
  u'chr06',
  u'chr07',
  u'chr08',
  u'chr09',
  u'chr10',
  u'chr11',
  u'chr12',
  u'chr13',
  u'chr14',
  u'chr15',
  u'chr16',
  u'chr17',
  u'chr18'
)

# Mapping of chromosome labels to resolved representation.
# NB: this assumes chromosome name simplification has been
# done (removing any leading zeros), and that the given
# chromosome has been uppercased.
_chrom_id_mapping = {
  u'1':  u'chr01',    u'I': u'chr01',
  u'2':  u'chr02',   u'II': u'chr02',
  u'3':  u'chr03',  u'III': u'chr03',
  u'4':  u'chr04',   u'IV': u'chr04',
  u'5':  u'chr05',    u'V': u'chr05',
  u'6':  u'chr06',   u'VI': u'chr06',
  u'7':  u'chr07',  u'VII': u'chr07',
  u'8':  u'chr08', u'VIII': u'chr08',
  u'9':  u'chr09',   u'IX': u'chr09',
  u'10': u'chr10',    u'X': u'chr10',
  u'11': u'chr11',   u'XI': u'chr11',
  u'12': u'chr12',  u'XII': u'chr12',
  u'13': u'chr13', u'XIII': u'chr13',
  u'14': u'chr14',  u'XIV': u'chr14',
  u'15': u'chr15',   u'XV': u'chr15',
  u'16': u'chr16',  u'XVI': u'chr16',
  u'17': u'chr17',   u'MT': u'chr17',  u'MITO': u'chr17',
  u'2-MICRON': u'chr18', u'2MICRON': u'chr18'
}

################################################################################

def norm_chrom_id(chrom):
    u"""Resolve the specified chromosome ID.
    
    Args:
        chrom (unicode or int): Chromosome name/number.
    
    Returns:
        unicode: Resolved chromosome ID. Returns None on failure.
    """
    
    # Init resolved chromosome to None.
    res = None
    
    # If putative chromosome is an integer, convert to unicode..
    if isinstance(chrom, int):
    
        chrom = unicode(chrom)
    
    # ..otherwise if of type str, convert to unicode..
    elif isinstance(chrom, str):
        
        chrom = chrom.decode('utf_8').strip()
        
    # ..otherwise check of unicode type.
    elif isinstance(chrom, unicode):
        
        chrom = chrom.strip()
        
    else:
        raise TypeError("chromosome is not of string or integer type")
    
    # If putative chromosome is in the set of 
    # normalised chromosomes, set as normalised..
    if chrom in _chrom_id_list:
        
        res = chrom
    
    # ..otherwise try to map to a resolved chromosome.
    else:
        
        m = _chrom_id_regex.match(chrom)
        
        try:
            k = m.group(1).upper()
            res = _chrom_id_mapping[k]
        except (AttributeError, KeyError):
            pass
    
    return res

def norm_chrom_ids(chroms):
    u"""Resolve the specified chromosome IDs.
    
    Args:
        chroms (FrozenList): List of chromosome names/numbers.
    
    Returns:
        FrozenDict: Mapping of input chromosomes to their resolved form.
    """
    return FrozenDict({ c: norm_chrom_id(c) for c in chroms })

################################################################################
