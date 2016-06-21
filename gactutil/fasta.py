#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil FASTA utilities."""

from Bio import SeqIO

from gactutil import FrozenList
from gactutil import TextReader
from gactutil import gactfunc

@gactfunc
def get_fasta_headers(infile):
    u"""Get headers of FASTA file.
    
    Args:
        infile (unicode): Input FASTA file.
        
    Returns:
        FrozenList: FASTA headers.
    """
    with TextReader(infile) as fh:
        headers = FrozenList( record.description
            for record in SeqIO.parse(fh, 'fasta') )
    return headers

def recode_fasta(infile, outfile, mapping):
    u"""Recode FASTA data.
    
    Args:
        infile (unicode): Input FASTA file.
        outfile (unicode): Output FASTA file.
        mapping (FrozenDict): Mapping of old item names to new names.
    """
    pass

################################################################################
