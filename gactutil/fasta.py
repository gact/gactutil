#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
"""GACTutil FASTA module."""

from Bio import SeqIO

from gactutil import TextReader
from gactutil.gaction import gactfunc

@gactfunc
def get_fasta_headers(infile):
    """Get FASTA headers.
    
    Args:
        infile (str): Input FASTA file.
        
    Returns:
        list: FASTA headers.
    """
    with TextReader(infile) as fh:
        headers = [ record.description for record in SeqIO.parse(fh, 'fasta') ]
    return headers

def recode_fasta(infile, outfile, mapping):
    """Recode FASTA data.
    
    Args:
        infile (str): Input FASTA file.
        outfile (str): Output FASTA file.
        mapping (dict): Mapping of old item names to new names.
    """
    get_fasta(infile)
    pass

################################################################################
