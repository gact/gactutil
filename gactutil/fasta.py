#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''GACTutil package FASTA utilities.'''




def recode_fasta(infile, outfile, mapping):
    '''Rename FASTA sequences.
    
    Args:
        infile (str): input FASTA file
        outfile (str): output FASTA file
        mapping (list): mapping of old item names to new names
    
    Returns:
        None
    '''
    print('reaching recode_fasta OK')
    print(infile)
    print(outfile)
    print(mapping)

# rename header by default
# --id to rename IDs only

# >ref|NC_001133| [org=Saccharomyces cerevisiae] [strain=S288C] [moltype=genomic] [chromosome=I]
# >gi|290878045|emb|FN393060.2| Saccharomyces cerevisiae EC1118 chromosome II, EC1118_1B15 genomic scaffold, whole genome shotgun sequence

# [top=circular]

