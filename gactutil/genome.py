#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil genome module."""

from __future__ import absolute_import
from collections import deque
from collections import OrderedDict
from datetime import datetime
import os
from os.path import getmtime
from os.path import isfile
import re
from string import Template
import sys
from textwrap import dedent
import time

from BCBio import GFF
from Bio import SeqIO

from gactutil import TextReader
from gactutil import TextWriter
from gactutil import temporary_directory
from gactutil import unidump
from gactutil import uniload
from gactutil import YAMLError
from gactutil.gaction import gactfunc
from gactutil.ncbi import check_efetch

################################################################################

_info = {

    # Normalised representations of yeast chromosomes. These
    # were chosen to sort consistently in most circumstances.
    u'norm_chr': (
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
    ),
    
    # Mapping of chromosome labels to normalised representation.
    # NB: this assumes chromosome name simplification has been
    # done (removing any leading zeros), and that the given
    # chromosome has been uppercased.
    u'chr2norm': {
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
    },
    
    # Expected pattern of sequence identifiers beginning with a GI number.
    u'pattern': {
        u'gi': re.compile(u'^gi[|](\d+)[|].+')
    },
    
    # GFF header appendix template for genome prep output GFF file.
    u'header_appendix': dedent(u'''\
    # Original file created by Saccharomyces Genome Database <www.yeastgenome.org>.
    # This version created at ${TIMEPOINT}. 
    # Modified from the original at GACT <www2.le.ac.uk/colleges/medbiopsych/research/gact>.
    # Reference sequence names may have been changed.
    #
    # For enquiries on the original data, contact: sgd-helpdesk@lists.stanford.edu
    # For more information on changes made, contact: ${CONTACT}
    #
    ''')
}

################################################################################

class GenomeIndex(object):
    u"""Genome index class.
    
    Handles reading, writing, and updating a genome index YAML file.
    """

    # Filename patterns of known genome data files.
    pattern = {
        
        u'COMMON': {
            # Captures: <strain>
            u'README': re.compile(u'^([^.]+)[.]README$'),
            
            # Captures: <strain>_<ID>
            u'prepped-reference': re.compile(u'^SGD_([^_]+)_([^_]+)[.]fa$'),
            
            # Captures: <strain>_<ID>
            u'prepped-annotation': re.compile(u'^SGD_([^_]+)_([^_]+)[.]gff$'),
        },

        u'S288C': {
            # Captures: <version>_<date>
            u'coding-sequence': re.compile(u'^orf_coding_all_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'gene-association': re.compile(u'^gene_association_((R\d+-\d+-\d+)_(\d{8}))[.]sgd$'),
            
            # Captures: <version>_<date>
            u'reference': re.compile(u'^(S288C)_reference_sequence_((R\d+-\d+-\d+)_(\d{8}))[.]fsa$'),
            
            # Captures: <version>_<date>
            u'annotation': re.compile(u'^saccharomyces_cerevisiae_((R\d+-\d+-\d+)_(\d{8}))[.]gff$'),
            
            # Captures: <version>_<date>
            u'non-feature': re.compile(u'^NotFeature_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'other-feature': re.compile(u'^other_features_genomic_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'peptide-sequence': re.compile(u'^orf_trans_all_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'rna-sequence': re.compile(u'^rna_coding_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$')
        }, 
        
        u'NON-S288C': {
            
            # Captures: <strain>_<ID>
            u'coding-sequence': re.compile(u'^([^_]+)_([^_]+)_cds[.]fsa(?:[.]gz)?$'),
            
             # Captures: <strain>_<institution>_<year>_<ID>
            u'reference': re.compile(u'^([^_]+)_([^_]+)_([^_]+)_([^_]+)[.]fsa(?:[.]gz)?$'),
            
            # Captures: <strain>_<ID>
            u'annotation': re.compile(u'^([^_]+)_([^_]+)[.]gff(?:[.]gz)?$'),
            
            # Captures: <strain>
            u'indel': re.compile(u'^(.+)[.]indel[.]gatk[.]vcf(?:[.]gz)?$'),
            
            # Captures: <strain>_<ID>
            u'peptide-sequence': re.compile(u'^([^_]+)_([^_]+)_pep[.]fsa(?:[.]gz)?$'),
            
            # Captures: <strain>
            u'snp': re.compile(u'^(?!.+(?:[.]indel))(.+)[.]gatk[.]vcf(?:[.]gz)?$')
        }
    } 
    
    # Genome index tags.
    tags = {
    
        # Info about genome.
        u'info': (u'date', u'name', u'ID', u'institution', u'strain'),
        
        # Files in genome.
        u'files': (u'annotation', u'coding-sequence', u'gene-association',
            u'reference', u'indel', u'non-feature', u'other-feature',
            u'peptide-sequence', u'snp', u'prepped-annotation',
            u'prepped-reference', u'README', u'rna-sequence')
    }
    
    filename = u'index.yaml'
    
    required = (u'reference', u'annotation')
    
    @classmethod
    def _get_index_path(cls, directory):
        u"""Get path of genome index from genome directory path."""
        if not isinstance(directory, unicode):
            raise TypeError("directory path must be of type unicode, not {!r}".format(
                type(directory).__name__))
        return os.path.join( os.path.normpath(directory), GenomeIndex.filename )
        
    @property
    def files(self):
        u"""dict: Dictionary of genome file info."""
        return self._data[u'files']
    
    @property
    def info(self):
        u"""dict: Dictionary of genome info."""
        return self._data[u'info']
        
    def __init__(self, directory):
        u"""Init genome index.
        
        Args:
            directory (unicode): Path of genome directory.
        """
        
        # Get path of index file.
        index_path = GenomeIndex._get_index_path(directory)
        
        # Get paths to other files in genome directory.
        item_paths = [ os.path.join(directory, x)
            for x in os.listdir(directory)
            if x != GenomeIndex.filename ]
        file_paths = [ x for x in item_paths if os.path.isfile(x) ]
        
        # If index already exists, load it and count any
        # previously-indexed files that are missing.
        if os.path.isfile(index_path):
            
            try:
                self.load(directory)
            except RuntimeError:
                os.remove(index_path)
            else:
                indexed_paths = [ os.path.join(directory, x)
                    for x in self._data[u'files'].values() ]
                num_missing = sum([ int( not os.path.isfile(x) )
                    for x in indexed_paths ])
        
        # Update index if not found, or if there are newer files,
        # or if any previously-indexed files are missing.
        if ( not os.path.isfile(index_path) or num_missing > 0 or
            any( getmtime(x) > getmtime(index_path) for x in file_paths ) ):
            self.update(directory)
        
    def dump(self, directory):
        u"""Dump genome index to genome directory.
        
        Args:
            directory (unicode): Path of genome directory to which genome index
                will be dumped.
        """
        
        index_path = GenomeIndex._get_index_path(directory)
        
        try:
            with open(index_path, 'w') as fh:
                unidump(self._data, fh, default_flow_style=False, width=sys.maxint)
        except (IOError, YAMLError):
            raise RuntimeError("failed to dump genome index to directory ~ {!r}".format(directory))
    
    def load(self, directory):
        u"""Load genome index from genome directory.
        
        Args:
            directory (unicode): Path of genome directory from which genome index
                will be loaded.
        """
        
        index_path = GenomeIndex._get_index_path(directory)
        
        try:
            with open(index_path, 'r') as fh:
                self._data = uniload(fh)
        except (IOError, YAMLError):
            raise RuntimeError("failed to load genome index from directory ~ {!r}".format(directory))
        
        if ( self._data is None or any( k not in GenomeIndex.tags for k in self._data ) or
            any( a not in GenomeIndex.tags[k] for k in self._data for a in self._data[k] ) ):
            raise RuntimeError("failed to load invalid genome index from directory ~ {!r}".format(directory))

    def update(self, directory):
        u"""Update genome index for given genome directory.
        
        Args:
            directory (unicode): Path of genome directory for which this genome index
                will be updated.
        """
        
        # Init index data.
        self._data = { u'info': dict(), u'files': dict() }
        
        if not isinstance(directory, unicode):
            raise TypeError("directory path must be of type unicode, not {!r}".format(
                type(directory).__name__))
        
        # Get list of files in genome directory.
        files = [ x for x in os.listdir(directory)
            if os.path.isfile( os.path.join(directory, x) ) ]
        
        # Match files against expected filename patterns.
        gmatch = { k: { f: set([ GenomeIndex.pattern[k][f].match(x) 
            for x in files ]) for f in GenomeIndex.pattern[k] } 
            for k in GenomeIndex.pattern }
        
        # Prune unmatched patterns.
        for k in gmatch.keys():
            for t in gmatch[k].keys():
                if None in gmatch[k][t]:
                    gmatch[k][t].remove(None)
                if len(gmatch[k][t]) == 0:
                    del gmatch[k][t]
                elif len(gmatch[k][t]) == 1:
                    gmatch[k][t] = gmatch[k][t].pop()
                else:
                    raise RuntimeError("multiple {!r} files found".format(t))
            if len(gmatch[k]) == 0:
                del gmatch[k]
        
        # Get genome type cues ('S288C' or 'NON-S288C') from matching files.
        genome_type_cues = [ k for k in gmatch.keys() if k != u'COMMON' ]
        
        # Set genome type if available cues clearly indicate one type of genome.
        if len(genome_type_cues) == 1:
            genome_type = genome_type_cues[0]
        elif len(genome_type_cues) > 1:
            raise RuntimeError("cannot update genome index - conflicting genome files found")
        else:
            raise RuntimeError("cannot update genome index - genome files not found")
    
        # Check that genome directory contains required files.
        if any( k not in gmatch[genome_type] for k in GenomeIndex.required ):
            raise RuntimeError("cannot update genome index - required files not found")
        
        # Update genome info.
        if genome_type == u'S288C':
            
            genome_match = gmatch[u'S288C'][u'reference']
            self._data[u'info'][u'strain'] = genome_match.group(1)
            self._data[u'info'][u'institution'] = u'SGD'
            self._data[u'info'][u'ID'] = genome_match.group(3)
            self._data[u'info'][u'date'] = genome_match.group(4)
            
        elif genome_type == u'NON-S288C':
            
            genome_match = gmatch[u'NON-S288C'][u'reference']
            self._data[u'info'][u'strain'] = genome_match.group(1)
            self._data[u'info'][u'institution'] = genome_match.group(2)
            self._data[u'info'][u'ID'] = genome_match.group(4)
            
            if u'README' in gmatch[u'COMMON']:
                readme = _load_genome_readme( os.path.join(directory,
                    gmatch[u'COMMON'][u'README'].group(0)) )
                self._data[u'info'][u'date'] = unicode(
-                   readme[u'date'].strftime('%Y%m%d') )
            else:
                self._data[u'info'][u'date'] = unicode(
-                   int(genome_match.group(3)) )
        
        # Set genome name from strain and ID.
        self._data[u'info'][u'name'] = u'SGD_{}_{}'.format(
            self._data[u'info'][u'strain'], self._data[u'info'][u'ID'])
        
        # Update file info.
        for k in gmatch.keys():
            for t in gmatch[k].keys():
                self._data[u'files'][t] = gmatch[k][t].group(0)

################################################################################

def _norm_chr(chromosome):
    u"""Normalise chromosome name.
    
    Args:
        chromosome: Chromosome name.
    
    Returns:
        unicode: Normalised chromosome name. Returns None on failure.
    """
    
    # This pattern is modelled on the method used by SnpEff to simplify 
    # chromosome names, as in the file 'ChromosomeSimpleName.java'. Prefixes of
    # the form 'chr', 'chromo', and 'chromosome' are removed, as well as leading
    # zeroes in the chromosome number and known delimiters (i.e. ':', '_', '-').
    # (See https://github.com/pcingola/SnpEff [Accessed: 16 Feb 2016].)
    chromosome_pattern = re.compile(u'^(?:chr(?:omo(?:some)?)?)?0*(\S+)$')

    # Init normalised chromosome to None.
    result = None
    
    # If putative chromosome is defined, try to normalise.
    if chromosome is not None:
        
        # If putative chromosome is an integer, convert to unicode..
        if isinstance(chromosome, (int, long)):
        
            chromosome = unicode(chromosome)
        
        # ..otherwise if of type str, convert to unicode..
        elif isinstance(chromosome, str):
            
            chromosome = chromosome.decode('utf_8').strip()
            
        # ..otherwise check of unicode type.
        elif isinstance(chromosome, unicode):
            chromosome = chromosome.strip()
        else:
            raise TypeError("chromosome is not a string or integer")
        
        # If putative chromosome is in the set of 
        # normalised chromosomes, set as normalised..
        if chromosome in _info[u'norm_chr']:
            result = chromosome
        
        # ..otherwise try to map to a normalised chromosome.
        else:
            m = chromosome_pattern.match(chromosome)
            
            try:
                k = m.group(1).upper()
                result = _info[u'chr2norm'][k]
            except (AttributeError, KeyError):
                pass
                
    return result

def _load_genome_readme(filepath):
    u"""Load SGD genome README file."""
    
    with TextReader(filepath) as fh:
        
        lines = [ line.rstrip() for line in fh.readlines() ]
        
        try:
            assert all( lines[i] == u'' for i in (1, 3) )
            
            heading = lines[0]
            date = datetime.strptime(lines[2], '%m/%d/%Y').date()
            body = deque( lines[4:] )
            
            # Strip blank lines from beginning and end of README body.
            while len(body) > 0 and body[0].strip() == u'':
                body.popleft()
            while len(body) > 0 and body[-1].strip() == u'':
                body.pop()
            
            body = u'\n'.join(body)
            
            result = OrderedDict([
                (u'heading', heading),
                (u'date', date),
                (u'body', body)
            ])
            
        except (AssertionError, IndexError):
            raise RuntimeError("unexpected README format")
    
    return result

################################################################################

@gactfunc
def index_genome(directory):
    u"""Index yeast genome data.
    
    This takes as input a directory containing genome assembly files downloaded 
    from the Saccharomyces Genome Database (SGD). It indexes the data files in 
    the input directory and saves these to a genome index file in YAML format.
    
    Args:
        directory (unicode): Path to yeast genome data directory.
    """
    gindex = GenomeIndex(directory)
    gindex.dump(directory)

@gactfunc
def prep_genome(directory, email=None):
    u"""Prep yeast genome data.
    
    This takes as input a directory containing genome assembly files downloaded 
    from the Saccharomyces Genome Database (SGD). It processes the SGD data 
    files in the input directory and prepares a genome sequence file and 
    annotation file. These prepped files will have consistent reference sequence 
    names and are suitable as input for downstream bioinformatics analysis.

    Several changes are made to the reference sequence and annotation files:

     * The reference sequence FASTA file is given the extension '.fa', which is 
       more commonly used, and is required by some software (e.g. Picard).

     * Each reference sequence is assigned to a chromosome if possible, and
       renamed to improve readability. If a reference sequence is a chromosome, 
       its name is changed to a normalised representation (e.g. 'chr01').
       Otherwise, if a reference sequence ID contains a GI number, this is used 
       to query NCBI for the name of the sequence and its corresponding 
       chromosome. In such cases, the new sequence name is concatenated with the 
       normalised chromosome name. An error results if the sequence cannot be
       assigned to a chromosome. Chromosome assignment is necessary because 
       yeast mitochondrial chromosomes are different to nuclear chromosomes - 
       with circular topology and a variant genetic code - in such a way that 
       this can affect downstream analyses (e.g. SnpEff annotation).
   
     * Sequences are sorted by chromosome, then alphabetically by sequence name. 
       Annotation records are further sorted by the start and end coordinates of 
       each feature.

    Note that some annotation files in SGD are not in GFF3 format — these must 
    be converted to GFF3 format before input to this script.
    
    Args:
        directory (unicode): Path to yeast genome data directory.
        email (unicode): Contact email for NCBI E-utilities.
    """
    
    # PROCESS ... ##############################################################
    
    gindex = GenomeIndex(directory)
    
    # Check annotation file found.
    if u'annotation' not in gindex.files:
        raise RuntimeError("cannot prep genome - annotation file not found")
    
    # Set output genome sequence file path.
    sequence_file = os.path.join(directory, u'{}.fa'.format(gindex.info[u'name']))
    
    # Set output genome annotation file path.
    annotation_file = os.path.join(directory, u'{}.gff'.format(gindex.info[u'name']))
    
    # Set sequence definition pattern.
    seq_def_pattern = re.compile(u"^Saccharomyces cerevisiae(?: strain)? (\S+)"
        u"(?: chromosome (\S+?),?)? (\S+)(?: genomic scaffold)?, whole genome "
        u"(?:shotgun)? sequence$", re.IGNORECASE)
    
    # Set GFF pragma patterns.
    pragma_pattern = {
        u'version': re.compile(u'^##gff-version (.+)$'),
        u'forward-reference-resolution': re.compile(u'^###$'),
        u'fasta': re.compile(u'^##FASTA$')
    }
    
    # Set GenBank query frequency: at least one second between queries.
    genbank_frequency = 1.0
    
    # Set GenBank query timer.
    genbank_timer = time.time()
    
    # READ GFF FILE ############################################################
    
    anno_path = os.path.join(directory, gindex.files[u'annotation'])
    
    # Read GFF pragmas and comments.
    with TextReader(anno_path) as fh:
        
        pragmas, comments = [ list() for _ in range(2) ]
        
        # Read all GFF comment lines.
        comment_lines = [ line.rstrip()
            for line in fh.readlines()
            if line.startswith(u'#') ]
            
        # Separate pragmas and comments.
        for line in comment_lines:
            if line.startswith(u'##'):
                pragmas.append(line)
            else: 
                comments.append(line)
        
        # Validate pragmas.
        for pragma in pragmas:
            pragmatch = { k: pragma_pattern[k].match(pragma) 
                for k in pragma_pattern }
            matching_pragma_types = [ k for k in pragma_pattern 
                if pragmatch[k] is not None ]
            if len(matching_pragma_types) == 0:
                raise ValueError("unknown pragma ~ {!r}".format(pragma))
        
        # Set GFF header comments.
        header_comments = u'\n'.join(comments)
    
    # Read and modify GFF records.
    with TextReader(anno_path) as fh:
        
        try:
            records = [ x for x in GFF.parse(fh) ]
        except AssertionError:
            raise RuntimeError("invalid annotation file ~ {!r}".format(anno_path))
        
        for i, record in enumerate(records):
            
            # Init new sequence name and corresponding chromosome.
            seq_id, seq_chr = [None] * 2
            
            # Check if sequence name matches expected pattern  
            # for a sequence header containing a GI number.
            gi_match = _info[u'pattern'][u'gi'].match(record.id)
                    
            # If sequence header appears to contain GI number, 
            # query GenBank for relevant sequence information..
            if gi_match is not None:
                
                # Get GI number from sequence header.
                gi = gi_match.group(1)
                
                # Polite delay until GenBank timer expires.
                if time.time() < genbank_timer:
                    polite_delay = genbank_timer - time.time()
                    time.sleep( polite_delay )
                
                # Fetch GenBank entry for GI number.
                result = check_efetch(db=u'nucleotide', id=gi, retmode=u'xml',
                    email=email)
                
                # Reset GenBank timer.
                genbank_timer = time.time() + genbank_frequency
                
                # Get sequence definition.
                seq_def = result[0][u'GBSeq_definition'].decode('utf_8')
            
                # Check if sequence definition matches expected pattern.
                seq_def_match = seq_def_pattern.match(seq_def)
            
                # Check sequence definition matches expected pattern.
                if seq_def_match is None:
                    raise RuntimeError("failed to identify chromosome of GenBank entry gi:{}".format(gi))
                
                # Get new sequence name and corresponding chromosome.
                seq_id, seq_chr = seq_def_match.group(3, 2) 
                
                # Normalise chromosome name.
                seq_chr = _norm_chr(seq_chr)
                
                # Incorporate chromosome in sequence name.
                seq_id = u'{}_{}'.format(seq_chr, seq_id)
                
            # ..otherwise check if sequence is itself a chromosome.
            else:
                
                # Assuming sequence represents a chromosome, 
                # normalise chromosome name.
                seq_chr = _norm_chr(record.id)
                
                # If chromosome normalised, map sequence to chromosome.
                if seq_chr is not None:
                    seq_id = seq_chr
                    
            # Check sequence name and associated chromosome identified.
            if seq_id is None:
                raise RuntimeError("failed to identify name or chromosome of {!r}".format(record.id))
            
            # Check that a chromosome was identified.
            if seq_chr is None:
                raise RuntimeError("failed to identify chromosome of {!r}".format(record.id))
            
            # Set new sequence ID.
            record.description = record.name = ''
            record.id = seq_id
            
            # If sequence is described in a feature record, 
            # update ID attribute to reflect new sequence ID.
            for j, feature in enumerate(record.features):
                if feature.type in (u'chromosome', u'contig'):
                    feature.id = seq_id
                    record.features[j] = feature
            
            records[i] = record

    # WRITE OUTPUT FILES #######################################################
    
    # Get timepoint and timestamp.
    dt = datetime.now()
    timepoint = unicode( dt.strftime('%H:%M:%S on %A %d %B %Y') )
    # timestamp = dt.strftime('%a %b %d %H:%M:%S %Y')
    
    # Set general GFF header appendix.
    template = Template( _info[u'header_appendix'] )
    additional_comments = template.substitute({
        u'TIMEPOINT': timepoint,
        u'SCRIPT': 'GACTutil',
        u'CONTACT': email
    })
    header_comments = u'\n'.join([header_comments,
        additional_comments])
    
    # Get serialised annotation data.
    with temporary_directory() as twd:
        
        annotation_temp = os.path.join(twd, u'annotation.tmp')
        
        with TextWriter(annotation_temp) as fh:
            GFF.write(records, fh)
            
        with TextReader(annotation_temp) as fh:
            lines = fh.readlines()
            version_pragma = lines[0]
            annotation_data = u''.join(lines[1:])
    
    # Check that BCBio GFF output starts with a version pragma.
    if pragma_pattern[u'version'].match(version_pragma) is None:
        raise RuntimeError("version pragma not found in BCBio GFF output")
    
    # Write output annotation file.
    with TextWriter(annotation_file) as fh:
        fh.write(version_pragma)
        fh.write(header_comments)
        fh.write(annotation_data)
    
    # Write output sequence file.
    with TextWriter(sequence_file) as fh:
        for record in records:
            SeqIO.write(record, fh, 'fasta')
    
    gindex.update(directory)
    gindex.dump(directory)

################################################################################
