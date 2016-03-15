#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''GACTutil genome module.'''

from collections import deque
from collections import OrderedDict
from datetime import datetime
import os
from os.path import getmtime
from os.path import isfile
import re
from string import Template
from textwrap import dedent
import time

from BCBio import GFF
from Bio import SeqIO
from yaml import safe_dump
from yaml import safe_load
from yaml import YAMLError

from gactutil.ncbi import check_efetch
from gactutil import prise
from gactutil import TemporaryDirectory

################################################################################

_info = {

    # A normalised representations of yeast chromosomes. For the most part
    # these reflect the nomenclature in use by SGD as of March 2016.
    'norm-chr': (
      'chrI',
      'chrII',
      'chrIII',
      'chrIV',
      'chrV',    
      'chrVI',   
      'chrVII',
      'chrVIII',
      'chrIX',
      'chrX',
      'chrXI',
      'chrXII',
      'chrXIII',
      'chrXIV',
      'chrXV',
      'chrXVI',
      'chrM', 
      '2micron'
    ),
    
    # Mapping of chromosome labels to normalised representation. 
    # NB: this assumes chromosome name simplification has been 
    # done, and that the given chromosome has been uppercased.
    'chr2norm': {
      '1': 'chrI',      'I':    'chrI', 
      '2': 'chrII',     'II':   'chrII',
      '3': 'chrIII',    'III':  'chrIII',
      '4': 'chrIV',     'IV':   'chrIV',
      '5': 'chrV',      'V':    'chrV',
      '6': 'chrVI',     'VI':   'chrVI',
      '7': 'chrVII',    'VII':  'chrVII',
      '8': 'chrVIII',   'VIII': 'chrVIII', 
      '9': 'chrIX',     'IX':   'chrIX',
      '10': 'chrX',     'X':    'chrX',
      '11': 'chrXI',    'XI':   'chrXI',
      '12': 'chrXII',   'XII':  'chrXII',
      '13': 'chrXIII',  'XIII': 'chrXIII',
      '14': 'chrXIV',   'XIV':  'chrXIV',
      '15': 'chrXV',    'XV':   'chrXV',
      '16': 'chrXVI',   'XVI':  'chrXVI',
      '17': 'chrM',     'MT':   'chrM',    'MITO': 'chrM',
      '2-micron': '2micron'
    },
    
    # Expected pattern of sequence identifiers beginning with a GI number.
    'pattern': {
        'gi': re.compile('^gi[|](\d+)[|].+')
    },
    
    # GFF header appendix template for genome prep output GFF file.
    'header-appendix': dedent('''\
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
    '''Genome index class.
    
    Handles reading, writing, and updating a genome index YAML file.
    '''

    # Filename patterns of known genome data files.
    pattern = {
        
        'COMMON': {
            # Captures: <strain>
            'README': re.compile('^([^.]+)[.]README$'),
            
            # Captures: <strain>_<ID>
            'prepped-genome': re.compile('^SGD_([^_]+)_([^_]+)[.]fa$'),
            
            # Captures: <strain>_<ID>
            'prepped-annotation': re.compile('^SGD_([^_]+)_([^_]+)[.]gff$'),
        },

        'S288C': {
            # Captures: <version>_<date>
            'coding-sequence': re.compile('^orf_coding_all_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            'gene-association': re.compile('^gene_association_((R\d+-\d+-\d+)_(\d{8}))[.]sgd$'),
            
            # Captures: <version>_<date>
            'genome': re.compile('^(S288C)_reference_sequence_((R\d+-\d+-\d+)_(\d{8}))[.]fsa$'),
            
            # Captures: <version>_<date>
            'annotation': re.compile('^saccharomyces_cerevisiae_((R\d+-\d+-\d+)_(\d{8}))[.]gff$'),
            
            # Captures: <version>_<date>
            'non-feature': re.compile('^NotFeature_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            'other-feature': re.compile('^other_features_genomic_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            'peptide-sequence': re.compile('^orf_trans_all_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            'rna-sequence': re.compile('^rna_coding_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$') 
        }, 
        
        'NON-S288C': {
            
            # Captures: <strain>_<ID>
            'coding-sequence': re.compile('^([^_]+)_([^_]+)_cds[.]fsa(?:[.]gz)?$'),
            
             # Captures: <strain>_<institution>_<year>_<ID>
            'genome': re.compile('^([^_]+)_([^_]+)_([^_]+)_([^_]+)[.]fsa(?:[.]gz)?$'),
            
            # Captures: <strain>_<ID>
            'annotation': re.compile('^([^_]+)_([^_]+)[.]gff(?:[.]gz)?$'),
            
            # Captures: <strain>
            'indel': re.compile('^(.+)[.]indel[.]gatk[.]vcf(?:[.]gz)?$'),
            
            # Captures: <strain>_<ID>
            'peptide-sequence': re.compile('^([^_]+)_([^_]+)_pep[.]fsa(?:[.]gz)?$'),
            
            # Captures: <strain>
            'snp': re.compile('^(?!.+(?:[.]indel))(.+)[.]gatk[.]vcf(?:[.]gz)?$')
        }
    } 
    
    # Genome index tags.
    tags = {
    
        # Info about genome.
        'info': ('date', 'name', 'ID', 'institution', 'strain'),
        
        # Files in genome.
        'files': ('annotation', 'coding-sequence', 'gene-association', 'genome', 
            'indel', 'non-feature', 'other-feature', 'peptide-sequence', 'snp',
            'prepped-annotation', 'prepped-genome', 'README', 'rna-sequence')
    }
    
    filename = 'index.yaml'
    
    required = ('genome', 'annotation')
    
    @classmethod
    def _get_index_path(cls, path):
        '''Get path of genome index from genome directory path.'''
        return os.path.join( os.path.normpath(path), GenomeIndex.filename )
        
    @property
    def files(self):
        '''dict: Dictionary of genome file info.'''
        return self._data['files']
    
    @property
    def info(self):
        '''dict: Dictionary of genome info.'''
        return self._data['info']
        
    def __init__(self, path):
        '''Init genome index.
        
        Args:
            path (str): Path of genome directory.
        '''
        
        # Get path of index file.
        index_path = GenomeIndex._get_index_path(path)
        
        # Get paths to other files in genome directory.
        item_paths = [ os.path.join(path, x) for x in os.listdir(path) 
            if x != GenomeIndex.filename ]
        file_paths = [ x for x in item_paths if os.path.isfile(x) ]
        
        # If index doesn't exist or is older than any other file, update index..
        if not isfile(index_path) or any( getmtime(x) > getmtime(index_path) 
            for x in file_paths ):
            self.update(path)
        # ..otherwise load existing index.
        else:
            self.load(path)
        
    def dump(self, path):
        '''Dump genome index to genome directory.
        
        Args:
            path (str): Path of genome directory to which genome index 
                will be dumped.
        '''
        
        index_path = GenomeIndex._get_index_path(path)
        
        try:
            with open(index_path, 'w') as fh:
                safe_dump(self._data, fh, default_flow_style=False)
        except (IOError, YAMLError):
            raise RuntimeError("failed to dump genome index to directory ~ {!r}".format(path))
    
    def load(self, path):
        '''Load genome index from genome directory.
        
        Args:
            path (str): Path of genome directory from which genome index 
                will be loaded.
        '''
        
        index_path = GenomeIndex._get_index_path(path)
        
        try:
            with open(index_path, 'r') as fh:
                self._data = safe_load(fh)
        except (IOError, YAMLError):
            raise RuntimeError("failed to load genome index from directory ~ {!r}".format(path))
        
        if ( any( k not in GenomeIndex.tags for k in self._data ) or 
            any( a not in GenomeIndex.tags[k] for k in self._data for a in self._data[k] ) ):
            raise RuntimeError("failed to load invalid genome index from directory ~ {!r}".format(path))

    def update(self, path):
        '''Update genome index for given genome directory.
        
        Args:
            path (str): Path of genome directory for which this genome index 
                will be updated.
        '''
        
        # Init index data.
        self._data = { 'info': dict(), 'files': dict() }
        
        # Get list of files in genome directory.
        files = [ x for x in os.listdir(path) 
            if os.path.isfile( os.path.join(path, x) ) ]
        
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
        genome_type_cues = [ k for k in gmatch.keys() if k != 'COMMON' ]
        
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
        if genome_type == 'S288C':
            
            genome_match = gmatch['S288C']['genome']
            self._data['info']['strain'] = genome_match.group(1)
            self._data['info']['institution'] = 'SGD'
            self._data['info']['ID'] = genome_match.group(3)
            self._data['info']['date'] = genome_match.group(4)
        
        elif genome_type == 'NON-S288C':
            
            genome_match = gmatch['NON-S288C']['genome']
            self._data['info']['strain'] = genome_match.group(1)
            self._data['info']['institution'] = genome_match.group(2)
            self._data['info']['ID'] = genome_match.group(4)
            
            if 'README' in gmatch['COMMON']:
                readme = _load_genome_readme( os.path.join(path, 
                    gmatch['COMMON']['README'].group(0)) )
                self._data['info']['date'] = readme['date'].strftime('%Y%m%d')
            else:
                self._data['info']['date'] = int(genome_match.group(3))
        
        # Set genome name from strain and ID.
        self._data['info']['name'] = 'SGD_{}_{}'.format(
            self._data['info']['strain'], self._data['info']['ID'])
        
        # Update file info.
        for k in gmatch.keys():
            for t in gmatch[k].keys():
                self._data['files'][t] = gmatch[k][t].group(0)

################################################################################

def _norm_chr(chromosome):
    '''Normalise chromosome name. (Returns None on failure.)'''
    
    # This pattern is modelled on the method used by SnpEff to simplify 
    # chromosome names, as in the file 'ChromosomeSimpleName.java'. Prefixes of
    # the form 'chr', 'chromo', and 'chromosome' are removed, as well as leading
    # zeroes in the chromosome number and known delimiters (i.e. ':', '_', '-').
    # (See https://github.com/pcingola/SnpEff [Accessed: 16 Feb 2016].)
    chromosome_pattern = re.compile('^(?:chr(?:omo(?:some)?)?)?0*(\S+)$')

    # Init normalised chromosome to None.
    result = None
    
    # If putative chromosome is defined, try to normalise.
    if chromosome is not None:
        
        # If putative chromosome is an integer, convert to string..
        if isinstance(chromosome, (int, long)):
        
            chromosome = str(chromosome)
            
        # ..otherwise check of string type.
        elif isinstance(chromosome, basestring):
            chromosome = chromosome.strip()
        else:
            raise TypeError("chromosome is not a string or integer")
        
        # If putative chromosome is in the set of 
        # normalised chromosomes, set as normalised..
        if chromosome in _info['norm-chr']:
            result = chromosome
        
        # ..otherwise try to map to a normalised chromosome.
        else:
            m = chromosome_pattern.match(chromosome)
            
            try:
                k = m.group(1).upper()
                result = _info['chr2norm'][k]
            except (AttributeError, KeyError):
                pass
                
    return result

def _load_genome_readme(filepath):
    '''Load SGD genome README file.'''
    
    with open(filepath, 'r') as fh:
        
        lines = [ line.rstrip() for line in fh.readlines() ]
        
        try:
            assert all( lines[i] == '' for i in (1, 3) )
            
            heading = lines[0]
            date = datetime.strptime(lines[2], '%m/%d/%Y').date()
            body = deque( lines[4:] )
            
            # Strip blank lines from beginning and end of README body.
            while len(body) > 0 and body[0].strip() == '':
                body.popleft()
            while len(body) > 0 and body[-1].strip() == '':
                body.pop()
            
            body = '\n'.join(body)
            
            result = OrderedDict([
                ('heading', heading), 
                ('date', date), 
                ('body', body)
            ])
            
        except (AssertionError, IndexError):
            raise RuntimeError("unexpected README format")
    
    return result

################################################################################

def index_genome(path):
    '''Index yeast genome data.
    
    This takes as input a directory containing genome assembly files downloaded 
    from the Saccharomyces Genome Database (SGD). It indexes the data files in 
    the input directory and saves these to a genome index file in YAML format.
    
    Args:
        path (str): Path to yeast genome data directory.
    '''
    gindex = GenomeIndex(path)
    gindex.dump(path)

def prep_genome(path, email=None):
    '''Prep yeast genome data.
    
    This takes as input a directory containing genome assembly files downloaded 
    from the Saccharomyces Genome Database (SGD). It processes the SGD data 
    files in the input directory and prepares a genome sequence file and 
    annotation file. These prepped files will have consistent reference sequence 
    names and are suitable as input for downstream bioinformatics analysis.

    Several changes are made to the genome sequence and annotation files:

     * The reference sequence FASTA file is given the extension '.fa', which is 
       more commonly used, and is required by some software (e.g. Picard).

     * Each reference sequence is assigned to a chromosome if possible, and
       renamed to improve readability. If a reference sequence is a chromosome, 
       its name is changed to a normalised representation (e.g. 'chrI'). 
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
        path (str): Path to yeast genome data directory.
        email (str): Contact email for NCBI E-utilities.
    '''
    
    # PROCESS ... ##############################################################
    
    gindex = GenomeIndex(path)
    
    # Check annotation file found.
    if 'annotation' not in gindex.files:
        raise RuntimeError("cannot prep genome - annotation file not found")
    
    # Set output genome sequence file path.
    sequence_file = os.path.join(path, '{}.fa'.format(gindex.info['name']))
    
    # Set output genome annotation file path.
    annotation_file = os.path.join(path, '{}.gff'.format(gindex.info['name']))
    
    # Set sequence definition pattern.
    seq_def_pattern = re.compile("^Saccharomyces cerevisiae(?: strain)? (\S+)"
        "(?: chromosome (\S+?),?)? (\S+)(?: genomic scaffold)?, whole genome "
        "(?:shotgun)? sequence$", re.IGNORECASE)
    
    # Set GFF pragma patterns.
    pragma_pattern = {
        'version': re.compile('^##gff-version (.+)$'),
        'forward-reference-resolution': re.compile('^###$'),
        'fasta': re.compile('^##FASTA$')
    }
    
    # Set GenBank query frequency: at least one second between queries.
    genbank_frequency = 1.0
    
    # Set GenBank query timer.
    genbank_timer = time.time()
    
    # READ GFF FILE ############################################################
    
    anno_path = os.path.join(path, gindex.files['annotation'])
    
    # Read GFF pragmas and comments.
    with prise(anno_path, 'r') as fh:
        
        pragmas, comments = [ list() for _ in range(2) ]
        
        # Read all GFF comment lines.
        comment_lines = [ line.rstrip() for line in fh.readlines() 
            if line.startswith('#') ]
            
        # Separate pragmas and comments.
        for line in comment_lines:
            if line.startswith('##'):
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
        header_comments = '\n'.join(comments)
    
    # Read and modify GFF records.
    with prise(anno_path, 'r') as fh:
        
        try:
            records = [ x for x in GFF.parse(fh) ]
        except AssertionError:
            raise RuntimeError("invalid annotation file ~ {!r}".format(anno_path))
        
        for i, record in enumerate(records):
            
            # Init new sequence name and corresponding chromosome.
            seq_id, seq_chr = [None] * 2
            
            # Check if sequence name matches expected pattern  
            # for a sequence header containing a GI number.
            gi_match = _info['pattern']['gi'].match(record.id)
                    
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
                result = check_efetch(db='nucleotide', id=gi, retmode='xml', 
                    email=email)
                
                # Reset GenBank timer.
                genbank_timer = time.time() + genbank_frequency
                
                # Get sequence definition.
                seq_def = result[0]['GBSeq_definition']
            
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
                seq_id = '{}_{}'.format(seq_chr, seq_id)
                
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
                if feature.type in ('chromosome', 'contig'):
                    feature.id = seq_id
                    record.features[j] = feature
            
            records[i] = record

    # WRITE OUTPUT FILES #######################################################
    
    # Get timepoint and timestamp.
    dt = datetime.now()
    timepoint = dt.strftime('%H:%M:%S on %A %d %B %Y')
    timestamp = dt.strftime('%a %b %d %H:%M:%S %Y')
    
    # Set general GFF header appendix.
    template = Template( _info['header-appendix'] )
    additional_comments = template.substitute({ 'TIMEPOINT': timepoint, 
        'SCRIPT': 'GACTutil', 'CONTACT': email })
    header_comments = '\n'.join([header_comments, additional_comments])
    
    # Get serialised annotation data.
    with TemporaryDirectory() as twd:
        
        annotation_temp = os.path.join(twd, 'annotation.tmp')
        
        with open(annotation_temp, 'w') as fh:
            GFF.write(records, fh)
            
        with open(annotation_temp, 'r') as fh:
            lines = fh.readlines()
            version_pragma = lines[0]
            annotation_data = ''.join(lines[1:])
    
    # Check that BCBio GFF output starts with a version pragma.
    if pragma_pattern['version'].match(version_pragma) is None:
        raise RuntimeError("version pragma not found in BCBio GFF output")
    
    # Write output annotation file.
    with open(annotation_file, 'w') as fh:
        fh.write(version_pragma)
        fh.write(header_comments)
        fh.write(annotation_data)
    
    # Write output sequence file.
    with open(sequence_file, 'w') as fh:
        for record in records:
            SeqIO.write(record, fh, 'fasta')
    
    gindex.update(path)
    gindex.dump(path)

################################################################################
