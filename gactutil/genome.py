#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil genome utilities."""

from collections import deque
from collections import Iterable
from collections import OrderedDict
from datetime import datetime
import os
from os.path import getmtime
import re
from string import Template
import sys
from textwrap import dedent
import time

from BCBio import GFF
from Bio import SeqIO
import pysam

from gactutil import about
from gactutil import const
from gactutil import FrozenDict
from gactutil import gactfunc
from gactutil import TextReader
from gactutil import TextWriter
from gactutil.chrom import norm_chrom_id
from gactutil.core import fsdecode
from gactutil.core import fsencode
from gactutil.core import remove_existing
from gactutil.core import temporary_directory
from gactutil.core.ncbi import check_efetch
from gactutil.core import tools
from gactutil.core.uniyaml import unidump
from gactutil.core.uniyaml import uniload
from gactutil.core.uniyaml import YAMLError

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
            u'prepped_reference': re.compile(u'^SGD_([^_]+)_([^_]+)[.]fa$'),
            
            # Captures: <strain>_<ID>
            u'prepped_annotation': re.compile(u'^SGD_([^_]+)_([^_]+)[.]gff$'),
        },

        u'S288C': {
            # Captures: <version>_<date>
            u'coding_sequence': re.compile(u'^orf_coding_all_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'gene_association': re.compile(u'^gene_association_((R\d+-\d+-\d+)_(\d{8}))[.]sgd$'),
            
            # Captures: <version>_<date>
            u'reference': re.compile(u'^(S288C)_reference_sequence_((R\d+-\d+-\d+)_(\d{8}))[.]fsa$'),
            
            # Captures: <version>_<date>
            u'annotation': re.compile(u'^saccharomyces_cerevisiae_((R\d+-\d+-\d+)_(\d{8}))[.]gff$'),
            
            # Captures: <version>_<date>
            u'non_feature': re.compile(u'^NotFeature_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'other_feature': re.compile(u'^other_features_genomic_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'peptide_sequence': re.compile(u'^orf_trans_all_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$'),
            
            # Captures: <version>_<date>
            u'rna_sequence': re.compile(u'^rna_coding_((R\d+-\d+-\d+)_(\d{8}))[.]fasta$')
        }, 
        
        u'NON-S288C': {
            
            # Captures: <strain>_<ID>
            u'coding_sequence': re.compile(u'^([^_]+)_([^_]+)_cds[.]fsa(?:[.]gz)?$'),
            
             # Captures: <strain>_<institution>_<year>_<ID>
            u'reference': re.compile(u'^([^_]+)_([^_]+)_([^_]+)_([^_]+)[.]fsa(?:[.]gz)?$'),
            
            # Captures: <strain>_<ID>
            u'annotation': re.compile(u'^([^_]+)_([^_]+)[.]gff(?:[.]gz)?$'),
            
            # Captures: <strain>
            u'indel': re.compile(u'^(.+)[.]indel[.]gatk[.]vcf(?:[.]gz)?$'),
            
            # Captures: <strain>_<ID>
            u'peptide_sequence': re.compile(u'^([^_]+)_([^_]+)_pep[.]fsa(?:[.]gz)?$'),
            
            # Captures: <strain>
            u'snp': re.compile(u'^(?!.+(?:[.]indel))(.+)[.]gatk[.]vcf(?:[.]gz)?$')
        },
        
        u'DERIVED': {
            
            u'bwa_index': re.compile(u'^SGD_([^_]+)_([^_]+)[.]fa[.](?:amb|ann|bwt|pac|sa)$'),
            
            u'fasta_index': re.compile(u'^SGD_([^_]+)_([^_]+)[.]fa[.]fai$'),
            
            u'seq_dict': re.compile(u'^SGD_([^_]+)_([^_]+)[.]dict$')
        }
    } 
    
    # Genome index tags.
    tags = {

        # Info about genome.
        u'genome_info': (u'date', u'name', u'ID', u'institution', u'strain'),
        
        # Files in genome.
        u'file_info': (u'annotation',  u'bwa_index', u'coding_sequence',
            u'fasta_index', u'gene_association', u'indel', u'non_feature',
            u'other_feature', u'peptide_sequence', u'snp', u'prepped_annotation',
            u'prepped_reference', u'README', u'reference', u'rna_sequence',
            u'seq_dict')
    }
    
    index_filename = u'index.yaml'
    
    required = (u'reference', u'annotation')
    
    @classmethod
    def _get_index_path(cls, directory):
        u"""Get path of genome index from genome directory path."""
        directory = fsdecode(directory)
        return os.path.join( os.path.normpath(directory), GenomeIndex.index_filename )
    
    @property
    def filenames(self):
        u"""tuple: Genome data filenames."""
        filenames = list()
        for x in sorted( self._data[u'file_info'].values() ):
            if isinstance(x, Iterable) and not isinstance(x, basestring):
                for filename in x:
                    filenames.append(filename)
            else:
                filenames.append(x)
        return tuple(filenames)
    
    @property
    def file_info(self):
        u"""FrozenDict: Dictionary of genome file info."""
        return self._data[u'file_info']
    
    @property
    def genome_info(self):
        u"""FrozenDict: Dictionary of genome info."""
        return self._data[u'genome_info']
        
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
            if x != GenomeIndex.index_filename ]
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
                    for x in self.filenames ]
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
            with TextWriter(index_path) as fh:
                unidump(self._data.thaw(), fh, default_flow_style=False,
                    width=sys.maxint)
        except (IOError, YAMLError):
            raise RuntimeError("failed to dump genome index to directory: {!r}".format(directory))
    
    def load(self, directory):
        u"""Load genome index from genome directory.
        
        Args:
            directory (unicode): Path of genome directory from which genome index
                will be loaded.
        """
        
        index_path = GenomeIndex._get_index_path(directory)
        
        try:
            with TextReader(index_path) as fh:
                self._data = FrozenDict( uniload(fh) )
        except (IOError, TypeError, YAMLError):
            raise RuntimeError("failed to load genome index from directory: {!r}".format(directory))
        
        if ( any( k not in GenomeIndex.tags for k in self._data ) or
            any( a not in GenomeIndex.tags[k] for k in self._data for a in self._data[k] ) ):
            raise RuntimeError("failed to load invalid genome index from directory: {!r}".format(directory))

    def update(self, directory):
        u"""Update genome index for given genome directory.
        
        Args:
            directory (unicode): Path of genome directory for which this genome index
                will be updated.
        """
        
        directory = fsdecode(directory)
        
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
                elif k == u'DERIVED': # allow multiple files in derived data unit
                    gmatch[k][t] = [ x for x in gmatch[k][t] ]
                else:
                    raise RuntimeError("multiple {!r} files found".format(t))
            if len(gmatch[k]) == 0:
                del gmatch[k]
        
        # Get genome type cues ('S288C' or 'NON-S288C') from matching files.
        genome_type_cues = [ k for k in (u'S288C', u'NON-S288C') if k in gmatch ]
        
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
        
        # Init index info.
        genome_info = dict()
        file_info = dict()
        
        # Update genome info.
        if genome_type == u'S288C':
            
            m = gmatch[u'S288C'][u'reference']
            genome_info[u'strain'] = m.group(1)
            genome_info[u'institution'] = u'SGD'
            genome_info[u'ID'] = m.group(3)
            genome_info[u'date'] = datetime.strptime(m.group(4), '%Y%m%d').date()
            
        elif genome_type == u'NON-S288C':
            
            m = gmatch[u'NON-S288C'][u'reference']
            genome_info[u'strain'] = m.group(1)
            genome_info[u'institution'] = m.group(2)
            genome_info[u'ID'] = m.group(4)
            
            if u'README' in gmatch[u'COMMON']:
                readme = _load_genome_readme( os.path.join(directory,
                    gmatch[u'COMMON'][u'README'].group(0)) )
                genome_info[u'date'] = readme[u'date']
            else:
                genome_info[u'date'] = int(m.group(3))
        
        # Set genome name from strain and ID.
        genome_info[u'name'] = u'SGD_{}_{}'.format(
            genome_info[u'strain'], genome_info[u'ID'])
        
        # Update file info.
        for k in gmatch.keys():
            
            for t in gmatch[k].keys():
                
                if isinstance(gmatch[k][t], Iterable):
                    file_info[t] = [ m.group(0) for m in gmatch[k][t] ]
                else:
                    file_info[t] = gmatch[k][t].group(0)
        
        self._data = FrozenDict({
            u'genome_info': genome_info,
            u'file_info': file_info
        })

################################################################################

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
            raise RuntimeError("unexpected genome README format")
    
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
def prep_genome(directory, bwa_index=False, fasta_index=False, seq_dict=False,
    complete=False):
    u"""Prep yeast genome data.
    
    This takes as input a directory containing genome assembly files downloaded
    from the Saccharomyces Genome Database (SGD). It processes the SGD data
    files in the input directory and prepares a genome sequence file and
    annotation file. These prepped files will have consistent reference sequence
    names and are suitable as input for downstream bioinformatics analysis.
    
    Derived data files can optionally be created, including a BWA index, a
    SAMtools FASTA index, and a Picard sequence dictionary. These options
    each require that the respective tool be installed and available.

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
        bwa_index (bool): Prepare BWA index of reference sequence.
        fasta_index (bool): Prepare SAMtools FASTA index of reference sequence.
        seq_dict (bool): Prepare Picard sequence dictionary of reference sequence.
        complete (bool): Prepare all derived data files.
    """
    
    # TODO: newline option
    
    # PROCESS ... ##############################################################
    
    gindex = GenomeIndex(directory)
    
    # Check annotation file found.
    if u'annotation' not in gindex.file_info:
        raise RuntimeError("cannot prep genome - annotation file not found")
    
    # Set prepped genome sequence file info.
    prepped_ref_file = u'{}.fa'.format(gindex.genome_info['name'])
    prepped_ref_path = os.path.join(directory, prepped_ref_file)
    prepped_ref_root, _ = os.path.splitext(prepped_ref_path)
    
    # Set prepped genome annotation file info.
    prepped_anno_file = u'{}.gff'.format(gindex.genome_info[u'name'])
    prepped_anno_path = os.path.join(directory, prepped_anno_file)
    
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
    
    # Set query timer.
    time_of_next_query = time.time()
    
    # READ GFF FILE ############################################################
    
    anno_path = os.path.join(directory, gindex.file_info[u'annotation'])
    
    # Read GFF pragmas and comments.
    with TextReader(anno_path) as fh:
        
        pragmas, comments = [ list() for _ in range(2) ]
        
        # Read all GFF comment lines.
        comment_lines = [ line.rstrip() for line in fh.readlines()
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
                raise ValueError("unknown pragma: {!r}".format(pragma))
        
        # Set GFF header comments.
        header_comments = u'\n'.join(comments)
    
    # Read and modify GFF records.
    with TextReader(anno_path) as fh:
        
        try:
            records = [ x for x in GFF.parse(fh) ]
        except AssertionError:
            raise RuntimeError("invalid annotation file: {!r}".format(anno_path))
        
        # Regex of sequence identifiers beginning with a GI number.
        gi_regex = re.compile(u'^gi[|](\d+)[|].+')
        
        for i, record in enumerate(records):
            
            # Init new sequence name and corresponding chromosome.
            seq_id, seq_chr = [None] * 2
            
            # Check if sequence name matches expected pattern  
            # for a sequence header containing a GI number.
            gi_match = gi_regex.match(record.id)
                    
            # If sequence header appears to contain GI number, 
            # query GenBank for relevant sequence information..
            if gi_match is not None:
                
                # Get GI number from sequence header.
                gi = gi_match.group(1)
                
                # Wait until query timer expires.
                if time.time() < time_of_next_query:
                    time.sleep( time_of_next_query - time.time() )
                
                # Fetch GenBank entry for GI number.
                result = check_efetch(db=u'nucleotide', id=gi, retmode=u'xml')
                
                # Reset query timer.
                time_of_next_query = time.time() + const.ncbi_query_beat
                
                # Get sequence definition.
                seq_def = result[0][u'GBSeq_definition'].decode('utf_8')
            
                # Check if sequence definition matches expected pattern.
                seq_def_match = seq_def_pattern.match(seq_def)
            
                # Check sequence definition matches expected pattern.
                if seq_def_match is None:
                    raise RuntimeError("failed to identify chromosome of GenBank entry 'gi:{}'".format(gi))
                
                # Get new sequence name and corresponding chromosome.
                seq_id, seq_chr = seq_def_match.group(3, 2) 
                
                # Normalise chromosome name.
                seq_chr = norm_chrom_id(seq_chr)
                
                # Incorporate chromosome in sequence name.
                seq_id = u'{}_{}'.format(seq_chr, seq_id)
                
            # ..otherwise check if sequence is itself a chromosome.
            else:
                
                # Assuming sequence represents a chromosome, 
                # normalise chromosome name.
                seq_chr = norm_chrom_id(record.id)
                
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
            record.description = record.name = u''
            record.id = seq_id
            
            # If sequence is described in a feature record, 
            # update ID attribute to reflect new sequence ID.
            for j, feature in enumerate(record.features):
                if feature.type in (u'chromosome', u'contig'):
                    feature.id = seq_id
                    for k in (u'ID', u'Name'):
                        if k in feature.qualifiers:
                            feature.qualifiers[k] = seq_id
                    record.features[j] = feature
            
            records[i] = record

    # WRITE PREPPED FILES ######################################################
    
    # Get timepoint and timestamp.
    timepoint = unicode( datetime.now().strftime('%H:%M:%S on %A %d %B %Y') )
    # timestamp = dt.strftime('%a %b %d %H:%M:%S %Y')
    
    # Set general GFF header appendix.
    template = Template( dedent(u'''\
    # Original file created by Saccharomyces Genome Database <www.yeastgenome.org>.
    # This version created at ${TIMEPOINT}.
    # Modified from the original at GACT <www2.le.ac.uk/colleges/medbiopsych/research/gact>.
    # Reference sequence names may have been changed.
    #
    # For enquiries on the original data, contact: sgd-helpdesk@lists.stanford.edu
    # For more information on changes made, contact: ${CONTACT}
    #
    ''') )
    
    additional_comments = template.substitute({ u'TIMEPOINT': timepoint,
        u'CONTACT': about[u'author_email'] })
    
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
    with TextWriter(prepped_anno_path) as fh:
        fh.write(version_pragma)
        fh.write(header_comments)
        fh.write(annotation_data)
    
    # Write output sequence file.
    with TextWriter(prepped_ref_path) as fh:
        for record in records:
            SeqIO.write(record, fh, 'fasta')
    
    # WRITE DERIVED DATA FILES #################################################
    
    if bwa_index or complete:
        tools.run(u'bwa', args=(u'index', u'-a', u'is', prepped_ref_path))
    
    if fasta_index or complete:
        pysam.faidx( fsencode(prepped_ref_path) )
    
    if seq_dict or complete:
        
        dict_path = u'{}.dict'.format(prepped_ref_root)
        remove_existing(dict_path)
        dict_arg = u'OUTPUT={}'.format(dict_path)
        ref_arg = u'REFERENCE={}'.format(prepped_ref_path)
        tools.run(u'picard', args=(u'CreateSequenceDictionary', ref_arg, dict_arg))
    
    # Update genome index file.
    gindex.update(directory)
    gindex.dump(directory)

################################################################################
