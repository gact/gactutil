#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil SAM utilities."""

from collections import OrderedDict
import pysam
from pysam import AlignmentFile
import re

from gactutil import about
from gactutil import const
from gactutil import FrozenTable
from gactutil import gactfunc
from gactutil.core import dropped_tempfile


################################################################################

try: # Get SAM header info from PySAM.
    const.sam_header_record_tags = pysam.calignmentfile.VALID_HEADERS
    const.sam_header_record_types = pysam.calignmentfile.VALID_HEADER_TYPES
    const.sam_header_field_tags = pysam.calignmentfile.VALID_HEADER_ORDER
    const.sam_header_field_types = pysam.calignmentfile.KNOWN_HEADER_FIELDS
except AttributeError:
    raise ImportError("cannot import SAM header information")

# From the SAM format spec.
const.sequencing_platforms = ('CAPILLARY', 'LS454', 'ILLUMINA', 'SOLID',
    'HELICOS', 'IONTORRENT', 'ONT', 'PACBIO')

################################################################################

@gactfunc
def set_bam_read_groups(infile, rginfo, outfile):
    u"""Set read group info of BAM file.
    
    Args:
        infile (unicode): Input BAM file.
        rginfo (FrozenTable): Table of read group info.
        outfile (unicode): Output BAM file with the specified read group
            information.
    """
    
    known_filter_types = (u'filename', u'qname')
    
    if u'ID' not in rginfo.headings:
        raise ValueError("read group 'ID' column not found in read group table")
    
    filter_types = list()
    rginfo_tags = list()
    
    for h in rginfo.headings:
        
        if h in const.sam_header_field_tags['RG']:
            rginfo_tags.append(h)
        elif h in known_filter_types:
            filter_types.append(h)
        else:
            raise ValueError("unknown read group table heading: {!r}".format(h))
    
    if len(filter_types) == 0 and len(rginfo) != 1:
        raise ValueError("ambiguous read group info")
    
    regexes = dict()
    for t in filter_types:
        regexes[t] = [ re.compile(x) for x in rginfo[..., t] ]
    
    matching_indices = dict()
    
    if u'filename' in regexes and infile != u'-':
        
        matches = [ regex.search(infile) for regex in regexes[u'filename'] ]
        matching_indices[u'filename'] = [ i for i, m in enumerate(matches) 
            if m is not None ]
    
    read_group_info = OrderedDict()
    
    record_read_groups = list()
    
    with dropped_tempfile() as tempfile:
        
        # First pass: copy to temp file unmodified, 
        # while identifying relevant read groups.
        with AlignmentFile(infile, 'rb') as reader:
            
            header = reader.header
            
            with AlignmentFile(tempfile, 'wb', template=reader) as writer:
                
                for record in reader:
                    
                    qname = record.query_name
                    
                    if u'qname' in regexes:
                        matches = [ regex.search(qname) 
                            for regex in regexes[u'qname'] ]
                        matching_indices[u'qname'] = [ i
                            for i, m in enumerate(matches) 
                            if m is not None ]
                    
                    if len(filter_types) > 0:
                        
                        index_lists = matching_indices.values()
                        
                        common_indices = set(index_lists[0]).intersection(*index_lists)
                        
                        if len(common_indices) == 1:
                            i = common_indices.pop()
                        elif len(common_indices) > 1:
                            raise RuntimeError("read does not match a unique read group: {!r}".format(qname))
                        elif len(common_indices) == 0:
                            raise RuntimeError("read does not match any read group: {!r}".format(qname))
                    
                    else:
                        
                        i = 0
                     
                    rgid = rginfo[i, u'ID']
                    
                    read_group_info.setdefault( rgid, { tag: rginfo[i, tag]
                        for tag in rginfo_tags } )
                        
                    record_read_groups.append(rgid)
                    
                    writer.write(record)
        
        # Update header program info.
        # TODO: generate ID automatically
        header[u'PG'].append({u'ID': u'gaction_set_bam_read_groups',
            u'PN': about[u'name'], u'VN': about[u'version'] })
        
        # Update header read group info.
        header[u'RG'] = read_group_info.values()
        
        # Second pass: copy temp file to output, 
        # while adding relevant read group info.
        with AlignmentFile(tempfile, 'rb') as reader:
            
            with AlignmentFile(outfile, 'wb', header=header) as writer:
                
                for i, record in enumerate(reader):
                    
                    # NB: PySAM expects bytestring
                    record.set_tag('RG', str(record_read_groups[i]))
                    
                    writer.write(record)

################################################################################
