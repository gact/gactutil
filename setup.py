#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-

from setuptools import find_packages
from setuptools import setup

from gactutil import _setup_about
from gactutil.gaction import _setup_commands

setup_info = {
    'name': 'gactutil',
    'version': '0.1.0',
    'description': 'Utilities for everyday yeast genomics',
    'author': 'Thomas Walsh',
    'author_email': 'tw164@le.ac.uk',
    'url': 'https://github.com/gact/gactutil',
    'packages': find_packages(),
    'include_package_data': True,

    'classifiers': [
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',   
        'Intended Audience :: Science/Research',    
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',   
        'Operating System :: MacOS :: MacOS X',
        "Operating System :: Microsoft :: Windows",
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ],

    'license': 'GNU GPL 3.0',
    'keywords': 'bioinformatics',
    'install_requires': [ 
        'biopython>=1.66', 
        'bcbio-gff>=0.6.2', 
        'pysam>=0.9.0', 
        'pyvcf>=0.6.7',
        'PyYAML>=3.11'
     ],
    'entry_points': { 
        'console_scripts': [
            'gaction = gactutil.gaction:main',
        ],
        'vcf.filters': [
            'mnp-only = gactutil.vcf:MnpOnly',
            'sv-only = gactutil.vcf:SvOnly'
        ]
    }
}

with open('README.md', 'r') as fh:
    setup_info['long_description'] = fh.read()

_setup_about(setup_info)

_setup_commands()

setup( **setup_info )

################################################################################
