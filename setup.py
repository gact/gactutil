#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-

import io
import setuptools

from gactutil.core.about import about
from gactutil.core.config import config
from gactutil.core.gaction import _GactfuncInterface

setup_info = {
    'name': 'gactutil',
    'version': '0.2.0',
    'description': 'Utilities for everyday yeast genomics',
    'author': 'Thomas Walsh',
    'author_email': 'tw164@le.ac.uk',
    'url': 'https://github.com/gact/gactutil',
    'packages': setuptools.find_packages(),
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
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'Topic :: Utilities'
    ],

    'license': 'GNU GPL 3.0',
    'keywords': 'bioinformatics',
    'install_requires': [
        'biopython>=1.66',
        'bcbio-gff>=0.6.2',
        'numpy>=1.8.0',
        'pysam>=0.9.0',
        'pyvcf>=0.6.7',
        'PyYAML>=3.11'
     ],
    'entry_points': { 
        'console_scripts': [
            'gaction = gactutil.core.gaction:main',
        ],
        'vcf.filters': [
            'callrate = gactutil.vcf:CallRate',
            'dao = gactutil.vcf:DiallelicObservations',
            'dav = gactutil.vcf:DiallelicVariant',
            'mnp-only = gactutil.vcf:MnpOnly',
            'pao = gactutil.vcf:PolyallelicObservations',
            'pav = gactutil.vcf:PolyallelicVariant',
            'pmo = gactutil.vcf:PolymorphicObservations',
            'pmv = gactutil.vcf:PolymorphicVariant',
            'sv-only = gactutil.vcf:SvOnly'
        ]
    }
}

with io.open('README.md', encoding='utf_8') as fh:
    setup_info['long_description'] = fh.read()

# Setup package about info.
about.setup(setup_info)

# Setup package config info.
config.setup()

# Setup gactfunc collection.
gfi = _GactfuncInterface()
gfi.populate()
gfi.dump()

setuptools.setup( **setup_info )

################################################################################
