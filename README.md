# GACTutil: a Python package of yeast genomics utilities.

This package contains utilities for day-to-day tasks in yeast genomics.

## Dependencies 

In addition to the Python standard library, this package depends on the following:

- [BCBio](https://github.com/chapmanb/bcbb)
- [Biopython](http://biopython.org/)
- [NumPy](http://www.numpy.org/)
- [PySAM](https://github.com/pysam-developers/pysam)
- [PyVCF](https://github.com/jamescasbon/PyVCF)
- [PyYAML](http://pyyaml.org/)

In particular, the YAML submodule of this package contains slightly modified versions of several classes from Kirill Simonov's [PyYAML package](http://pyyaml.org), and the PyYAML software license is included in the YAML submodule file.

Also, VCF filters use the [PyVCF filter framework](https://pyvcf.readthedocs.io/en/latest/FILTERS.html), and Unicode CSV data are processed using classes based on the recipe in the [CSV module docs](https://docs.python.org/2/library/csv.html).

## Installation 

To install this package, navigate to the package root directory and input the command:

```
python setup.py install
```

This will install GACTutil and any dependencies.

## Usage 

Selected functions can be run from the command line using the package entry 
point 'gaction' as follows:

```
gaction <command> <modifier> [<modifier> ...] [-h] ...
```

...where `<command>` is the main command, which is modified by one or more `<modifier>` subcommands.

To see options for a command, use the help flag (`-h`).

## Contact

For information or issues email tw164 (-a-) le.ac.uk
