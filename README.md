# GACTutil: a Python package of yeast genomics utilities.

This package contains utilities for day-to-day tasks in yeast genomics.

## Dependencies 

In addition to the Python standard library, this package depends on the following:

- [BCBio](https://github.com/chapmanb/bcbb)
- [Biopython](http://biopython.org/)
- [PySAM](https://github.com/pysam-developers/pysam)
- [PyVCF](https://github.com/jamescasbon/PyVCF)
- [PyYAML](http://pyyaml.org/)

## Installation 

To install this package, navigate to the package root and input the command:

```
python setup.py install
```

This will install GACTutil and any dependencies.

## Usage 

Selected functions can be run from the command line using the package entry 
point 'gaction' as follows:

```
gaction <command> <qualifier> [-h] ...
```

...where `<command>` is the main command, and `<qualifier>` modifies the command. 

To see options for a command, use the help flag (`-h`).

## Contact

For information or issues email tw164 (-a-) le.ac.uk
