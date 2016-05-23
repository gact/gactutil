#!/usr/bin/env python -tt
# -*- coding: utf-8 -*-
u"""GACTutil CSV utilities.

Unicode CSV classes are taken from the recipe in the Python 2 CSV module docs.
Available: https://docs.python.org/2/library/csv.html [Accessed: May 2016]
"""

from __future__ import absolute_import
import csv
import os

################################################################################

def utf8_encoder(csv_data):
    """Generator function to encode bytestring as UTF-8.
    
    Based on the utf_8_encoder recipe in the Python 2 CSV module docs.
    Available: https://docs.python.org/2/library/csv.html [Accessed: May 2016]
    """
    for line in csv_data:
        yield line.encode('utf_8')

class UTF8Reader(object):
    """A CSV writer that will read rows from a UTF-8 encoded CSV file.
    
    Based on the UnicodeReader recipe in the Python 2 CSV module docs.
    Available: https://docs.python.org/2/library/csv.html [Accessed: May 2016]
    """
    
    def __init__(self, csvfile, dialect=csv.excel, **kwds):
        self._reader = csv.reader(utf8_encoder(csvfile),
            dialect=dialect, **kwds)
    
    def __iter__(self):
        return self
    
    def __next__(self):
        return [ unicode(x, 'utf_8') for x in self._reader.next() ]
    
    def next(self):
        return self.__next__()

class UTF8Writer(object):
    """A CSV writer that will write rows to a UTF-8 encoded CSV file.
    
    Based on the UnicodeWriter recipe in the Python 2 CSV module docs.
    Available: https://docs.python.org/2/library/csv.html [Accessed: May 2016]
    """

    def __init__(self, csvfile, dialect=csv.excel, **kwds):
        self._writer = csv.writer(csvfile, dialect=dialect, **kwds)

    def writerow(self, row):
        self._writer.writerow( [ x.encode('utf_8')
            if isinstance(x, unicode) else str(x) for x in row ] )

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

################################################################################
