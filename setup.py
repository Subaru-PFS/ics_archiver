#!/usr/bin/env python
"""Install this package. Requires setuptools.

To use:
python setup.py install
"""
import sdss3tools

sdss3tools.setup(
    name = 'ics_archiver',
    description = "Data archiver for SDSS-III",
    data_dirs = ("web",),
)
