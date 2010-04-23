#!/usr/bin/python
from setuptools import setup, find_packages

setup(
    name = "KISSmetrics",
    version = "1.0.1",
    packages = find_packages(),

    # metadata for upload to PyPI
    author = 'KISSmetrics',
    author_email = 'info@kissmetrics.com',
    description = "Python API for KISSmetrics",
    long_description = (
"""
KISSmetrics is different from other analytics platforms due to a combination of
three key features. We focus on funnels and conversions and make them easy to
track and interpret, use people as the basic unit of measure, and support
tracking of highly-flexible custom data through our simple API.

Find out more at http://www.kissmetrics.com
"""
    ),
    license = 'Other/Proprietary License',
    keywords = 'kissmetrics',
    url = 'http://github.com/kissmetrics/KISSmetrics',

    # Script for running cronjob
    entry_points = {
        'console_scripts': [
            'kissmetrics = km:main',
        ],
    },

    # Tests
    test_suite = 'tests',
    tests_require = ['mocker'],
)
