from __future__ import print_function
import sys

import pytest
from setuptools.command.test import test as TestCommand
from setuptools import setup


class PyTest(TestCommand):

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['--strict', '--verbose', '--tb=long', 'tests']
        self.test_suite = True

    def run_tests(self):
        errno = pytest.main(self.test_args)
        sys.exit(errno)

setup(
    name='test-boto',
    url='http://www.scality.com',
    tests_require=['pytest'],
    install_requires=[
        'boto',
    ],
    cmdclass={'test': PyTest},
    description='Testing Ironmna-S3 with boto',
    platforms='any',
    test_suite='boto.test.test_app',
    extras_require={
        'testing': ['pytest'],
    }
)
