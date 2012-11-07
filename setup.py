# Copyright 2012 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import sys
import setuptools
from setuptools.command.test import test as TestCommand

ci_cmdclass = {}

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        pytest.main(self.test_args)

ci_cmdclass['test'] = PyTest

setuptools.setup(
        name="MyDrizzle",
        description="MySQL compatible libdrizzle based connector",
        version="0.1",
        author="Andrew Hutchings <andrew@linuxjedi.co.uk>",
        ext_modules = [
            setuptools.Extension('_drizzle', ['_drizzle.c'],
                libraries=['drizzle'], include_dirs=['/usr/local/include'],
                library_dirs=['/usr/local/lib'])
        ],
        py_modules = ['_drizzle_exceptions'],
        cmdclass=ci_cmdclass
    )
