# Copyright 2016 Quora, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup
from setuptools.extension import Extension

import codecs
import sys


CYTHON_MODULES = [
    'async_task',
    'batching',
    'contexts',
    '_debug',
    'decorators',
    'futures',
    'profiler',
    'scheduler',
    'scoped_value',
    'utils',
]


DATA_FILES = ['%s.pxd' % module for module in CYTHON_MODULES]


VERSION = '1.0.2'


EXTENSIONS = [
    Extension(
        'asynq.%s' % module,
        ['asynq/%s.py' % module]
    ) for module in CYTHON_MODULES
]


if __name__ == '__main__':
    with codecs.open('./README.rst', encoding='utf-8') as f:
        long_description = f.read()

    setup(
        name='asynq',
        version=VERSION,
        author='Quora, Inc.',
        author_email='asynq@quora.com',
        description='Quora\'s asynq library',
        long_description=long_description,
        url='https://github.com/quora/asynq',
        license='Apache Software License',
        classifiers=[
            'License :: OSI Approved :: Apache Software License',

            'Programming Language :: Python',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
        ],
        keywords='quora asynq common utility',
        packages=['asynq', 'asynq.tests'],
        package_data={'asynq': DATA_FILES},
        ext_modules=EXTENSIONS,
        setup_requires=['Cython>=0.27.1', 'qcore', 'setuptools'],
        install_requires=[
            'Cython>=0.27.1',
            'qcore',
            'inspect2',
            'mock; python_version < "3.3"',
        ],
    )
