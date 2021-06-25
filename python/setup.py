# Kinesis Aggregation/Deaggregation Libraries for Python
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from io import open
from setuptools import setup

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
  name='aws_kinesis_agg',
  packages=['aws_kinesis_agg'],
  version='1.2.0',
  description='Python module to assist in taking advantage of the Kinesis message aggregation '
              'format for both aggregation and deaggregation.',
  long_description=long_description,
  long_description_content_type='text/markdown',
  author='Ian Meyers',
  author_email='meyersi@amazon.com',
  license="Apache-2.0",
  url='http://github.com/awslabs/kinesis-aggregation',
  keywords=['aws', 'kinesis', 'aggregation', 'deaggregation', 'kpl'],
  classifiers=['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'License :: OSI Approved :: Apache Software License',
               'Natural Language :: English',
               'Operating System :: OS Independent',
               'Programming Language :: Python',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3.6',
               'Topic :: Software Development :: Libraries :: Python Modules',
               'Topic :: Utilities'],
  install_requires=['protobuf']
)
