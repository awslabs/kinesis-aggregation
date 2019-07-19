# Kinesis Aggregation/Deaggregation Libraries for Python
# 
# Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved. 
# 
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
# 
#  http://aws.amazon.com/asl/
# 
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from setuptools import setup

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
  name='aws_kinesis_agg',
  packages=['aws_kinesis_agg'],
  version='1.1.1',
  description='Python module to assist in taking advantage of the Kinesis message aggregation '
              'format for both aggregation and deaggregation.',
  long_description=long_description,
  long_description_content_type='text/markdown',
  author='Brent Nash',
  author_email='brenash@amazon.com',
  license="Amazon Software License",
  url='http://github.com/awslabs/kinesis-aggregation',
  keywords=['aws', 'kinesis', 'aggregation', 'deaggregation', 'kpl'],
  classifiers=['Development Status :: 5 - Production/Stable',
               'Intended Audience :: Developers',
               'License :: Other/Proprietary License',
               'Natural Language :: English',
               'Operating System :: OS Independent',
               'Programming Language :: Python',
               'Programming Language :: Python :: 2.7',
               'Programming Language :: Python :: 3.6',
               'Topic :: Software Development :: Libraries :: Python Modules',
               'Topic :: Utilities'],
  install_requires=['protobuf']
)
