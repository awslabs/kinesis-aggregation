from setuptools import setup
import os.path

def read_file(filename):
    path = os.path.join(os.path.dirname(__file__),filename)
    with open(path,'r') as source_file:
        return source_file.read()

setup(
  name = 'aws_kpl_agg',
  packages = ['aws_kpl_agg'],
  version = '1.0.10',
  description = 'Python module to simplify processing of Amazon Kinesis Records which have been created with the Kinesis Producer Library.',
  long_description=read_file('README.rst'),
  author = 'Brent Nash',
  author_email = 'brenash@amazon.com',
  license = 'SEE LICENSE IN LICENSE.TXT',
  url = 'http://github.com/awslabs/kinesis-deaggregation',
  keywords = ['aws','kinesis','deaggregation'],
  classifiers = ['Development Status :: 5 - Production/Stable',
                'Intended Audience :: Developers',
                 'License :: Other/Proprietary License',
                 'Natural Language :: English',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2.7',
                 'Topic :: Software Development :: Libraries :: Python Modules',
                 'Topic :: Utilities'],
  install_requires = ['protobuf']
)
