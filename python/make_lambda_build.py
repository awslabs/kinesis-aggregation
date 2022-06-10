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

from __future__ import print_function
import os.path
import shutil
import subprocess
import sys
import zipfile
import argparse
from pathlib import Path
import glob

BUILD_DIR_NAME = 'build'
TEST_DIR_NAME = 'test'
REQUIREMENTS_FILE_NAME = 'requirements.txt'

IGNORE_ITEMS = [
    BUILD_DIR_NAME,
    TEST_DIR_NAME,
    "dist",
    ".idea",
    "__pycache__",
    __file__,
    "aws_kinesis_agg.egg-info"
]

cur_dir = None
proj_dir = None
build_dir = None
version = "1.2.3"


def is_python_file(filename):
    """Returns True if the input file path has a .py extension. False otherwise."""
    path = Path(filename)
    return path.is_file() and os.path.splitext(filename)[-1] == '.py'


def initialize_current_working_dir():
    """Forces the current working directory to be set to the directory where
    this build script lives (which should be the python project root)."""

    global cur_dir, proj_dir

    cur_dir = os.path.normpath(os.getcwd())
    print(f'Current Working Directory = {cur_dir}')

    proj_dir = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
    print(f'Project Directory = {proj_dir}')

    if cur_dir != proj_dir:
        print('Changing CWD to script directory...')
        os.chdir(proj_dir)
    print(f'Current Working Directory = {proj_dir}')


def setup_build_dir():
    """Removes any existing build directories and creates a new one.  Due to issues with
    running "pip3 install <foo> -t <build_dir>" multiple times to the same directory, it's
    easier to just create a new build dir every time."""

    global build_dir

    print('')
    build_dir = os.path.join(os.getcwd(), BUILD_DIR_NAME)
    print(f'Setting up build directory: {build_dir}')
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir, True)
    os.mkdir(build_dir)


def copy_source_to_build_dir():
    """Copy all Python source files to the build directory."""

    global build_dir

    print('')
    print('Looking for Python source files...')
    for source in os.listdir(os.getcwd()):
        if is_python_file(source) and source != __file__:
            print(f'Copy file {source} to build directory...')
            shutil.copy(source, build_dir)
        elif os.path.isdir(source) and source not in IGNORE_ITEMS:
            print(f'Copy folder {source} to build directory...')
            shutil.copytree(source, os.path.join(build_dir, source))


def install_dependencies():
    """Using PIP, install all dependencies to the build directory."""

    global proj_dir, build_dir

    print('')

    #  Install PIP dependencies to the build directory
    print('')
    print('Installing necessary modules from pip...')
    requirements_file = os.path.join(proj_dir, REQUIREMENTS_FILE_NAME)
    pip_install_cmd = f'pip3 install -r {requirements_file} -t "{build_dir}"'
    print(pip_install_cmd)
    pip_install_cmd_line_result = subprocess.call(pip_install_cmd, shell=True)
    if pip_install_cmd_line_result != 0:
        print('Failed to install modules via pip. Try running \'{}\' to debug the issue.'.format(pip_install_cmd),
              file=sys.stderr)
        sys.exit(1)
    print('Successfully installed dependencies from pip.')

    #  AWS Lambda has issues with the normal protobuf install lacking a root level __init__.py
    protobuf_install_dir = os.path.join(build_dir, 'google')
    protobuf_init_file = os.path.join(protobuf_install_dir, '__init__.py')
    if os.path.exists(protobuf_install_dir) and not os.path.exists(protobuf_init_file):
        open(protobuf_init_file, 'a').close()


def create_zip(to_dest: str = None):
    """Zip up the contents of the build directory into a zip file that can be deployed
    to AWS Lambda."""

    global build_dir

    print('')
    print('Building zip file for AWS Lambda...')

    zip_dest = os.getcwd() if to_dest is None else to_dest
    zip_path = os.path.join(zip_dest, f'python_lambda_build-{version}')

    shutil.make_archive(zip_path, 'zip', build_dir)

    print('')
    print(f'Successfully created Lambda build zip file: {zip_path}.zip')
    print('')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', type=str, required=False)
    args = parser.parse_args()

    print('')
    print('Creating build for AWS Lambda...')
    print('')

    initialize_current_working_dir()

    setup_build_dir()

    copy_source_to_build_dir()

    install_dependencies()

    create_zip(args.output_dir)

    sys.exit(0)
