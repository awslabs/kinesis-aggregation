#Kinesis Aggregation/Deaggregation Libraries for Python
#
#Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved. 
#
#Licensed under the Amazon Software License (the "License").
#You may not use this file except in compliance with the License.
#A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
#or in the "license" file accompanying this file. This file is distributed
#on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#express or implied. See the License for the specific language governing
#permissions and limitations under the License.

import os.path
import shutil
import subprocess
import sys
import zipfile

#Add your PIP dependencies here
PIP_DEPENDENCIES = ['protobuf']

BUILD_DIR_NAME = 'build'

cur_dir = None
proj_dir = None
build_dir = None

def is_python_file(filename):
    '''Returns True if the input flie path has a .py extension. False otherwise.'''
    
    return os.path.isfile(filename) and os.path.splitext(filename)[-1] == '.py'


def initialize_current_working_dir():
    '''Forces the current working directory to be set to the directory where
    this build script lives (which should be the python project root).'''

    global cur_dir, proj_dir

    cur_dir = os.path.normpath(os.getcwd())
    print 'Current Working Directory = %s' % (os.getcwd())
    
    proj_dir = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
    print 'Project Directory = %s' % (proj_dir)
    
    if cur_dir != proj_dir:
        print 'Changing CWD to script directory...'
        os.chdir(proj_dir)        
    print 'Current Working Directory = %s' % (os.getcwd())

    
def setup_build_dir():
    '''Removes any existing build directories and creates a new one.  Due to issues with
    running "pip install <foo> -t <build_dir>" multiple times to the same directory, it's
    easier to just create a new build dir every time.'''
    
    global BUILD_DIR_NAME, build_dir
    
    print ''
    build_dir = os.path.join(os.getcwd(),BUILD_DIR_NAME)
    print 'Setting up build directory: %s' % (build_dir)
    if os.path.exists(build_dir):
        shutil.rmtree(build_dir,True)
    os.mkdir(build_dir)

    
def copy_source_to_build_dir():
    '''Copy all Python source files to the build directory.'''
    
    global BUILD_DIR_NAME, build_dir
    
    print ''
    print 'Looking for Python source files...'
    for source in os.listdir(os.getcwd()):
        if is_python_file(source) and source != __file__:
            print 'Copy file %s to build directory...' % (source)
            shutil.copy(source, build_dir)
        elif os.path.isdir(source) and source != BUILD_DIR_NAME:
            print 'Copy folder %s to build directory...' % (source)
            shutil.copytree(source, os.path.join(build_dir,source))
            
            
def install_dependencies():
    '''Using PIP, install all dependencies to the build directory.'''
    
    global PIP_DEPENDENCIES, build_dir
    
    #Make sure PIP is available on the command line
    print ''
    print 'Verifying PIP installation...'
    with open(os.devnull,'w') as devnull:
        aws_cmd_line_result = subprocess.call('pip', shell=True, stdout=devnull, stderr=subprocess.STDOUT)
        if aws_cmd_line_result != 0:
            print>>sys.stderr,'You do not have "pip" installed or it is not on your PATH.  This script requires access to it.'
            sys.exit(1)
        print 'Successfully located "pip".'
    
    #Install PIP dependencies to the build directory
    print ''
    print 'Installing necessary modules from pip...'
    for dependency in PIP_DEPENDENCIES:
        pip_install_cmd = 'pip install %s -t "%s"' % (dependency, build_dir)
        print pip_install_cmd
        pip_install_cmd_line_result = subprocess.call(pip_install_cmd, shell=True)
        if pip_install_cmd_line_result != 0:
            print>>sys.stderr,'Failed to install module via pip. Try running \'%s\' to debug the issue.' % (pip_install_cmd)
            sys.exit(1)
        print 'Successfully installed %s from pip.' % (dependency)

    #AWS Lambda has issues with the normal protobuf install lacking a root level __init__.py
    protobuf_install_dir = os.path.join(build_dir,'google')
    protobuf_init_file = os.path.join(protobuf_install_dir,'__init__.py')
    if os.path.exists(protobuf_install_dir) and not os.path.exists(protobuf_init_file):
        open(protobuf_init_file, 'a').close()

        
def create_zip():
    '''Zip up the contents of the build directory into a zip file that can be deployed
    to AWS Lambda.'''
    
    global build_dir
    
    print ''
    print 'Building zip file for AWS Lambda...'
    zip_file_path = os.path.join(os.getcwd(),'python_lambda_build.zip')
    os.chdir(build_dir)
    with zipfile.PyZipFile(zip_file_path, 'w') as output_zip:
        for item in os.listdir(build_dir):
            if os.path.isdir(item) or is_python_file(item):
                print 'Adding %s to zip...' % (item)
                output_zip.writepy(item)
    
    return zip_file_path

    
if __name__ == '__main__':
    
    print ''
    print 'Creating build for AWS Lambda...'
    print ''
    
    initialize_current_working_dir()
    
    setup_build_dir()
    
    copy_source_to_build_dir()
    
    install_dependencies()
    
    zip_file_path = create_zip()
        
    print ''
    print 'Successfully created Lambda build zip file: %s' % (zip_file_path)
    print ''
    
    sys.exit(0)
    
