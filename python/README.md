

# Python Kinesis Producer Library Deaggregation Module

The [Amazon Kinesis Producer Library (KPL)](http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-kpl.html) gives you the ability to write data to Amazon Kinesis with a highly efficient, asyncronous delivery model that can [improve performance](http://docs.aws.amazon.com/kinesis/latest/dev/developing-producers-with-kpl.html#d0e4909). When you write to the Producer, you can also elect to turn on Aggregation, which writes multiple producer events to a single Kinesis Record, aggregating lots of smaller events into a 1MB record. When you use Aggregation, the KPL serialises data to the Kinesis stream using [Google Protocol Buffers](https://developers.google.com/protocol-buffers), and consumer applications must be able to deserialise this protobuf message. This module gives you the ability to process KPL serialised data using any Python consumer including [AWS Lambda](https://aws.amazon.com/lambda).

## Installation

The Python KPL Deaggregation module is available on the Python Package Index (PyPI) as [aws_kpl_deagg](https://pypi.python.org/pypi/aws_kpl_deagg).  You can install it via the `pip` command line tool:

```
pip install aws_kpl_deagg
```

Alternately, you can simply copy the aws_kpl_deagg module from this repository and use it directly with the caveat that the [Google protobuf module](https://pypi.python.org/pypi/protobuf) must also be available (if you install via `pip`, this dependency will be handled for you).

## Usage

The Python KPL Deaggregation module provides a simple interface for working with KPL encoded data in a consumer application. The aws_kpl_deagg Python module provides for both bulk and generator-based processing. 

When using deaggregation, you provide a Kinesis Record, and get back multiple Kinesis User Records. If a Kinesis Record that is provided is not a KPL encoded message, that's perfectly fine - you'll just get a single record output from the single record input. A Kinesis User Record which is returned from the kpl-deagg looks like:

```
{
    'eventVersion' : String - The version number of the Kinesis event used
    'eventID' : String - The unique ID of this Kinesis event
    'kinesis' :
    {
        'partitionKey' : String - The Partition Key provided when the record was submitted
		'explicitHashKey' : String - The hash value used to explicitly determine the shard the data record is assigned to by overriding the partition key hash (or None if absent) 
        'data' : String - The original data transmitted by the producer (base64 encoded)
        'kinesisSchemaVersion' : String - The version number of the Kinesis message schema used,
        'sequenceNumber' : BigInt - The sequence number assigned to the record on submission to Kinesis
		'subSequenceNumber' : Int - The sub-sequence number for the User Record in the aggregated record, if aggregation was in use by the producer
		'aggregated' : Boolean - Always True for a user record extracted from a KPL aggregated record
    },
    'invokeIdentityArn' : String - The ARN of the IAM user used to invoke this Lambda function
    'eventName' : String - Always "aws:kinesis:record" for a Kinesis record
    'eventSourceARN' : String - The ARN of the source Kinesis stream
    'eventSource' : String - Always "aws:kinesis" for a Kinesis record
    'awsRegion' : String - The name of the source region for the event (e.g. "us-east-1")
}
```

To get started, include the `aws_kpl_deagg` module:

`import aws_kpl_deagg`

Next, when you receive a Kinesis Record in your consumer application, you will extract the User Records using the deaggregations methods available in the `aws_kpl_deagg` module.

**IMPORTANT**: The deaggregation methods available in the `aws_kpl_deagg` module expect input records in the same dictionary-based format they are normally received from AWS Lambda. See the [Programming Model for Authoring Lambda Functions in Python](https://docs.aws.amazon.com/lambda/latest/dg/python-programming-model.html) section of the AWS documentation for more details.

### Bulk Conversion

The bulk conversion method of deaggregation takes in a list of Kinesis Records, extracts all the aggregated User Records and accumulates them into a list.  Any records that are passed in to this method that are not KPL-aggregated records will be returned unchanged.  The method returns a list of Kinesis User Records in the same format as they are normally delivered by Lambda's Kinesis event handler.

```
user_records = deaggregate_records(raw_kinesis_records)
```

### Generator-based Conversion

The generator-based conversion method of deaggregation uses a Python [generator function](https://wiki.python.org/moin/Generators) to extract User Records from a raw Kinesis Record one at a time in an iterative fashion.  Any records that are passed in to this method that are not KPL-aggregated records will be returned unchanged.  For example, you could use this code to iterate through each deaggregated record:

```
for record in iter_deaggregate_records(raw_kinesis_records):        
        
	#Process each record
	pass 
```

## Examples

This module includes two example AWS Lambda function in the file [lambda_function.py](src/lambda_function.py), which gives you the ability to easily build new functions to process KPL encoded data.

### Bulk Conversion Example

```
from __future__ import print_function

from aws_kpl_deagg.deaggregator import deaggregate_records
import base64

def lambda_bulk_handler(event, context):
    
    raw_kinesis_records = event['Records']
    
    #Deaggregate all records in one call
    user_records = deaggregate_records(raw_kinesis_records)
    
    #Iterate through deaggregated records
    for record in user_records:        
        
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        
        #TODO: Process each record
    
    return 'Successfully processed {} records.'.format(len(user_records))
```

### Generator-based Conversion Example

```
from __future__ import print_function

from aws_kpl_deagg.deaggregator import iter_deaggregate_records
import base64

def lambda_generator_handler(event, context):
    
    raw_kinesis_records = event['Records']
    record_count = 0
    
    #Deaggregate all records using a generator function
    for record in iter_deaggregate_records(raw_kinesis_records):   
             
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
       
        #TODO: Process each record
       
        record_count += 1
        
    return 'Successfully processed {} records.'.format(record_count)
```

## Build & Deploy a Lambda Function to process Kinesis Records

One easy way to get started processing Kinesis data is to use AWS Lambda.  By building on top of the existing [lambda_function.py](lambda_function.py) module in this repository, you can take advantage of KPL deaggregation features without having to write boilerplate code.

When you're ready to make a build and upload to AWS Lambda, you have two choices:

* Follow the existing instructions at [Creating a Deployment Package (Python)](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html)

OR 

* At the root of this Python project, you can find a sample build file called [make_lambda_build.py](make_lambda_build.py).  This file is platform-agnostic build script that will take the existing Python project in this demo and package it in a single build file called `python_lambda_build.zip` that you can upload directly to AWS Lambda.

In order to use the build script, make sure that the python `pip` tool is available on your command line.  If you have other `pip` dependencies, make sure to add them to the `PIP_DEPENDENCIES` list at the top of the [make_lambda_build.py](make_lambda_build.py).  Then run this command:

```
python make_lambda_build.py
```

The build script will create a new folder called `build`, copy all the Python source files, download any necessary dependencies via `pip` and create the file `python_lambda_build.zip` that you can deploy to AWS Lambda.

### Important Build Note for AWS Lambda Users

If you choose to make your own Python zip file to deploy to AWS Lambda, be aware that the Google [protobuf](https://pypi.python.org/pypi/protobuf) module normally relies on using a Python `pth` setting to make the root `google` module importable.  If you see an error in your AWS Lambda logs such as:

```
"Unable to import module 'lambda_function': No module named google.protobuf"
```

You can go into the `google` module folder (the same folder containing the `protobuf` folder) and make an empty file called `__init__.py`.  Once you rezip everything and redeploy, this should fix the error above.

**NOTE**: If you used the provided [make_lambda_build.py](make_lambda_build.py) script, this issue is already handled for you.
 
----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.