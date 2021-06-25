# Python Kinesis Aggregation & Deaggregation Modules

The Kinesis Aggregation/Deaggregation Modules for Python provide the ability to do in-memory aggregation and deaggregation of standard Kinesis user records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) to allow for more efficient transmission of records.

## Installation

The Python record aggregation/deaggregation modules are available on the Python Package Index (PyPI) as [aws_kinesis_agg](https://pypi.python.org/pypi/aws_kinesis_agg).  You can install it via the `pip` command line tool:

```
pip install aws_kinesis_agg
```

Alternately, you can simply copy the aws_kinesis_agg module from this repository and use it directly with the caveat that the [Google protobuf module](https://pypi.python.org/pypi/protobuf) must also be available (if you install via `pip`, this dependency will be handled for you).

## Record Aggregation Module (aggregator.py)

The [aggregator.py](aggregator.py) module contains Python classes that allow you to aggregate records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md).  Using record aggregation improves throughput and reduces costs when writing producer applications that publish data to Amazon Kinesis.

### Caution - this module is only suitable for low-value messages which are processed in aggregate. Do not use Kinesis Aggregation for data which is sensitive or where every message must be delivered, and where the KCL (including with AWS Lambda) is used for processing. [DATA LOSS CAN OCCUR.](../potential_data_loss.md)

### Usage

The record aggregation module provides a simple interface for creating protocol buffers encoded data in a producer application. The `aws_kinesis_agg` module provides methods for efficiently packing individual records into larger aggregated records, and deaggregating large records into a set of 'real' user records.

When using aggregation, you create a managing class which helps you to target the correct Kinesis Shard, and then provide a partition key, raw data and (optionally) an explicit hash key for each record.  You can choose to either provide a callback function that will be invoked when a fully-packed aggregated record is available or you can add records and check byte sizes or number of records until the aggregated record is suitably full.  You're guaranteed that any aggregated record returned from the RecordAggregator object will fit within a single PutRecord request to Kinesis. As you produce records in your producer application, you will aggregate them using a base `RecordAggregator` object, which provides methods to do both iterative aggregation and callback-based aggregation.

There are two ways to create aggregated user records. The first is to use a raw `RecordAggregator`, which can aggregate messages *which are targeted for a single Shard*, or use the `AggregationManager` to aggregate messages which may span Shards. From version `1.2.0`, we __highly__ recommend the use of `AggregationManager` to limit any exposure to data loss.

### Aggregation Manager

Record Aggregation results in a single message that tightly packs `UserRecords`, and in v2 of the Kinesis Client Library all of the messages in a single aggregated payload must target the same Shard. Version 1.2.0 of Kinesis Aggregation for Python includes the `AggregationManager` class which ensures that aggregated messages will only target a single Kinesis Shard at a time. It does this by periodically refreshing the underlying Shard topology, and managing one `RecordAggregator` per destination Shard.

To use `AggregationManager`, import it:

```
import aws_kinesis_agg as agg
aggregation_manager = agg.AggregationManager(
	stream_name: str,
	region_name: str,
	refresh_shard_frequency_count: int
)
```

Where:

* `stream_name` is the name of the destination Stream
* `region_name` is the AWS Region in which the Stream is provisioned. Default is `us-east-1`
* `refresh_shard_frequency_count` is the number of aggregated records that can be added before a Shard refresh occurs. Default is 1000.

You can then perform aggregation by executing:

```
aggregation_manager.add_user_record(
	partition_key: str,
	explicit_hash_key: int = None,
	data
)
```

This will generate (including callbacks) or extract the `RecordAggregator` for the target Shard, and add the user record to it. You can also use the non-encapsulated format which access the underlying `RecordAggregator` directly:

```
aggregator = aggregation_manager.get_record_aggregator(
	partition_key:str,
	explicit_hash_key:int = None
)
aggregator.add_user_record(...)
```

Please __NOTE__ that every time the Shard cache is refreshed, all Callbacks will be executed and then `RecordAggregator` will be discarded. Therefore the use of Callbacks is mandatory.

### Raw Aggregation

You can construct a raw `RecordAggregator` class with:

```
import aws_kinesis_agg as agg
kinesis_aggregator = agg.RecordAggregator()
```

#### Iterative Aggregation

The iterative aggregation method involves adding records one at a time to the RecordAggregator and checking the response to determine when a full aggregated record is available.  The `add_user_record` method returns `None` when there is room for more records in the existing aggregated record and returns an `AggRecord` object when a full aggregated record is available for transmission.

```
for rec in records:
    result = kinesis_aggregator.add_user_record(rec.PartitionKey, rec.Data, rec.ExplicitHashKey)
    if result:
        #Send the result to Kinesis    
```

#### Callback-based Aggregation

To use callback-based aggregation, you must register a callback via the `on_record_complete` method.  As you add individual records to the `RecordAggregator` object, you will receive a callback (on a separate thread) whenever a new fully-packed aggregated record is available.

```
def my_callback(agg_record):
    #Send the record to Kinesis
   
...

kinesis_aggregator.on_record_complete(my_callback)
for rec in records:
    kinesis_aggregator.add_user_record(rec.PartitionKey, rec.Data, rec.ExplicitHashKey)
```

### Examples

This repository includes an example script that uses the record aggregation module [aggregator.py](aggregator.py) to aggregate records and transmit them to Amazon Kinesis using callback-based aggregation. You can find this example functionality in the file [kinesis_publisher.py](src/kinesis_publisher.py), which you can use as a template for your own applications to to easily build and transmit encoded data.

#### Callback-based Aggregation and Transmission Example

The example below assumes you are running Python version 2.7.x and also requires you to install and configure the `boto3` module.  You can install `boto3` via `pip install boto3` or any other normal Python install mechanism.  To configure the example to be able to publish to your Kinesis stream, make sure you follow the instructions in the [Boto3 Configuration Guide](https://boto3.readthedocs.org/en/latest/guide/configuration.html).  The example below has been stripped down for brevity, but you can still find the full working version at [kinesis_publisher.py](src/kinesis_publisher.py). The abridged example is:

```
import boto3
import aws_kinesis_agg.aggregator
    
kinesis_client = None
    
def send_record(agg_record):
    global kinesis_client
    pk, ehk, data = agg_record.get_contents()
    kinesis_client.put_record(StreamName='MyKinesisStreamName',
                                  Data=data,
                                  PartitionKey=pk,
                                  ExplicitHashKey=ehk)
    
if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis', region_name='us-west-2')
     
    kinesis_agg = aws_kinesis_agg.aggregator.RecordAggregator()
    kinesis_agg.on_record_complete(send_record)
    
    for i in range(0,1024):
        pk, ehk, data = get_record(...)
        kinesis_agg.add_user_record(pk, data, ehk)
    
    #Clear out any remaining records that didn't trigger a callback yet
    send_record(kinesis_agg.clear_and_get()) 
```


## Record Deaggregation Module (deaggregator.py)

The [deaggregator.py](deaggregator.py) module contains Python classes that allow you to deaggregate records that were transmitted using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md), including those transmitted by the Kinesis Producer Library.  This library will allow you to deaggregate aggregated records in any Python environment, including AWS Lambda.

### Usage

The record deaggregation module provides a simple interface for working with Kinesis aggregated message data in a consumer application. The `aws_kinesis_agg` module provides methods for both bulk and generator-based processing. 

When using deaggregation, you provide an aggregated Kinesis Record and get back multiple Kinesis User Records. If a Kinesis Record that is provided is not an aggregated Kinesis record, that's perfectly fine - you'll just get a single record output from the single record input. A Kinesis user record which is returned from deaggregation looks like:

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
        'aggregated' : Boolean - Always True for a user record extracted from a Kinesis aggregated record
    },
    'invokeIdentityArn' : String - The ARN of the IAM user used to invoke this Lambda function
    'eventName' : String - Always "aws:kinesis:record" for a Kinesis record
    'eventSourceARN' : String - The ARN of the source Kinesis stream
    'eventSource' : String - Always "aws:kinesis" for a Kinesis record
    'awsRegion' : String - The name of the source region for the event (e.g. "us-east-1")
}
```

To get started, import the `aws_kinesis_agg` module:

`import aws_kinesis_agg`

Next, when you receive a Kinesis Record in your consumer application, you will extract the user records using the deaggregation methods available in the `aws_kinesis_agg` module.

**IMPORTANT**: The deaggregation methods available in the `aws_kinesis_agg` module expect input records in the same dictionary-based format that they are normally received in from AWS Lambda. See the [Programming Model for Authoring Lambda Functions in Python](https://docs.aws.amazon.com/lambda/latest/dg/python-programming-model.html) section of the AWS documentation for more details.

#### Bulk Conversion

The bulk conversion method of deaggregation takes in a list of Kinesis Records, extracts all the aggregated user records and accumulates them into a list.  Any records that are passed in to this method that are not Kinesis aggregated records will be returned unchanged.  The method returns a list of Kinesis user records in the same format as they are normally delivered by Lambda's Kinesis event handler.

```
user_records = deaggregate_records(raw_kinesis_records)
```

#### Generator-based Conversion

The generator-based conversion method of deaggregation uses a Python [generator function](https://wiki.python.org/moin/Generators) to extract user records from a raw Kinesis Record one at a time in an iterative fashion.  Any records that are passed in to this method that are not Kinesis aggregated records will be returned unchanged.  For example, you could use this code to iterate through each deaggregated record:

```
for record in iter_deaggregate_records(raw_kinesis_records):        
        
    #Process each record
    pass 
```

### Examples

This module includes two example AWS Lambda function in the file [lambda_function.py](src/lambda_function.py) that give you the ability to easily build new functions to process Kinesis aggregated data via AWS Lambda.

#### Bulk Conversion Example

```
from __future__ import print_function

from aws_kinesis_agg.deaggregator import deaggregate_records
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

#### Generator-based Conversion Example

```
from __future__ import print_function

from aws_kinesis_agg.deaggregator import iter_deaggregate_records
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

### Build & Deploy a Lambda Function to process Kinesis Records

One easy way to get started processing Kinesis data is to use AWS Lambda.  By building on top of the existing [lambda_function.py](lambda_function.py) module in this repository, you can take advantage of Kinesis message deaggregation features without having to write boilerplate code.

When you're ready to make a build and upload to AWS Lambda, you have two choices:

* Follow the existing instructions at [Creating a Deployment Package (Python)](https://docs.aws.amazon.com/lambda/latest/dg/lambda-python-how-to-create-deployment-package.html)

OR 

* At the root of this Python project, you can find a sample build file called [make_lambda_build.py](make_lambda_build.py).  This file is a platform-agnostic build script that will take the existing Python project in this demo and package it in a single build file called `python_lambda_build.zip` that you can upload directly to AWS Lambda.

In order to use the build script, make sure that the python `pip` tool is available on your command line.  If you have other `pip` dependencies, make sure to add them to the `PIP_DEPENDENCIES` list at the top of the [make_lambda_build.py](make_lambda_build.py).  Then run this command:

```
python make_lambda_build.py
```

The build script will create a new folder called `build`, copy all the Python source files, download any necessary dependencies via `pip` and create the file `python_lambda_build.zip` that you can deploy to AWS Lambda.

#### Important Build Note for AWS Lambda Users

If you choose to make your own Python zip file to deploy to AWS Lambda, be aware that the Google [protobuf](https://pypi.python.org/pypi/protobuf) module normally relies on using a Python `pth` setting to make the root `google` module importable.  If you see an error in your AWS Lambda logs such as:

```
"Unable to import module 'lambda_function': No module named google.protobuf"
```

You can go into the `google` module folder (the same folder containing the `protobuf` folder) and make an empty file called `__init__.py`.  Once you rezip everything and redeploy, this should fix the error above.

**NOTE**: If you used the provided [make_lambda_build.py](make_lambda_build.py) script, this issue is already handled for you.
 
----

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
