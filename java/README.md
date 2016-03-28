# Java Kinesis Aggregation & Deaggregation Modules

The Kinesis Aggregation/Deaggregation Libraries for Java provide the ability to do in-memory aggregation and deaggregation of standard Kinesis user records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) to allow for more efficient transmission of records.

## KinesisAggregator 

The [KinesisAggregator](KinesisAggregator) subproject contains Java classes that allow you to aggregate records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md).  Using record aggregation improves throughput and reduces costs when writing producer applications that publish data to Amazon Kinesis.

## KinesisDeaggregator

The [KinesisDeaggregator](KinesisDeaggregator) subproject contains Java classes that allow you to deaggregate records that were transmitted using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md), including those transmitted by the Kinesis Producer Library.  This library will allow you to deaggregate aggregated records in any Java environment, including AWS Lambda.

## KinesisTestConsumers

The [KinesisTestConsumers](KinesisTestConsumers) subproject contains Java examples of AWS Lambda functions that leverage the [KinesisDeaggregator](KinesisDeaggregator) subproject to demonstrate how to deaggregate Kinesis aggregated records in an AWS Lambda function.  You can build on the examples in this subproject to make your own Java-based AWS Lambda functions that can handle aggregated records.

## KinesisTestProducers

The [KinesisTestProducers](KinesisTestProducers) subproject contains three separate standalone Java applications that demonstrate different methods of sending data to Amazon Kinesis:

* Sending non-aggregated records via the standard Kinesis PutRecords API
* Sending aggregated records via the Amazon Kinesis Producer Library
* Sending aggregated records via the [KinesisAggregator](KinesisAggregator) utility in this project

These utilities can be used to help ensure that your Kinesis consumer applications can properly handle both standard and aggregated records.
 
----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.