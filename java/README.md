# Java Kinesis Aggregation & Deaggregation Modules

The Kinesis Aggregation/Deaggregation Libraries for Java provide the ability to do in-memory aggregation and deaggregation of standard Kinesis user records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) to allow for more efficient transmission of records.

## KinesisAggregator 

The [KinesisAggregator](KinesisAggregator) subproject contains Java classes that allow you to aggregate records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md).  Using record aggregation improves throughput and reduces costs when writing producer applications that publish data to Amazon Kinesis.

### Caution - this module is only suitable for low-value messages which are processed in aggregate. Do not use Kinesis Aggregation for data which is sensitive or where every message must be delivered, and where the KCL (including with AWS Lambda) is used for processing. DATA LOSS CAN OCCUR.

## KinesisDeaggregator

The Deaggregation subprojects contain Java classes that allow you to deaggregate records that were transmitted using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md), including those transmitted by the Kinesis Producer Library.  This library will allow you to deaggregate aggregated records in any Java environment, including AWS Lambda.

There are 2 versions of Deaggregator modules, based upon the AWS SDK version you are using:

| SDK | Project |
| --- | ------- |
|Version 1 | [KinesisDeaggregator](KinesisDeaggregator) |
|Version 2 | [KinesisDeaggregatorV2](KinesisDeaggregatorV2) |

## KinesisTestConsumers

The [KinesisTestConsumers](KinesisTestConsumers) subproject contains Java examples of AWS Lambda functions that leverage the [KinesisDeaggregator](KinesisDeaggregator) subproject to demonstrate how to deaggregate Kinesis aggregated records in an AWS Lambda function.  You can build on the examples in this subproject to make your own Java-based AWS Lambda functions that can handle aggregated records.

## KinesisTestProducers

The [KinesisTestProducers](KinesisTestProducers) subproject contains three separate standalone Java applications that demonstrate different methods of sending data to Amazon Kinesis:

* Sending non-aggregated records via the standard Kinesis PutRecords API
* Sending aggregated records via the Amazon Kinesis Producer Library
* Sending aggregated records via the [KinesisAggregator](KinesisAggregator) utility in this project

These utilities can be used to help ensure that your Kinesis consumer applications can properly handle both standard and aggregated records.
 
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
