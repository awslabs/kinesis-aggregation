# Kinesis Java Record Aggregator Test Producers

This project contains a number of sample executable Java classes that demonstrate different methods of publishing records to Amazon Kinesis.

## Runnable Classes

This project provides three different runnable Java classes for publishing data to Amazon Kinesis:

`SampleNormalProducer.java` - Uses a standard Kinesis `PutRecords` call to send records to the Kinesis stream in a single batch.

`SampleKPLProducer.java` - Uses the official Kinesis Producer Library (KPL) to send records to the Kinesis stream as aggregated records.

`SampleAggregatorProducer.java` - Uses the `KinesisAggregator` project from this repository to create aggregated records and send them to Kinesis.

`SampleAggregatorProducerKCLCompliant.java` - Uses the `KinesisAggregator` project from this repository to create aggregated records and send them to Kinesis. It uses the same PartitionKey and ExplicitHashKey on all UserRecords that belongs to the same KinesisRecord in order to be compliant with KCL.

## Run Instructions

1. Build the project with Maven: `mvn install`

2. The producers rely on the Java [DefaultAWSCredentialsProviderChain] (https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html) to set permissions.  You must supply credentials that have Put* access to the Kinesis stream you specify.  See [Using the Default Credential Provider Chain] (http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html#id1) for the various methods you can use to supply AWS credentials.

3.  The command to run the producers is of the form:

`java -cp target/KinesisTestProducers-1.0.jar com.amazonaws.kinesis.producer.<ProducerClassName> <streamName> <region>` 

E.g.:

`java -cp target/KinesisTestProducers-1.0.jar com.amazonaws.kinesis.producer.SampleNormalProducer myStreamName us-east-1`

## Configuring Runtime Behavior

The supplied `ProducerConfig.java` class is a simple configuration shared by the various sample producer applications mentioned above.  You can tune the `RECORD SIZE BYTES` variable to control how big each transmitted user record is and you can use the `RECORDS_TO_TRANSMIT` variable to control how many records are sent during each run of the application.

## Sample Record

The sample records sent by these applications look like this:

```
RECORD 42 wajbxagglpzpbuxfoeoxznhwlrrxhsnnfjkprznvxedqfxpqucfbwaiudhkgzbzmdamsjkezcfrrredlfndbudldudfipzkar
```

The `RECORD_SIZE_BYTES` variable in `ProducerConfig.java` file will control how big the records are (the bigger the record size, the longer the random character string at the end of the record).

----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.