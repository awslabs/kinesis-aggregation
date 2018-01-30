# Node.js Kinesis Aggregation & Deaggregation Modules

These Kinesis Aggregation and Deaggregation modules provide a simple interface for creating and working with data using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) from any type of application. If you are generating Kinesis records using the Kinesis Producer Library (KPL), you can easily deaggregate and process that data from node.js. Alternatively, if you want to create Kinesis records that are tightly packed to reach the maximum record size, then this is straightforward to achieve. Using record aggregation improves throughput and reduces costs when writing applications that publish data to and consume data from Amazon Kinesis.

## Installation

The Node.js record aggregation/deaggregation modules are available on NPM as [aws-kinesis-agg](https://www.npmjs.com/package/aws-kinesis-agg).  To get started, include the `aws-kinesis-agg` module from npm into your new or existing NodeJS application:

```
var agg = require('aws-kinesis-agg');
```

## Record Deaggregation

When using deaggregation, you provide a single aggregated Kinesis Record and get back multiple Kinesis User Records. If a Kinesis record that is provided is *not* using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md), that's perfectly fine - you'll just get a single record output from the single record input. A Kinesis User Record that is returned from the deaggregation module looks like:

```
{
	partitionKey : String - The Partition Key provided when the record was submitted
	explicitPartitionKey : String - The Partition Key provided for the purposes of Shard Allocation
	sequenceNumber : BigInt - The sequence number assigned to the record on submission to Kinesis
	subSequenceNumber : Int - The sub-sequence number for the User Record in the aggregated record, if aggregation was in use by the producer
	data : Buffer - The original data transmitted by the producer (base64 encoded)
}
```

When you receive a Kinesis Record in your consumer application, you will extract the User Records using deaggregation methods in the `aws-kinesis-agg` module.  The `aws-kinesis-agg` module provides both syncronous and asyncronous methods of deaggregating records.

### Synchronous

The syncronous model of deaggregation extracts all the Kinesis User Records from the received Kinesis Record, and accumulates them into an array. The method then makes a callback with any errors encountered, and the array of User Records that were deaggregated:

```
deaggregateSync(kinesisRecord, computeChecksums, afterRecordCallback(err, UserRecord[]);
```

### Asyncronous

The asyncronous model of deaggregation allows you to provide a callback which is invoked for each User Record that is extracted from the Kinesis Record. When all User Records have been extracted from the Kinesis Record, an ```afterRecordCallback``` is invoked which allows you to continue processing additional Kinesis Records that your consumer receives:

```
deaggregate(kinesisRecord, computeChecksums, perRecordCallback(err, UserRecord), afterRecordCallback(err, errorKinesisRecord));
```
If any errors are encountered during processing of the `perRecordCallback`, then the `afterRecordCallback` is called with the `err` plus an error Record which contains the failed subSequenceNumber from the aggregated data with details about the enclosing Kinesis Record:

```
{
	partitionKey : String - The Partition Key of the enclosing Kinesis Aggregated Record
	explicitPartitionKey : String - The Partition Key provided for the purposes of Shard Allocation
	sequenceNumber : BigInt - The sequence number assigned to the record on submission of the Aggregated Record by the encoder
	subSequenceNumber : Int - The sub-sequence number for the failing User Record in the aggregated record, if aggregation was in use by the producer
	data : Buffer - The original protobuf message transmitted by the producer (base64 encoded)
}
```

The `computeChecksums` parameter accepts a Boolean that indicates whether the checksum in the kinesis record should be validated. If the checksum is incorrect, an error will be returned via the `afterRecordCallback`.


## Record Aggregation

Applications implemented in AWS Lambda often emit new events based on the events that were received in the function. They may be doing enrichment, parsing, or filtering, and the ability to take advantage of Kinesis record-based aggregation will result in more efficient systems.

To use Aggregation, you simply use `aggregate` function.

```
aggregate(records, encodedRecordHandler, afterPutAggregatedRecords, errorCallback)

```

With:
 - `encodedRecordHandler`: a callback `function (record, (err, data) => ...)` invoked when the number of records supplied exceeds the Kinesis maximum record size (1MB as of March 2016). record has field {data, partitionKey, explicitHashKey}
 - `afterPutAggregatedRecords`: a callback invoked when all call of `encodedRecordHandler` are finished.
 - `errorCallback`: a `function (err, data)` called each time that an error occurs


The following example demonstrates an AWS Lambda function that was does per-record processing and then transmits those record to Amazon Kinesis (pseudo code):

```
// create a record aggregator
const aggregate = require('aws-kinesis-agg').aggregate;

// create the function which sends data to Kinesis with a random partition key
var onReady = function(encodedRecord, callback) {
	// build putRecords params
	const params = {
		Data: encodedRecord.data,
		PartitionKey: encodedRecord.partitionKey,
		StreamName: 'my-stream'
	}
	if (encodedRecord.explicitHashKey) {
		params.ExplicitHashKey = encodedRecord.explicitHashKey
	}
	// send to kinesis
	// kinesisClient.putRecord(param , ...)
  	myKinesisConnection.putRecord(params, callback)
};

aggregator.aggregate(event.Records, onReady, () => { 
	// lambda is done close context
	...
}, (err, data) => {
    console.log(`${err}, ${data}`) 
})

```



## Examples

This module includes an example AWS Lambda function in the `example` folder, which gives you easy ability to build new functions to process Kinesis aggregated message data. Both examples use [async.js](async.js) to process the received Kinesis Records.


----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
