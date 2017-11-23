# Node.js Kinesis Aggregation & Deaggregation Modules

These Kinesis Aggregation and Deaggregation modules provide a simple interface for creating and working with data using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) from any type of application. If you are generating Kinesis records using the Kinesis Producer Library (KPL), you can easily deaggregate and process that data from node.js. Alternatively, if you want to create Kinesis records that are tightly packed to reach the maximum record size, then this is straightforward to achieve. Using record aggregation improves throughput and reduces costs when writing applications that publish data to and consume data from Amazon Kinesis.

## Installation

The Node.js record aggregation/deaggregation modules are available on NPM as [aws-kinesis-agg](https://www.npmjs.com/package/aws-kinesis-agg).  To get started, include the `aws-kinesis-agg` module from npm into your new or existing NodeJS application:

```javascript
var agg = require('aws-kinesis-agg');
```

## Record Aggregation

Applications implemented in AWS Lambda often emit new events based on the events that were received in the function. They may be doing enrichment, parsing, or filtering, and the ability to take advantage of Kinesis record-based aggregation will result in more efficient systems.

To use Aggregation, you simply construct a `RecordAggregator` function:

```javascript
var agg = require('aws-kinesis-agg/RecordAggregator');
var aggregator = new RecordAggregator();
```

You can then aggregate batches of messages by using the `aggregate` function:

```javascript
aggregator.aggregate(rawRecords, function(err, encoded));
```

where `encoded` is of type `Buffer`. With the `aggregate` function, the supplied callback will be invoked when the number of records supplied exceeds the Kinesis maximum record size (1MB as of March 2016). You can also retrieve the records being aggregated by calling the `flushBufferedRecords` function:

```javascript
aggregator.flushBufferedRecords(function(err,encoded));
```

The following example demonstrates an AWS Lambda function that was does per-record processing and then transmits those record to Amazon Kinesis (pseudo code):

```javascript
// create a record aggregator
var aggregator = new RecordAggregator();

// create the function which sends data to Kinesis with a random partition key
var onReady = function(err,encoded) {
  myKinesisConnection.putRecord(Math.random(), encoded.toString('base64'));
};

// process each provided Record in the event
async.map(event.Records, function(record, asyncCallback) {
  // do your per record processing here
  var recordAfterProcessing = doSomething(record.kinesis);
  
  asyncCallback(null, recordAfterProcessing);
}, function(err, mapResults) {
  // aggregate the records and call the onReady function for each block of prepared messages which are 1MB in size
  aggregator.aggregate(mapResults,onReady);
  
  // flush any final messages that were under the emission threshold
  aggregator.flushBufferedRecords(onReady);
});
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

```javascript
deaggregateSync(kinesisRecord, computeChecksums, afterRecordCallback(err, UserRecord[]);
```
The `computeChecksums` parameter accepts a Boolean that indicates whether the checksum in the kinesis record should be validated. If the checksum is incorrect, an error will be returned via the `afterRecordCallback`.

### Asyncronous

The asyncronous model of deaggregation allows you to provide a callback which is invoked for each User Record that is extracted from the Kinesis Record. When all User Records have been extracted from the Kinesis Record, an ```afterRecordCallback``` is invoked which allows you to continue processing additional Kinesis Records that your consumer receives:

```javascript
deaggregate(kinesisRecord, computeChecksums, perRecordCallback(err, UserRecord), afterRecordCallback(err, errorKinesisRecord));
```
The `computeChecksums` parameter accepts a Boolean that indicates whether the checksum in the kinesis record should be validated. If the checksum is incorrect, an error will be returned via the `afterRecordCallback`.

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

### Examples

This module includes an example AWS Lambda function in the [index.js](index.js) file, which gives you easy ability to build new functions to process Kinesis aggregated message data. Both examples use [async.js](async.js) to process the received Kinesis Records.

#### Syncronous Example

```javascript
/**
 * Example lambda function which uses the syncronous deaggregation
 * interface to process Kinesis Records from the Event Source
 */
exports.exampleSync = function(event, context) {
	console.log("Processing Aggregated Messages using aws-kinesis-agg(sync)");

	handleNoProcess(event, function() {
		console.log("Processing " + event.Records.length + " Kinesis Input Records");
		var totalUserRecords = 0;

		async.map(event.Records, function(record, asyncCallback) {
			// use the deaggregateSync interface which receives a single
			// callback with an error and an array of Kinesis Records
			agg.deaggregateSync(record.kinesis, true, function(err, userRecords) {
				if (err) {
					console.log(err);
					asyncCallback(err);
				} else {
					console.log("Received " + userRecords.length + " Kinesis User Records");
					totalUserRecords += userRecords.length;

					userRecords.map(function(record) {
						var recordData = new Buffer(record.data, 'base64');

						console.log("Kinesis Aggregated User Record: " + JSON.stringify(record) + " " + recordData.toString('ascii'));
						
						// you can do something else useful with each kinesis User Record here
					});

					asyncCallback(err);
				}
			});
		}, function(err, results) {
			if (common.debug) {
				console.log("Completed processing " + totalUserRecords + " Kinesis User Records");
			}

			if (err) {
				finish(event, context, error, err);
			} else {
				finish(event, context, ok, "Success");
			}
		});
	});
};
```

#### Asyncronous Example

This example accumulates User Records into an enclosing array in a similar fashion to the syncronous interface:

```javascript
/**
 * Example lambda function which uses the asyncronous deaggregation
 * interface to process Kinesis Records from the Event Source
 */
exports.exampleAsync = function(event, context) {
	console.log("Processing Aggregated Messages using aws-kinesis-agg(async)");

	handleNoProcess(event, function() {
		// process all records in parallel
		var realRecords = [];

		console.log("Processing " + event.Records.length + " Kinesis Input Records");

		async.map(event.Records, function(record, asyncCallback) {
			// use the async deaggregate interface. the per-record callback
			// appends the records to an array, and the finally callback calls
			// the async callback to mark the kinesis record as completed within
			// async.js
			agg.deaggregate(record.kinesis, true, function(err, userRecord) {
				if (err) {
					console.log("Error on Record: " + err);
					asyncCallback(err);
				} else {
					var recordData = new Buffer(userRecord.data, 'base64');

					console.log("Per Record Callback Invoked with Record: " + JSON.stringify(userRecord) + " " + recordData.toString('ascii'));

					realRecords.push(userRecord);
					
					// you can do something else useful with each kinesis user record here
				}
			}, function(err) {
				console.log(err);
				asyncCallback(err);
			});
		}, function(err, results) {
			if (common.debug) {
				console.log("Kinesis Record Processing Completed");
				console.log("Processed " + realRecords.length + " Kinesis User Records");
			}

			if (err) {
				finish(event, context, error, err);
			} else {
				finish(event, context, ok, "Success");
			}
		});
	});
};
```

## Build & Deploy a Lambda Function to process Kinesis Records

One easy way to get started processing Kinesis data is to use AWS Lambda. By extending the [index.js](index.js) file, you can take advantage of Kinesis message deaggregation features without having to write any boilerplate code. To do this, fork the GitHub codebase to a new project, select whether you want to build on the exampleSync or exampleAsync interfaces, and write your Kinesis processing code as normal. You can run `node test.js` to test your code (including both Kinesis aggregated message data as well as non-aggregated plain Kinesis records). Give your function a name and version number in [package.json](package.json) and then when you are ready to run from AWS Lambda, use:

```bash
$ ./build.js true
```

which requires a local install of the [AWS Command Line Interface](https://aws.amazon.com/cli).  This script invokes `aws lambda upload-function-code` directly. When you are finally happy with your AWS Lambda module, ensure that `constants.js[debug]` is set to false to reduce the amount of output messages generated in CloudWatch Logging.


----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at [http://aws.amazon.com/asl/](http://aws.amazon.com/asl/)

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
