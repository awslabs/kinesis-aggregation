/*!
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
const aggregate = require('aws-kinesis-agg').aggregate;
const async = require('async');

var ok = 'OK';
var error = 'ERROR';

/** function which closes the context correctly based on status and message */
var finish = function (event, context, status, message) {
	"use strict";

	console.log("Processing Complete");

	// log the event if we've failed
	if (status !== ok) {
		if (message) {
			console.log(message);
		}

		// ensure that Lambda doesn't checkpoint to kinesis
		context.done(status, JSON.stringify(message));
	} else {
		context.done(null, message);
	}
};

/** function which handles cases where the input message is malformed */
var handleNoProcess = function (event, callback) {
	"use strict";

	var noProcessReason;

	if (!event.Records || event.Records.length === 0) {
		noProcessReason = "Event contains no Data";
	}
	if (event.Records[0].eventSource !== "aws:kinesis") {
		noProcessReason = "Invalid Event Source " + event.Records[0].eventSource;
	}
	if (event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
		noProcessReason = "Unsupported Event Schema Version " +
			event.Records[0].kinesis.kinesisSchemaVersion;
	}

	if (noProcessReason) {
		finish(event, error, noProcessReason);
	} else {
		callback();
	}
};

function doSomething(input) {
	return input
};

exports.handler = function (event, context) {
	"use strict";

	console.log("Processing input records and emitting aggregated versions");

	handleNoProcess(event, function () {
		console.log("Processing " + event.Records.length +
			" Kinesis Input Records");
		var totalUserRecords = 0;

		// process each provided Record in the event
		async.map(event.Records, function (record, asyncCallback) {
			var recordAfterProcessing = doSomething(record.kinesis);
			asyncCallback(null, recordAfterProcessing);
		}, function (err, mapResults) {
			// aggregate the records and call the onReady function for each
			// block of prepared messages which are 1MB in size
			aggregate(mapResults, (err, encoded) => {
					console.log("Encoded records of size " + Buffer.byteLength(encoded) +
						" received");
					// build putRecords params
					const params = {
						Data: encodedRecord.data,
						PartitionKey: encodedRecord.partitionKey,
						StreamName: streamName
					}
					if (encodedRecord.explicitHashKey) {
						params.ExplicitHashKey = encodedRecord.explicitHashKey
					}
					// send to kinesis
					// kinesisClient.putRecord(param , ...)
				}, () => {
					// aggregation end
					finish(event, context, ok, 'Success')
				},
				(err, data) => {
					// error occurs when aggregate records
					console.log(`Error ${err}`)
				});

		});
	});
};
