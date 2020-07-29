/*!
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
var deagg = require('aws-kinesis-agg/kpl-deagg');
var async = require('async');
require('constants');
var computeChecksums = true;

var ok = 'OK';
var error = 'ERROR';

/** function which closes the context correctly based on status and message */
var finish = function(event, context, status, message) {
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
var handleNoProcess = function(event, callback) {
	"use strict";

	var noProcessReason;

	if (!event.Records || event.Records.length === 0) {
		noProcessReason = "Event contains no Data";
	}
	if (event.Records[0].eventSource !== "aws:kinesis") {
		noProcessReason = "Invalid Event Source " + event.Records[0].eventSource;
	}
	if (event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
		noProcessReason = "Unsupported Event Schema Version " + event.Records[0].kinesis.kinesisSchemaVersion;
	}

	if (noProcessReason) {
		finish(event, error, noProcessReason);
	} else {
		callback();
	}
};

/**
 * Example lambda function which uses the KPL asyncronous deaggregation
 * interface to process Kinesis Records from the Event Source
 */
exports.exampleAsync = function(event, context) {
	"use strict";

	console.log("Processing KPL Aggregated Messages using kpl-deagg(async)");

	handleNoProcess(event, function() {
		var realRecords = [];

		console.log("Processing " + event.Records.length + " Kinesis Input Records");

		// process all records in parallel
		async.map(event.Records, function(record, asyncCallback) {
			// use the async deaggregate interface. the per-record callback
			// appends the records to an array, and the after record callback
			// calls the async callback to mark the kinesis record as completed
			// within
			// the async map operation
			deagg.deaggregate(record.kinesis, computeChecksums, function(err, userRecord) {
				if (err) {
					console.log("Error on Record: " + err);
					asyncCallback(err);
				} else {
					var recordData = new Buffer(userRecord.data, 'base64');

					console.log("Per Record Callback Invoked with Record: " + recordData.toString('ascii'));

					realRecords.push(userRecord);

					// you can do something else with each kinesis user
					// record here!
				}
			}, function(err) {
				if (err) {
					console.log(err);
				}

				// call the async callback to reflect that the kinesis message
				// is completed
				asyncCallback(err);
			});
		}, function(err, results) {
			// function is called once all kinesis records have been processed
			// by async.map
			if (debug) {
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

/**
 * Example lambda function which uses the KPL syncronous deaggregation interface
 * to process Kinesis Records from the Event Source
 */
exports.exampleSync = function(event, context) {
	"use strict";

	console.log("Processing KPL Aggregated Messages using kpl-deagg(sync)");

	handleNoProcess(event, function() {
		console.log("Processing " + event.Records.length + " Kinesis Input Records");
		var totalUserRecords = 0;

		async.map(event.Records, function(record, asyncCallback) {
			// use the deaggregateSync interface which receives a single
			// callback with an error and an array of Kinesis Records
			deagg.deaggregateSync(record.kinesis, computeChecksums, function(err, userRecords) {
				if (err) {
					console.log(err);
					asyncCallback(err);
				} else {
					console.log("Received " + userRecords.length + " Kinesis User Records");
					totalUserRecords += userRecords.length;

					userRecords.map(function(record) {
						var recordData = new Buffer(record.data, 'base64');

						console.log("Kinesis Aggregated User Record:" + recordData.toString('ascii'));

						// you can do something else with each kinesis
						// user record here!
					});

					// call the async callback to reflect that the kinesis
					// message is completed
					asyncCallback(err);
				}
			});
		}, function(err, results) {
			// function is called once all kinesis records have been processed
			// by async.map
			if (debug) {
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
