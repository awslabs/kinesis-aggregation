/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
var assert = require('assert');
var libPath = "..";
var crypto = require("crypto"), md5 = crypto.createHash('md5'), should = require('should'), fs = require("fs"), ProtoBuf = require('protobufjs'), uuid = require('node-uuid');
var constants = require("constants");
var RecordAggregator = require(libPath + '/RecordAggregator');
var KINESIS_MAX_PAYLOAD_BYTES = 1048576;

var emitCount = 0;

var onReady = function(message, err, emitData) {
	console.log(message);
	if (err) {
		console.log(JSON.stringify(err));
	} else {
		if (emitData) {
			emitCount += 1;
			console.log("Received "
					+ Buffer.byteLength(emitData.Data.toString('base64'))
					+ " bytes");
		} else {
			console.log("No Data");
		}
	}
};

var continuousFunction = onReady.bind(undefined, 'Flush Continuous');
var agg = new RecordAggregator();
var messageSize = 1024;
var recordCount = 1000;
var records = [];

for (var i = 0; i < recordCount; i++) {
	var u = uuid.v4();
	var record = {
		PartitionKey : u,
		ExplicitHashKey : u,
		// random payload
		Data : new Buffer(crypto.randomBytes(messageSize).toString('base64'))
	};

	records.push(record);
}

// aggregate the records
agg.aggregateRecords(records, false, undefined, continuousFunction);

agg.flushBufferedRecords(onReady.bind(undefined, 'Flush Final'));

emitCount.should.equal(Math.ceil(recordCount * messageSize
		/ KINESIS_MAX_PAYLOAD_BYTES) + 1);
