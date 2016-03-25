/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var AWS = require('aws-sdk');
var libPath = "./node_modules/aws-kpl-agg"
var RecordAggregator = require(libPath + '/RecordAggregator');
require(libPath + "/constants");

// Used for generating random record bodies
var ALPHABET = 'abcdefghijklmnopqrstuvwxyz';

function randomRange(min, max) {
	return Math.trunc(Math.random() * (max - min)) + min;
}

function getRandomRecord(seqNum, desiredLen) {
	seqNum = seqNum !== undefined ? seqNum : 0;
	desiredLen = desiredLen !== undefined ? desiredLen : 50;

	var pk = (1.0 * Math.random()).toString().replace('.', '');
	var ehk = (1.0 * Math.random()).toString().replace('.', '');
	
	while (ehk[0] === '0' && ehk.length > 0) {
		ehk = ehk.substring(1);
	}

	var data = 'RECORD ' + seqNum.toString() + ' ';
	while (data.length < desiredLen - 1) {
		data += ALPHABET[randomRange(0, ALPHABET.length)]
	}
	data += '\n'

	return {
		'PartitionKey' : pk,
		'ExplicitHashKey' : ehk,
		'Data' : data
	};
}

function getKinesisClient(region_name) {
	return new AWS.Kinesis({
		'region' : region_name
	});
}

function sendRecord(kinesisClient, streamName, aggRecord) {
	params = {
		'StreamName' : streamName,
		'PartitionKey' : aggRecord.PartitionKey,
		'ExplicitHashKey' : aggRecord.ExplicitHashKey,
		'Data' : aggRecord.Data
	};

	var pk = aggRecord.PartitionKey;
	var ehk = aggRecord.ExplicitHashKey;

	console.log('Submitting record with PK=' + pk + ' EHK=' + ehk
			+ ' NumBytes=' + Buffer.byteLength(params.Data,'binary'));

	try {
		kinesisClient.putRecord(params, function(err, data) {
			if (err) {
				console.log('Transmission FAILED: ' + err);
				return;
			}

			console.log('Completed record with PK=' + pk + ' EHK=' + ehk);
		});
	} catch (err) {
		console.log('Transmission FAILED: ' + err);
	}
}

/*******************************************************************************
 * For setting AWS credentials, see:
 * https://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html
 ******************************************************************************/

var RECORD_SIZE_BYTES = 1024;
var RECORDS_TO_TRANSMIT = 1024;

if (process.argv.length != 4) {
	console.log("USAGE: node kinesis_publisher.js <stream name> <region>");
	process.exit(1);
}

var streamName = process.argv[2];
var region = process.argv[3];

var kinesisClient = getKinesisClient(region);

// create semi-random records to transmit
console.log('Creating ' + RECORDS_TO_TRANSMIT + ' records...');
var recordsToSend = [];
for (i = 1; i <= RECORDS_TO_TRANSMIT; i++) {
	var rec = getRandomRecord(i, RECORD_SIZE_BYTES);
	recordsToSend.push(rec);
}

// aggregate all the records
console.log('Aggregating...');
var aggregator = new RecordAggregator();
aggregator.aggregateRecords(recordsToSend, false, null,
		function(err, aggRecord) {
			sendRecord(kinesisClient, streamName, aggRecord);
		});

// flush out any remaining records we haven't handled yet
console.log('Flushing...');
aggregator.flushBufferedRecords(function(err, aggRecord) {
	sendRecord(kinesisClient, streamName, aggRecord);
});
