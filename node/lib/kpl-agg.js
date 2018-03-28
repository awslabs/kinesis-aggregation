/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

'use strict';
const crypto = require("crypto");
const async = require('async')

const common = require('./common');
const AggregatedRecord = common.AggregatedRecord

// calculate the maximum amount of data to accumulate before emitting to
// kinesis. 1MB - 16 bytes for checksum and the length of the magic number
const KINESIS_MAX_PAYLOAD_BYTES = (1024 * 1024) - 16 - Buffer.byteLength(common.magic);


function calculateVarIntSize(value) {
	if (value < 0) {
		raise
		Error("Size values should not be negative.");
	} else if (value == 0) {
		return 1;
	}

	let numBitsNeeded = 0;

	// shift the value right one bit at a time until
	// there are no more '1' bits left...this should count
	// how many bits we need to represent the number
	while (value > 0) {
		numBitsNeeded++;
		value = value >> 1;
	}

	// varints only use 7 bits of the byte for the actual value
	let numVarintBytes = Math.trunc(numBitsNeeded / 7);
	if (numBitsNeeded % 7 > 0) {
		numVarintBytes += 1;
	}

	return numVarintBytes;
}

function getPotentialIndex(lookup, key, count) {
	const it = lookup[key]
	return (it) ? it : count
}

function calculateRecordSize(self, record) {
	let messageSize = 0;

	// calculate the total new message size when aggregated into protobuf
	if (!self.partitionKeyTable.hasOwnProperty(record.partitionKey)) {
		// add the size of the partition key when encoded
		const pkLength = record.partitionKey.length;
		messageSize += 1; // (message index + wire type for PK table)
		messageSize += calculateVarIntSize(pkLength); // size of pk lengthvalue
		messageSize += pkLength; // actual pk length
	}

	if (record.explicitHashKey && !self.explicitHashKeyTable.hasOwnProperty(record.explicitHashKey)) {
		// add the size of the explicit hash key when encoded
		const ehkLength = record.explicitHashKey.length;
		messageSize += 1; // (message index + wire type for EHK table)
		messageSize += calculateVarIntSize(ehkLength); /* size of ehk length value */
		messageSize += ehkLength; // actual ehk length
	}

	/* compute the data record length */

	// add the sizes of the partition and hash key indexes
	let innerRecordSize = 1;
	innerRecordSize += calculateVarIntSize(getPotentialIndex(self.partitionKeyTable, record.partitionKey, self.partitionKeyCount));
	// explicit hash key field (this is optional)
	if (record.explicitHashKey) {
		innerRecordSize += 1;
		innerRecordSize += calculateVarIntSize(getPotentialIndex(self.explicitHashKeyTable, record.explicitHashKey, self.explicitHashKeyCount));
	}

	if (typeof (record.data) === 'string') {
		record.data = Buffer.from(record.data); // default utf8
	}
	var dataLength = Buffer.byteLength(record.data, 'binary');

	// message index + wire type for record data
	innerRecordSize += 1;
	// size of data length value
	innerRecordSize += calculateVarIntSize(dataLength);
	// actual data length
	innerRecordSize += dataLength;

	// data field
	messageSize += 1; // message index + wire type for record
	messageSize += calculateVarIntSize(innerRecordSize); // size of entire record length value

	messageSize += innerRecordSize; // actual entire record length
	return messageSize
}

function aggregateRecord(records) {
	if (common.debug) {
		console.log("Protobuf Aggregation of " + records.length + " records");
	}

	const partitionKeyTable = {};
	let partitionKeyCount = 0;
	const explicitHashKeyTable = {};
	let explicitHashKeyCount = 0;
	const putRecords = records.map(function (record) {
		// add the partition key and explicit hash key entries
		if (!partitionKeyTable.hasOwnProperty(record.partitionKey)) {
			partitionKeyTable[record.partitionKey] = partitionKeyCount;
			partitionKeyCount += 1;
		}

		if (record.explicitHashKey &&
			!explicitHashKeyTable
			.hasOwnProperty(record.explicitHashKey)) {
			explicitHashKeyTable[record.explicitHashKey] = explicitHashKeyCount;
			explicitHashKeyCount += 1;
		}

		// add the AggregatedRecord object with partition and hash
		// key indexes
		return {
			"partition_key_index": partitionKeyTable[record.partitionKey],
			"explicit_hash_key_index": explicitHashKeyTable[record.explicitHashKey],
			data: record.data,
			tags: []
		};
	});

	// encode the data
	const protoData = AggregatedRecord.encode({
		"partition_key_table": Object.keys(partitionKeyTable),
		"explicit_hash_key_table": Object.keys(explicitHashKeyTable),
		"records": putRecords
	});

	if (common.debug) {
		console.log(JSON.stringify({
			"partition_key_table": Object.keys(partitionKeyTable),
			"explicit_hash_key_table": Object.keys(explicitHashKeyTable),
			records: putRecords.map((record) => record.data.toString('base64'))
		}));
	}

	const bufferData = protoData.toBuffer();

	// get the md5 for the encoded data
	const md5 = crypto.createHash('md5');
	md5.update(bufferData);
	const checksum = md5.digest();
	// create the final object as a concatenation of the magic KPL number,
	// the encoded data records, and the md5 checksum
	var finalBuffer = Buffer.concat([common.magic, bufferData, checksum]);
	if (common.debug) {
		console.log("Checksum: " + checksum.toString('base64'));
		console.log("actual totalBytes=" + Buffer.byteLength(bufferData, 'binary'));
		console.log("final totalBytes=" + Buffer.byteLength(finalBuffer, 'binary'));
	}
	return finalBuffer;
}


function generateEncodedRecord(records) {
	if (common.debug) {
		console.log("generate " + records.length + " records.");
	}

	if (records.length == 0) {
		return;
	}

	// do our best to find a valid partition key to use
	let pk;
	for (var i = 0; i < records.length; i++) {
		if (records[i].partitionKey) {
			pk = records[i].partitionKey;
			break;
		}
	}

	// do our best to find a valid explicit hash key to use
	let ehk;
	for (var j = 0; j < records.length; j++) {
		if (records[j].explicitHashKey) {
			ehk = records[0].explicitHashKey;
			break;
		}
	}
	const encodedRecord = {
		partitionKey: pk,
		data: aggregateRecord(records)
	}
	// if we find an ExplicitHashKey set it
	if(ehk !== undefined) {
		encodedRecord["ExplicitHashKey"] = ehk
	}
	// return encoded record 
	return encodedRecord
}

// call onReadyCallback with encoded record
function callOnReadyCallback(err, records, onReadyCallback) {
	if (onReadyCallback) {
		if (err) {
			onReadyCallback(err)
		} else {
			onReadyCallback(null, generateEncodedRecord(records))
		}
	}
}

/**
 * RecordAggregator build an object which aggregate records with a max size of 1Mo.
 * @param {*} onReadyCallback 
 */
function RecordAggregator(onReadyCallback) {

	this.totalBytes = 0;
	this.putRecords = [];
	this.partitionKeyTable = {};
	this.partitionKeyCount = 0;
	this.explicitHashKeyTable = {};
	this.explicitHashKeyCount = 0;

	this.onReadyCallback = onReadyCallback;

};
module.exports = RecordAggregator;

/**
 * Set onReadyCallback
 * @param {*} onReadyCallback  callback function
 * @returns current onReadyCallback function
 */
RecordAggregator.prototype.setOnReadyCallback = function (onReadyCallback) {
	if (onReadyCallback) {
		this.onReadyCallback = onReadyCallback
	}
	return this.onReadyCallback
}

/**
 * reset this object to empty (all records currently in the object
 * will be lost)
 */
RecordAggregator.prototype.clearRecords = function () {
	this.totalBytes = 0;
	this.putRecords = [];
	this.partitionKeyTable = {};
	this.partitionKeyCount = 0;
	this.explicitHashKeyTable = {};
	this.explicitHashKeyCount = 0;
};

/**
 * Method to flush of the current inflight records.
 * @param {function} onReadyCallback optional onReadyCallback function
 */
RecordAggregator.prototype.flushBufferedRecords = function (onReadyCallback) {
	if (common.debug) {
		console.log("calculated totalBytes=" + this.totalBytes);
	}
	callOnReadyCallback(null, this.putRecords, onReadyCallback ||  this.onReadyCallback);
	this.clearRecords();
};

/**
 * method to aggregate a set of records.
 * @param {*} records records to aggregate
 * @param {boolean} forceFlush if true call #flushBufferedRecords at end of process
 * @param {function} onReadyCallback optional onReadyCallback function
 */
RecordAggregator.prototype.aggregateRecords = function (records, forceFlush, onReadyCallback) {
	const self = this;
	const _onReadyCallback = onReadyCallback || this.onReadyCallback
	records.forEach(function (record) {

		let messageSize = calculateRecordSize(self, record)

		if (!record.data) {
			return callOnReadyCallback(new Error('Record.Data field is mandatory'), record, _onReadyCallback)
		}
		if (!record.partitionKey) {
			return callOnReadyCallback(new Error('record.partitionKey field is mandatory'), record, _onReadyCallback)
		}

		if (common.debug) {
			console.log("Current Pending Size: " +
				self.putRecords.length + " records, " +
				self.totalBytes + " bytes");
			console.log("Next: " + messageSize + " bytes");
		}

		// if the size of this record would push us over the limit,
		// then encode the current set
		if (messageSize > KINESIS_MAX_PAYLOAD_BYTES) {
			callOnReadyCallback(new Error('Input record (PK=' + record.partitionKey +
				', EHK=' + record.explicitHashKey +
				', SizeBytes=' + messageSize +
				') is too large to fit inside a single Kinesis record.'), null, _onReadyCallback);
		} else if ((self.totalBytes + messageSize) > KINESIS_MAX_PAYLOAD_BYTES) {
			if (common.debug) {
				console.log("calculated totalBytes=" + self.totalBytes);
			}
			callOnReadyCallback(null, self.putRecords, _onReadyCallback);
			self.clearRecords();

			// total size tracked is now the size of the current record
			self.totalBytes = calculateRecordSize(self, record)

			// current inflight becomes just this record
			self.putRecords = [record];
		} else {
			// the current set of records is still within the kinesis
			// max payload size so increment inflight/total bytes
			self.putRecords.push(record);
			self.totalBytes += messageSize;
		}

		if (!self.partitionKeyTable.hasOwnProperty(record.partitionKey)) {
			// add the size of the partition key when encoded
			self.partitionKeyTable[record.partitionKey] = self.partitionKeyCount;
			self.partitionKeyCount += 1;
		}

		if (record.explicitHashKey &&
			!self.explicitHashKeyTable
			.hasOwnProperty(record.explicitHashKey)) {
			// add the size of the explicit hash key when encoded
			self.explicitHashKeyTable[record.explicitHashKey] = self.explicitHashKeyCount;
			self.explicitHashKeyCount += 1;
		}

	});

	if (forceFlush === true && self.putRecords.length > 0) {
		callOnReadyCallback(null, this.putRecords, _onReadyCallback);
		this.clearRecords();
	}

};


/**
 * Aggregate function.
 * @param {*} records collection of record to send
 * @param {function} encodedRecordHandler function (encodedRecord, callback) which process each encoded record.
 * 		`encodedRecord` is an object with data, partitionKey and [explicitHashKey] fields.
 * @param {function} afterPutAggregatedRecords called once all records are processed
 * @param {function} errorCallback called each time an error occurs
 * @param {number} [queueSize] maximum concurrency when processing encoded records (1 per default)
 */
module.exports.aggregate = (records, encodedRecordHandler, afterPutAggregatedRecords, errorCallback, queueSize = 1) => {

	const taskHandler = (params, done) => {
		encodedRecordHandler(params, (err, result) => {
			if (err) {
				errorCallback(err, result)
			}
			done()
		})
	}

	const aggregatorQueue = async.queue(taskHandler, queueSize);

	// when all task is done call afterPutAggregatedRecords callback
	aggregatorQueue.drain = () => {
		afterPutAggregatedRecords()
	}

	// aggregator call back
	const onReadyCallback = (error, encoded) => {
		if (error) {
			return errorCallback(error, encoded)
		}
		aggregatorQueue.push(encoded)
	}

	const aggregator = new RecordAggregator(onReadyCallback)

	aggregator.aggregateRecords(records, true)

	if (!aggregatorQueue.started) {
		errorCallback(new Error('No records'))
		afterPutAggregatedRecords()
	}
}
