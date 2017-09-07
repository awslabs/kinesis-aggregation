/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */

'use strict';

var ProtoBuf = require("protobufjs");
var crypto = require("crypto");
var _ = require('underscore');
var libPath = ".";
var common = require('./common');
var constants = require('./constants');
var magic = new Buffer(constants.kplConfig[constants.useKplVersion].magicNumber, 'hex');
var AggregatedRecord;

// calculate the maximum amount of data to accumulate before emitting to
// kinesis. 1MB - 16 bytes for checksum and the length of the magic number
var KINESIS_MAX_PAYLOAD_BYTES = 1048576 - 16 - Buffer.byteLength(magic,'binary');

module.exports = RecordAggregator;

function calculateVarIntSize(value) {
	if (value < 0) {
		raise
		Error("Size values should not be negative.");
	} else if (value == 0) {
		return 1;
	}

	var numBitsNeeded = 0;

	// shift the value right one bit at a time until
	// there are no more '1' bits left...this should count
	// how many bits we need to represent the number
	while (value > 0) {
		numBitsNeeded++;
		value = value >> 1;
	}

	// varints only use 7 bits of the byte for the actual value
	var numVarintBytes = Math.trunc(numBitsNeeded / 7);
	if (numBitsNeeded % 7 > 0) {
		numVarintBytes += 1;
	}

	return numVarintBytes;
}

// function which encodes the provided records
function flush(records, onReadyCallback) {
	if (constants.debug) {
		console.log("Flushing " + records.length + " records.");
	}

	if (records.length == 0) {
		return;
	}

	// do our best to find a valid partition key to use
	var pk;
	for (var i = 0; i < records.length; i++) {
		if (records[i].PartitionKey) {
			pk = records[i].PartitionKey;
			break;
		}
	}

	// do our best to find a valid explicit hash key to use
	var ehk;
	for (var j = 0; j < records.length; j++) {
		if (records[j].ExplicitHashKey) {
			ehk = records[0].ExplicitHashKey;
			break;
		}
	}

	aggregate(records, function(err, aggRecord) {
		if (err) {
			onReadyCallback(err);
		} else {

			// call the provided callback
			onReadyCallback(undefined, {
				PartitionKey : pk,
				ExplicitHashKey : ehk,
				Data : aggRecord
			});
		}
	});
};

function getSizeIncrement(value) {
	var newBytes = 0;
	if (value) {
		newBytes += 1; // (message index + wire type for index table)
		newBytes += calculateVarIntSize(value.length); // size of value length
		// value
		newBytes += value.length; // actual value length
	}

	return newBytes;
};

function getSizeForFirstRecord(record) {
	var dataLength = Buffer.byteLength(record.Data, 'binary');
	var newBytes = getSizeIncrement(record.PartitionKey);
	var recordSize = calculateVarIntSize(0);
	if (record.ExplicitHashKey) {
		newBytes += getSizeIncrement(record.ExplicitHashKey);
		recordSize += 1 + calculateVarIntSize(0);
	}
	recordSize += 2 + calculateVarIntSize(dataLength) + dataLength;
	newBytes += 1 + calculateVarIntSize(recordSize) + recordSize;
	return newBytes;
}

function aggregate(records, callback) {
	if (constants.debug) {
		console.log("Protobuf Aggregation of " + records.length + " records");
	}

	try {
		if (!AggregatedRecord) {
			AggregatedRecord = common.loadBuilder();
		}

		var partitionKeyTable = {};
		var partitionKeyCount = 0;
		var explicitHashKeyTable = {};
		var explicitHashKeyCount = 0;
		var putRecords = [];

		records
				.map(function(record) {
					// add the partition key and explicit hash key entries
					if (!partitionKeyTable.hasOwnProperty(record.PartitionKey)) {
						partitionKeyTable[record.PartitionKey] = partitionKeyCount;
						partitionKeyCount += 1;
					}

					if (record.ExplicitHashKey
							&& !explicitHashKeyTable
								.hasOwnProperty(record.ExplicitHashKey)) {
						explicitHashKeyTable[record.ExplicitHashKey] = explicitHashKeyCount;
						explicitHashKeyCount += 1;
					}

					// add the AggregatedRecord object with partition and hash
					// key indexes
					putRecords
							.push({
								"partition_key_index" : partitionKeyTable[record.PartitionKey],
								"explicit_hash_key_index" : explicitHashKeyTable[record.ExplicitHashKey],
								data : record.Data,
								tags : []
							});
				});

		// encode the data
		var protoData = AggregatedRecord.encode({
			"partition_key_table" : Object.keys(partitionKeyTable),
			"explicit_hash_key_table" : Object.keys(explicitHashKeyTable),
			"records" : putRecords
		});

		if (constants.debug) {
			var debugRecords = [];
			putRecords.map(function(record) {
				debugRecords.push(record.data.toString('base64'));
			})
			console.log(JSON.stringify({
				"partition_key_table" : Object.keys(partitionKeyTable),
				"explicit_hash_key_table" : Object.keys(explicitHashKeyTable),
				records : debugRecords
			}));
		}

		var bufferData = protoData.toBuffer();

		// get the md5 for the encoded data
		var md5 = crypto.createHash('md5');
		md5.update(bufferData);
		var checksum = md5.digest();

		if (constants.debug) { console.log("Checksum: " + checksum.toString('base64')); }

		// create the final object as a concatenation of the magic KPL number,
		// the encoded data records, and the md5 checksum
		if(constants.debug) { console.log("actual totalBytes=" + Buffer.byteLength(bufferData,'binary')); }
		var finalBuffer = Buffer.concat([ magic, bufferData, checksum ]);
		if(constants.debug) { console.log("final totalBytes=" + Buffer.byteLength(finalBuffer,'binary')); }

		// call the provided callback with no-error and the final value
		callback(null, finalBuffer);
	} catch (e) {
		console.log(e);
		callback(e);
	}
};
module.exports.aggregate = aggregate;

// constructor
function RecordAggregator(records) {
	if (!AggregatedRecord) {
		AggregatedRecord = common.loadBuilder();
	}

	this.totalBytes = 0;
	this.putRecords = [];
	this.partitionKeyTable = {};
	this.partitionKeyCount = 0;
	this.explicitHashKeyTable = {};
	this.explicitHashKeyCount = 0;

	// initialise the current state with the provided records
	if (records) {
		this.totalBytes = calculateSize(records);
		this.putRecords.push(records);
	}
};

// reset this object to empty (all records currently in the object
// will be lost)
RecordAggregator.prototype.clearRecords = function() {
	this.totalBytes = 0;
	this.putRecords = [];
	this.partitionKeyTable = {};
	this.partitionKeyCount = 0;
	this.explicitHashKeyTable = {};
	this.explicitHashKeyCount = 0;
};

// method to force a flush of the current inflight records
RecordAggregator.prototype.flushBufferedRecords = function(onReadyCallback) {
	if(constants.debug) { console.log("calculated totalBytes=" + this.totalBytes); }
	flush(this.putRecords, onReadyCallback);
	this.clearRecords();
};

// method to aggregate a set of records
RecordAggregator.prototype.aggregateRecords = function(records, forceFlush,
		afterRecordsCallback, onReadyCallback) {
	var self = this;

	records
			.map(function(record) {
				var newBytes = 0;

				// calculate the total new message size when aggregated into
				// protobuf
				if (!self.partitionKeyTable.hasOwnProperty(record.PartitionKey)) {
					// add the size of the partition key when encoded
					newBytes += getSizeIncrement(record.PartitionKey);
				}

				if (record.ExplicitHashKey
						&& !self.explicitHashKeyTable
								.hasOwnProperty(record.ExplicitHashKey)) {
					// add the size of the explicit hash key when encoded
					newBytes += getSizeIncrement(record.ExplicitHashKey);
				}

				/* compute the data record length */
				var recordSize = 1;

				// add the sizes of the partition and hash key indexes
				var pkIndex = self.partitionKeyCount;
				if (self.partitionKeyTable.hasOwnProperty(record.PartitionKey)) {
					pkIndex = self.partitionKeyTable[record.PartitionKey];
				}
				recordSize += calculateVarIntSize(pkIndex);

				if (record.ExplicitHashKey) {
					recordSize += 1;
					var ehkIndex = self.explicitHashKeyCount;
					if (self.explicitHashKeyTable
							.hasOwnProperty(record.ExplicitHashKey)) {
						ehkIndex = self.explicitHashKeyTable[record.ExplicitHashKey];
					}
					recordSize += calculateVarIntSize(ehkIndex);
				}

				if (typeof (record.Data) === 'string') {
					record.Data = new Buffer(record.Data, 'binary');
				}
				var dataLength = Buffer.byteLength(record.Data, 'binary');

				// message index + wire type for record data
				recordSize += 1;
				// size of data length value
				recordSize += calculateVarIntSize(dataLength);
				// actual data length
				recordSize += dataLength;

				// message index + wire type for record
				newBytes += 1;
				// size of entire record length value
				newBytes += calculateVarIntSize(recordSize);
				// actual entire record length
				newBytes += recordSize;

				if (constants.debug) {
					console.log("Current Pending Size: "
							+ self.putRecords.length + " records, "
							+ self.totalBytes + " bytes");
					console.log("Next: " + newBytes + " bytes");
				}

				// if the size of this record would push us over the limit,
				// then encode the current set
				if (newBytes > KINESIS_MAX_PAYLOAD_BYTES) {
					onReadyCallback('Input record (PK=' + record.PartitionKey +
									', EHK=' + record.ExplicitHashKey +
									', SizeBytes=' + newBytes +
									') is too large to fit inside a single Kinesis record.');
				}
				else if ((self.totalBytes + newBytes) > KINESIS_MAX_PAYLOAD_BYTES) {
					if(constants.debug) { console.log("calculated totalBytes=" + self.totalBytes); }

					// flush with a copy of the current inflight records
					flush(_.clone(self.putRecords), onReadyCallback);
					self.clearRecords();

					// total size tracked is now the size of the current record
					self.totalBytes = getSizeForFirstRecord(record);

					// current inflight becomes just this record
					self.putRecords = [ record ];
				} else {
					// the current set of records is still within the kinesis
					// max
					// payload size so increment inflight/total bytes
					self.putRecords.push(record);
					self.totalBytes += newBytes;
				}

				if (!self.partitionKeyTable.hasOwnProperty(record.PartitionKey)) {
					// add the size of the partition key when encoded
					self.partitionKeyTable[record.PartitionKey] = self.partitionKeyCount;
				}
				self.partitionKeyCount += 1;

				if (record.ExplicitHashKey
						&& !self.explicitHashKeyTable
								.hasOwnProperty(record.ExplicitHashKey)) {
					// add the size of the explicit hash key when encoded
					self.explicitHashKeyTable[record.ExplicitHashKey] = self.explicitHashKeyCount;
				}
				self.explicitHashKeyCount += 1;
			});

	if (forceFlush === true && self.putRecords.length > 0) {
		flush(_.clone(self.putRecords), onReadyCallback);
		this.clearRecords();
	}

	// done - call the afterRecordsCallback
	if (afterRecordsCallback) {
		afterRecordsCallback();
	}
};
