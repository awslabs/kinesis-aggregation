/*!
 * aws-kinesis-agg 4.0.4
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
const crypto = require("crypto");

const common = require('./common');

/** synchronous deaggregation interface */
module.exports.deaggregateSync = function(kinesisRecord, computeChecksums,
		afterRecordCallback) {
	"use strict";

	var userRecords = [];

	// use the async deaggregation interface, and accumulate user records into
	// the userRecords array
	exports.deaggregate(kinesisRecord, computeChecksums, function(err,
			userRecord) {
		if (err) {
			afterRecordCallback(err);
		} else {
			userRecords.push(userRecord);
		}
	}, function(err) {
		afterRecordCallback(err, userRecords);
	});
};

/** asynchronous deaggregation interface */
module.exports.deaggregate = function(kinesisRecord, computeChecksums,
		perRecordCallback, afterRecordCallback) {
	"use strict";
	/* jshint -W069 */// suppress warnings about dot notation (use of
	// underscores in protobuf model)
	//
	// we receive the record data as a base64 encoded string
	const isV3DataFormat = typeof kinesisRecord.Data !== 'undefined';
	kinesisRecord = isV3DataFormat ? common.v3FormatToV2Format(kinesisRecord) : kinesisRecord;

	var recordBuffer = Buffer.from(kinesisRecord.data, 'base64');

	// first 4 bytes are the kpl assigned magic number
	// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	if (recordBuffer.slice(0, 4).toString('hex') === common.magicNumber) {
		try {

			// decode the protobuf binary from byte offset 4 to length-16 (last
			// 16 are checksum)
			var protobufMessage = common.AggregatedRecord.decode(recordBuffer.slice(4,
					recordBuffer.length - 16));

			// extract the kinesis record checksum
			var recordChecksum = recordBuffer.slice(recordBuffer.length - 16,
					recordBuffer.length).toString('base64');

			if (computeChecksums === true) {
				// compute a checksum from the serialised protobuf message
				var md5 = crypto.createHash('md5');
				md5.update(recordBuffer.slice(4, recordBuffer.length - 16));
				var calculatedChecksum = md5.digest('base64');

				// validate that the checksum is correct for the transmitted
				// data
				if (calculatedChecksum !== recordChecksum) {
					if (common.debug) {
						console.log("Record Checksum: " + recordChecksum);
						console.log("Calculated Checksum: "
								+ calculatedChecksum);
					}
					throw new Error("Invalid record checksum");
				}
			} else {
				if (common.debug) {
					console
							.log("WARN: Record Checksum Verification turned off");
				}
			}

			if (common.debug) {
				console.log("Found " + protobufMessage.records.length
						+ " KPL Encoded Messages");
			}

			// iterate over each User Record in order
			for (var i = 0; i < protobufMessage.records.length; i++) {
				try {
					var item = protobufMessage.records[i];

					// emit the per-record callback with the extracted partition
					// keys and sequence information
					const newRecord = {
						partitionKey : protobufMessage["partition_key_table"][item["partition_key_index"]],
						explicitPartitionKey : protobufMessage["explicit_hash_key_table"][item["explicit_hash_key_index"]],
						sequenceNumber : kinesisRecord.sequenceNumber,
						subSequenceNumber : i,
						data : (typeof item.Data !== 'undefined') ? item.Data.toString('base64') : item.data.toString('base64')
					};
					perRecordCallback(
							null,
							isV3DataFormat ? common.v2FormatToV3Format(newRecord) : newRecord
					);
				} catch (e) {
					// call the after record callback, indicating the enclosing
					// kinesis record information and the subsequence number of
					// the erroring user record
					const newRecord = {
						partitionKey : kinesisRecord.partitionKey,
						explicitPartitionKey : kinesisRecord.explicitPartitionKey,
						sequenceNumber : kinesisRecord.sequenceNumber,
						subSequenceNumber : i,
						data : kinesisRecord.data
					};
					afterRecordCallback(
							e,
							isV3DataFormat ? common.v2FormatToV3Format(newRecord) : newRecord
					);
				}
			}

			// finished processing the kinesis record
			afterRecordCallback();
		} catch (e) {
			afterRecordCallback(e);
		}
	} else {
		// not a KPL encoded message - no biggie - emit the record with
		// the same interface as if it was. Customers can differentiate KPL
		// user records vs plain Kinesis Records on the basis of the
		// sub-sequence number
		if (common.debug) {
			console
					.log("WARN: Non KPL Aggregated Message Processed for DeAggregation: "
							+ kinesisRecord.partitionKey
							+ "-"
							+ kinesisRecord.sequenceNumber);
		}
		const newRecord = {
			partitionKey : kinesisRecord.partitionKey,
			explicitPartitionKey : kinesisRecord.explicitPartitionKey,
			sequenceNumber : kinesisRecord.sequenceNumber,
			data : kinesisRecord.data
		};
		perRecordCallback(null, isV3DataFormat ? common.v2FormatToV3Format(newRecord) : newRecord);
		afterRecordCallback();
	}
};
