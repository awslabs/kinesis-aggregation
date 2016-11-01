/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
var assert = require('assert');
var libPath = "..";
var crypto = require("crypto"), md5 = crypto.createHash('md5'), should = require('should'), fs = require("fs"), ProtoBuf = require('protobufjs');
var agg = require(libPath + '/RecordAggregator');
var constants = require(libPath + "/constants");

var rawRecords = [ {
	PartitionKey : 'aaaaaaaaa',
	ExplicitHashKey : 'ccccccccc',
	Data : new Buffer('Testing KPL Aggregated Record 1')
}, {
	PartitionKey : 'bbbbbbbb',
	ExplicitHashKey : 'ccccccccc',
	Data : new Buffer('Testing KPL Aggregated Record 2')
} ];

var aggRecord = {
	"partition_key_table" : [ rawRecords[0].PartitionKey,
			rawRecords[1].PartitionKey ],
	"explicit_hash_key_table" : [ rawRecords[0].ExplicitHashKey ],
	records : [ {
		"partition_key_index" : 0,
		"explicit_hash_key_index" : 0,
		data : rawRecords[0].Data,
		tags : []
	}, {
		"partition_key_index" : 1,
		"explicit_hash_key_index" : 0,
		data : rawRecords[1].Data,
		tags : []
	} ]
};
var builder = ProtoBuf.loadProtoFile(libPath + "/" + constants.protofile), AggregatedRecord = builder
		.build(constants.kplConfig[constants.useKplVersion].messageName);

// prepare data for sending a protobuf mocked message
var magic = new Buffer(constants.kplConfig[constants.useKplVersion].magicNumber, 'hex');
var protoData = AggregatedRecord.encode(aggRecord);

// record checksum
md5.update(protoData.toBuffer());
var checksum = md5.digest();

// capture the buffer
var manuallyGeneratedProtobufMessage = Buffer.concat(
		[ magic, protoData.toBuffer(), checksum ]).toString('base64');

agg.aggregate(rawRecords, function(err, encoded) {
	encoded.toString('base64').should.equal(manuallyGeneratedProtobufMessage);
	console.log("OK");
});
