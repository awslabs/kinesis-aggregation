/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var lambda = require('./index');
var crypto = require("crypto");
var md5 = crypto.createHash('md5');
var deaggPath = "./node_modules/kpl-deagg/";

require(deaggPath + "constants");

// sample kinesis event - record 0 has no data, and will be filled in with
// dynamic content using protobuf
var event = {
	"Records" : [
			{
				"kinesis" : {
					"kinesisSchemaVersion" : "1.0",
					"partitionKey" : "-48903309263388366",
					"sequenceNumber" : "49550822123942288925422195661801699673398497972964035234",
					"data" : undefined
				},
				"eventSource" : "aws:kinesis",
				"eventVersion" : "1.0",
				"eventID" : "shardId-000000000176:49550822123942288925422195661801699673398497972964035234",
				"eventName" : "aws:kinesis:record",
				"invokeIdentityArn" : "arn:aws:iam::999999999999:role/LambdaExecRole",
				"awsRegion" : "eu-west-1",
				"eventSourceARN" : "arn:aws:kinesis:eu-west-1:999999999999:stream/MyStream"
			},
			{
				"kinesis" : {
					"kinesisSchemaVersion" : "1.0",
					"partitionKey" : "-48903309263388367",
					"sequenceNumber" : "49550822123942288925422195661801699673398497972964035235",
					"data" : "MzcxICgxZikgdHMtMTQzNTczODI4ODkxOSA1Ni4zNjM5MTkwNzg3ODk0NXgtMS42NDA1NjI4ODM3NDE1MjAzIDEwOS45NzkzOTQwMzc4NDA1NSBhdCAxNi4xMjMyNjMyOTY0NjM2MDUgVDoyLjIxMTY3MjU2ODE0NTYwNDQgYzogIDAuMDAxMTk0IGRlZyAgMC4wMDAwMDE="
				},
				"eventSource" : "aws:kinesis",
				"eventVersion" : "1.0",
				"eventID" : "shardId-000000000176:49550822123942288925422195661801699673398497972964035234",
				"eventName" : "aws:kinesis:record",
				"invokeIdentityArn" : "arn:aws:iam::999999999999:role/LambdaExecRole",
				"awsRegion" : "eu-west-1",
				"eventSourceARN" : "arn:aws:kinesis:eu-west-1:999999999999:stream/MyStream"
			} ]
};

/** mock context object to simulate AWS Lambda context */
function context() {}
context.done = function(status, message) {
	console.log("Context Closure Message - Status:" + JSON.stringify(status) + " Message:" + JSON.stringify(message));

	if (status && status !== null) {
		process.exit(-1);
	} else {
		process.exit(0);
	}
};
context.getRemainingTimeInMillis = function() {
	return 60000;
};

// create a set of protobuf messages
var fs = require("fs");
var ProtoBuf = require('protobufjs');

var aggRecord = {
	"partition_key_table" : [ 'aaaaaaaaa', 'bbbbbbbb' ],
	"explicit_hash_key_table" : [ 'ccccccccc' ],
	records : [ {
		"partition_key_index" : 0,
		"explicit_hash_key_index" : 0,
		data : new Buffer('Testing KPL Aggregated Record 1'),
		tags : []
	}, {
		"partition_key_index" : 1,
		"explicit_hash_key_index" : 0,
		data : new Buffer('Testing KPL Aggregated Record 2'),
		tags : []
	} ]
};

try {
	var builder = ProtoBuf.loadProtoFile(deaggPath + protofile), AggregatedRecord = builder.build(kplConfig[useKplVersion].messageName);

	// prepare data for sending a protobuf mocked message
	var magic = new Buffer(kplConfig[useKplVersion].magicNumber, 'hex');
	var protoData = AggregatedRecord.encode(aggRecord);

	// record checksum
	md5.update(protoData.toBuffer());

	var checksum = md5.digest();

	event.Records[0].kinesis.data = Buffer.concat([ magic, protoData.toBuffer(), checksum ]).toString('base64');

	// invoke the function as a lambda invocation
	lambda.exampleAsync(event, context);
} catch (e) {
	console.log(e);
	console.log(JSON.stringify(e));
	context.done(e);
}