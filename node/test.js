/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var lambda = require('./index');
var libPath = "./node_modules/aws-kpl-agg"
var agg = require(libPath + '/MessageAggregator');
require(libPath + "/constants");

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
function context() {
}
context.done = function(status, message) {
	console.log("Context Closure Message - Status:" + JSON.stringify(status)
			+ " Message:" + JSON.stringify(message));

	if (status && status !== null) {
		process.exit(-1);
	} else {
		process.exit(0);
	}
};
context.getRemainingTimeInMillis = function() {
	return 60000;
};

var rawRecords = [ {
	PartitionKey : 'aaaaaaaaa',
	ExplicitHashKey : 'ccccccccc',
	Data : new Buffer('Testing KPL Aggregated Record 1')
}, {
	PartitionKey : 'bbbbbbbb',
	ExplicitHashKey : 'ccccccccc',
	Data : new Buffer('Testing KPL Aggregated Record 2')
} ];

try {
	agg.aggregate(rawRecords, function(err, encoded) {
		event.Records[0].kinesis.data = encoded;

		// invoke the function as a lambda invocation
		lambda.exampleAsync(event, context);
	});
} catch (e) {
	console.log(e);
	console.log(JSON.stringify(e));
	context.done(e);
}