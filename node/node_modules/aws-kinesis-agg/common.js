var ProtoBuf = require("protobufjs");
require('./constants');

module.exports.loadBuilder = function() {
	if (debug) {
		console.log("Loading Protocol Buffer Model from " + protofile);
	}

	// create the builder from the proto file
	var builder = ProtoBuf.loadProtoFile(__dirname + "/" + protofile);
	return builder.build(kplConfig[useKplVersion].messageName);
};