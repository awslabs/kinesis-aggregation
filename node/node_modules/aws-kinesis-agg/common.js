var ProtoBuf = require("protobufjs");
var constants = require('./constants');

module.exports.loadBuilder = function() {
	if (constants.debug) {
		console.log("Loading Protocol Buffer Model from " + constants.protofile);
	}

	// create the builder from the proto file
	var builder = ProtoBuf.loadProtoFile(__dirname + "/" + constants.protofile);
	return builder.build(constants.kplConfig[constants.useKplVersion].messageName);
};
