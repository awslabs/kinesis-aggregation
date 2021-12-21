/**
 * @module common.js
 * Common methods.
 */
const ProtoBuf = require("protobufjs");
const kplProto = require('./kpl.json');

/**
 * Define some constant.
 */
const constants = {
	debug: false, // enable debug log message
	protofile: "kpl.json", // protbuf definition file
	useKplVersion: "0.9.0",
	kplConfig: {
		"0.9.0": {
			magicNumber: "f3899ac2",
			messageName: "AggregatedRecord"
		}
	}
};
module.exports.KplVersion = constants.useKplVersion;
module.exports.debug = constants.debug;


/**
 * Magic Number
 */
module.exports.magic =  Buffer.from(constants.kplConfig[constants.useKplVersion].magicNumber, 'hex');
module.exports.magicNumber = constants.kplConfig[constants.useKplVersion].magicNumber;


/**
 * Load builder of Kinesis Aggregated Record Format.
 */
const loadBuilder = () => {
	if (constants.debug) {
		console.log("Loading Protocol Buffer Model from " + constants.protofile);
	}

	// create the builder from the proto file
	const builder = ProtoBuf.Root.fromJSON(kplProto);
	return builder.lookupType(constants.kplConfig[constants.useKplVersion].messageName);
};
module.exports.loadBuilder = loadBuilder;

module.exports.v3FormatToV2Format = (record) => {
	const recordProps = Object.keys(record);
	const converted = {};
	for (let n = 0; n < recordProps.length; n++) {
		converted[recordProps[n].charAt(0).toLowerCase() + recordProps[n].slice(1)] = record[recordProps[n]];
	}
	return converted;
}

module.exports.v2FormatToV3Format = (record) => {
	const recordProps = Object.keys(record);
	const converted = {};
	for (let n = 0; n < recordProps.length; n++) {
		converted[recordProps[n].charAt(0).toUpperCase() + recordProps[n].slice(1)] = record[recordProps[n]];
	}
	return converted;
}

/**
 * AggregatedRecord encoder/decoder.
 * Global object which will hold the protocol buffer model
 */
const AggregatedRecord = loadBuilder();
module.exports.AggregatedRecord = AggregatedRecord;

// randomPartitionKey function definition
const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
const floor = Math.floor;
const random = Math.random;
const randomPartitionKey = () => floor(MAX_SAFE_INTEGER * random()).toString();
module.exports.randomPartitionKey = randomPartitionKey;
