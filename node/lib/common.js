/**
 * @module common.js
 * Common methods.
 */
const ProtoBuf = require("protobufjs")
const Path = require('path')

/**
 * Define some constant.
 */
const constants = {
	debug: false, // enable debug log message
	protofile: "kpl.proto", // protbuf definition file
	useKplVersion: "0.9.0",
	kplConfig: {
		"0.9.0": {
			magicNumber: "f3899ac2",
			messageName: "AggregatedRecord"
		}
	}
}
module.exports.KplVersion = constants.useKplVersion
module.exports.debug = constants.debug


/**
 * Magic Number
 */
module.exports.magic = new Buffer(constants.kplConfig[constants.useKplVersion].magicNumber, 'hex')
module.exports.magicNumber = constants.kplConfig[constants.useKplVersion].magicNumber


/**
 * Load builder of Kinesis Aggregated Record Format.
 */
const loadBuilder = () => {
	if (constants.debug) {
		console.log("Loading Protocol Buffer Model from " + constants.protofile)
	}

	// create the builder from the proto file
	const builder = ProtoBuf.loadProtoFile(Path.resolve(__dirname, constants.protofile))
	return builder.build(constants.kplConfig[constants.useKplVersion].messageName)
}
module.exports.loadBuilder = loadBuilder

/**
 * AggregatedRecord encoder/decoder.
 * Global object which will hold the protocol buffer model
 */
const AggregatedRecord = loadBuilder();
module.exports.AggregatedRecord = AggregatedRecord

// randomPartitionKey function definition
const MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER
const floor = Math.floor
const random = Math.random
const randomPartitionKey = () => floor(MAX_SAFE_INTEGER * random()).toString()
module.exports.randomPartitionKey = randomPartitionKey
