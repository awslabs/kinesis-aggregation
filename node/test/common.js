// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
const assert = require('assert')
const should = require('should')

const common = require('../lib/common')
describe('#common', () => {

    it('Current KplVersion is 0.9.0', () => {
        common.KplVersion.should.equal('0.9.0')
    })

    it('Current magicNumber is f3899ac2', () => {
        common.magicNumber.should.equal('f3899ac2')
    })

    it('magic buffer represent f3899ac2', () => {
        common.magic.toString('hex').should.equal('f3899ac2')
    })

    it('loadBuilder load object which will hold the protocol buffer model', () => {
        common.loadBuilder.should.be.Function()
        const builded = common.loadBuilder();
        builded.encode.should.be.Function()
        builded.decode.should.be.Function()
    })

    it('AggregatedRecord is an object which will hold the protocol buffer model', () => {
        common.AggregatedRecord.should.be.Object()
        common.AggregatedRecord.encode.should.be.Function()
        common.AggregatedRecord.decode.should.be.Function()
    })

    it('randomPartitionKey generate random string', () => {
        common.randomPartitionKey.should.be.Function()
        common.randomPartitionKey().should.be.String()
        common.randomPartitionKey().should.not.equal(common.randomPartitionKey())
    })

    it('v3FormatToV2Format should return object with property name converted to first letter lowercase', () => {
        common.v3FormatToV2Format.should.be.Function()
        common.v3FormatToV2Format({
            Data: 1,
            SequenceNumber: 1234
        }).should.deepEqual({
            data: 1,
            sequenceNumber: 1234
        })
    })

    it('v2FormatToV3Format should return object with property name converted to first letter uppercase', () => {
        common.v2FormatToV3Format.should.be.Function()
        common.v2FormatToV3Format({
            data: 1,
            sequenceNumber: 1234
        }).should.deepEqual({
            Data: 1,
            SequenceNumber: 1234
        })
    })
})
