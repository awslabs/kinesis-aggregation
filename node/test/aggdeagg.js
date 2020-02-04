// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
const assert = require('assert')
const sinon = require('sinon');
const should = require('should')
require('should-sinon')

const aggregate = require('../index').aggregate
const deaggregate = require('../index').deaggregate

const data1 = 'Testing KPL Aggregated Record 1 我愛你';
const data2 = 'Testing KPL Aggregated Record 2';

describe('#deaggregate-aggregate', () => {

    it('deaggregates aggregated record', (done) => {
        const errorHandlerFunction = sinon.spy()

        const rawRecords = [{
            partitionKey: 'aaaaaaaaa',
            explicitHashKey: 'ccccccccc',
            data: data1
        }, {
            partitionKey: 'bbbbbbbb',
            explicitHashKey: 'ccccccccc',
            data: Buffer.from(data2)
        }]

        const deaggregatedRecords = []

        const encodedRecordHandler = (encodedRecord, callback) => {
            deaggregate(encodedRecord, true, (err, record) => {
                    deaggregatedRecords.push(record)
                },
                () => {
                })
            callback(null, encodedRecord)
        }

        errorHandlerFunction.should.have.callCount(0)

        aggregate(rawRecords, encodedRecordHandler, () => {
            errorHandlerFunction.should.have.callCount(0)

            deaggregatedRecords.length.should.be.equal(2)

            deaggregatedRecords.forEach((item) => {
                item.explicitPartitionKey.should.be.equal('ccccccccc');

                if (item.partitionKey == 'aaaaaaaaa') {
                    Buffer.from(item.data, 'base64').toString('utf-8').should.be.equal(data1)
                }
                if (item.partitionKey == 'bbbbbbbb') {
                    Buffer.from(item.data, 'base64').toString('utf-8').should.be.equal(data2)
                }

            })

            done()

        }, errorHandlerFunction)
    })

})
