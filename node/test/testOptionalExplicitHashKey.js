// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
const assert = require('assert')
const sinon = require('sinon');
const should = require('should')
require('should-sinon')

const aggregate = require('../index').aggregate

const crypto = require("crypto")
const uuid = require('node-uuid')

const generateUserRecordsWithoutEHKs = function() {
    var records = []
    for (var i = 0; i < 100; i++) {
        var u = uuid.v4()
        var record = {
            partitionKey : u,
            // random payload
            data : Buffer.from(crypto.randomBytes(100).toString('base64'))
        };
    
        records.push(record)
    }
    return records
};

const generateUserRecordsWithEHKs = function() {
    return generateUserRecordsWithoutEHKs()
        .map(function(record, index) {
            record.explicitHashKey = index;
            return record
        });
};

describe('#aggregate optional ExplicitHashKey', () => {

    it('aggregate without ExplicitHashKey should not add an ExplicitHashKey', (done) => {
        const encodedRecordHandler = (encodedRecord, callback) => {
            should.not.exist(encodedRecord['ExplicitHashKey'])
            callback(null, true)
        }

        const encodedRecordHandlerFunction = sinon.spy(encodedRecordHandler)
        const errorHandlerFunction = sinon.spy()

        encodedRecordHandlerFunction.should.have.callCount(0)
        errorHandlerFunction.should.have.callCount(0)

        aggregate(generateUserRecordsWithoutEHKs(), encodedRecordHandlerFunction, () => {
            encodedRecordHandlerFunction.should.have.callCount(1)
            errorHandlerFunction.should.have.callCount(0)
            done()
        }, errorHandlerFunction)
    })

    it('aggregate with ExplicitHashKey should have an ExplicitHashKey', (done) => {
        const encodedRecordHandler = (encodedRecord, callback) => {
            should.exist(encodedRecord['ExplicitHashKey'])
            callback(null, true)
        }

        const encodedRecordHandlerFunction = sinon.spy(encodedRecordHandler)
        const errorHandlerFunction = sinon.spy()

        encodedRecordHandlerFunction.should.have.callCount(0)
        errorHandlerFunction.should.have.callCount(0)

        aggregate(generateUserRecordsWithEHKs(), encodedRecordHandlerFunction, () => {
            encodedRecordHandlerFunction.should.have.callCount(1)
            errorHandlerFunction.should.have.callCount(0)
            done()
        }, errorHandlerFunction)
    })

})
