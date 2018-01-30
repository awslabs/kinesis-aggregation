const assert = require('assert')
const sinon = require('sinon');
const should = require('should')
require('should-sinon')

const aggregate = require('../index').aggregate

const crypto = require("crypto")
const uuid = require('node-uuid')

const MAX_PAYLOAD = 1024 * 1024

const generateRandomRecord = (messageSize = 1024) => {
    const uid = uuid.v4()
    return {
        partitionKey: uid,
        explicitHashKey: uid,
        data: crypto.randomBytes(messageSize).toString('base64')
    }
}

const encodedRecordHandler = (encodedRecord, callback) => {
    callback(null, encodedRecord)
}

describe('#aggregate', () => {

    it('aggregate is a function', () => {
        aggregate.should.be.Function()
    })

    it('aggregate call afterPutAggregatedRecords when no encoded record are processed', (done) => {
        const encodedRecordHandlerFunction = sinon.spy()
        const afterPutAggregatedRecordsFunction = sinon.spy()
        const errorHandlerFunction = sinon.spy()

        encodedRecordHandlerFunction.should.have.callCount(0)
        afterPutAggregatedRecordsFunction.should.have.callCount(0)
        errorHandlerFunction.should.have.callCount(0)

        aggregate([], encodedRecordHandlerFunction, () => {
            encodedRecordHandlerFunction.should.have.callCount(0)
            // one error raised
            errorHandlerFunction.should.have.callCount(1)
            afterPutAggregatedRecordsFunction()
            errorHandlerFunction.should.have.callCount(1)
            afterPutAggregatedRecordsFunction.should.have.callCount(1)
            done()
        }, errorHandlerFunction)
    })

    it('aggregate should raise error when record is over 1Mo', (done) => {
        const encodedRecordHandlerFunction = sinon.spy(encodedRecordHandler)
        const errorHandlerFunction = sinon.spy()

        encodedRecordHandlerFunction.should.have.callCount(0)
        errorHandlerFunction.should.have.callCount(0)

        aggregate([generateRandomRecord(MAX_PAYLOAD + 42), generateRandomRecord()], encodedRecordHandlerFunction, () => {
            encodedRecordHandlerFunction.should.have.callCount(1)
            errorHandlerFunction.should.have.callCount(1)
            done()
        }, errorHandlerFunction)
    })


    it('aggregate call encodedRecordHandler on each encoded record', (done) => {
        const encodedRecordHandlerFunction = sinon.spy(encodedRecordHandler)
        const afterPutAggregatedRecordsFunction = sinon.spy()
        const errorHandlerFunction = sinon.spy()

        const MAX_RECORD = 1600
        const MESSAGE_SIZE = 1024;
        const records = []
        for (let i = 0; i < MAX_RECORD; i = i + 1) {
            records.push(generateRandomRecord(MESSAGE_SIZE))
        }

        encodedRecordHandlerFunction.should.have.callCount(0)
        afterPutAggregatedRecordsFunction.should.have.callCount(0)
        errorHandlerFunction.should.have.callCount(0)

        aggregate(records, encodedRecordHandlerFunction, () => {
            afterPutAggregatedRecordsFunction.should.have.callCount(0)
            encodedRecordHandlerFunction.should.have.callCount(Math.ceil(MAX_RECORD * MESSAGE_SIZE / MAX_PAYLOAD) + 1)
            errorHandlerFunction.should.have.callCount(0)
            done()
        }, errorHandlerFunction)
    })

    it('aggregate encoded records with max size of 1Mo', (done) => {
        const encodedRecordHandler = (encodedRecord, callback) => {
            Buffer.byteLength(encodedRecord.data, 'binary').should.belowOrEqual(MAX_PAYLOAD)
            callback(null, true)
        }
        const encodedRecordHandlerFunction = sinon.spy(encodedRecordHandler)
        const afterPutAggregatedRecordsFunction = sinon.spy()
        const errorHandlerFunction = sinon.spy()
        const MAX_RECORD = 1600
        const MESSAGE_SIZE = 1024;
        const records = []
        for (let i = 0; i < MAX_RECORD; i = i + 1) {
            records.push(generateRandomRecord(MESSAGE_SIZE))
        }

        encodedRecordHandlerFunction.should.have.callCount(0)
        afterPutAggregatedRecordsFunction.should.have.callCount(0)
        errorHandlerFunction.should.have.callCount(0)

        aggregate(records, encodedRecordHandlerFunction, () => {
            encodedRecordHandlerFunction.should.have.callCount(Math.ceil(MAX_RECORD * MESSAGE_SIZE / MAX_PAYLOAD) + 1)
            errorHandlerFunction.should.have.callCount(0)
            done()
        }, errorHandlerFunction)
    })

    it('aggregate call afterPutAggregatedRecords when on each encoded record are processed', (done) => {
        const MAX_RECORD = 1028
        const MESSAGE_SIZE = 1024;
        let count = 0
        const encodedRecordHandler = (encodedRecord, callback) => {
            setTimeout(() => {
                count = count + 1
                callback(null, true)
            }, 300)
        }
        const encodedRecordHandlerFunction = sinon.spy(encodedRecordHandler)
        const afterPutAggregatedRecordsFunction = sinon.spy()
        const errorHandlerFunction = sinon.spy()

        const records = []
        for (let i = 0; i < MAX_RECORD; i = i + 1) {
            records.push(generateRandomRecord(MESSAGE_SIZE))
        }

        aggregate(records, encodedRecordHandlerFunction, () => {
            errorHandlerFunction.should.have.callCount(0)
            count.should.be.equal(2)
            done()
        }, errorHandlerFunction)
    })

})