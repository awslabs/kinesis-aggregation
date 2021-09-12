// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package deaggregator_test

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	deagg "github.com/awslabs/kinesis-aggregation/go/deaggregator"
	rec "github.com/awslabs/kinesis-aggregation/go/records"	
)

// Generate an aggregate record in the correct AWS-specified format
// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
func generateAggregateRecord(numRecords int) []byte { 

	aggr := &rec.AggregatedRecord{}
	// Start with the magic header
	aggRecord := []byte("\xf3\x89\x9a\xc2")
	partKeyTable := make([]string, 0)

	// Create proto record with numRecords length
	for i := 0; i < numRecords; i++ {
		var partKey uint64
		var hashKey uint64
		partKey = uint64(i)
		hashKey = uint64(i) * uint64(10)
		r := &rec.Record{
			PartitionKeyIndex: &partKey,
			ExplicitHashKeyIndex: &hashKey,
			Data: []byte("Some test data string"),
			Tags: make([]*rec.Tag, 0),
		}

		aggr.Records = append(aggr.Records, r)
		partKeyVal := "test" + fmt.Sprint(i)
		partKeyTable = append(partKeyTable, partKeyVal)
	}

	aggr.PartitionKeyTable = partKeyTable
	// Marshal to protobuf record, create md5 sum from proto record
	// and append both to aggRecord with magic header
	data, _ := proto.Marshal(aggr)
	md5Hash := md5.Sum(data)
	aggRecord = append(aggRecord, data...)
	aggRecord = append(aggRecord, md5Hash[:]...)
	return aggRecord
}

// Generate a generic kinesis.Record using whatever []byte
// is passed in as the data (can be normal []byte or proto record)
func generateKinesisRecord(data []byte) *kinesis.Record {
	currentTime := time.Now()
	encryptionType := "NONE"
	partitionKey := "1234"
	sequenceNumber := "21269319989900637946712965403778482371"
	return &kinesis.Record{
		ApproximateArrivalTimestamp: &currentTime,
		Data: data,
		EncryptionType: &encryptionType,
		PartitionKey: &partitionKey,
		SequenceNumber: &sequenceNumber,
	}
}

// This tests to make sure that the data is at least larger than the length
// of the magic header to do some array slicing with index out of bounds
func TestSmallLengthReturnsCorrectNumberOfDeaggregatedRecords(t *testing.T) {
	var err error
	var kr *kinesis.Record

	krs := make([]*kinesis.Record, 0)
	dars := make([]*kinesis.Record, 0)
	
	smallByte := []byte("No")
	kr = generateKinesisRecord(smallByte)
	krs = append(krs, kr)
	dars, err = deagg.DeaggregateRecords(krs)
	if err != nil {
		panic(err)
	}
	
	// Small byte test, since this is not a deaggregated record, should return 1
	// record in the array.
	assert.Equal(t, 1, len(dars), "Small Byte test should return length of 1.")
}

// This function tests to make sure that the data starts with the correct magic header
// according to KPL aggregate documentation.
func TestNonMatchingMagicHeaderReturnsSingleRecord(t *testing.T) {
	var err error
	var kr *kinesis.Record

	krs := make([]*kinesis.Record, 0)
	dars := make([]*kinesis.Record, 0)

	min := 1
	max := 10
	n := rand.Intn(max - min) + min
	aggData := generateAggregateRecord(n)
	mismatchAggData := aggData[1:]
	kr = generateKinesisRecord(mismatchAggData)

	krs = append(krs, kr)

	dars, err = deagg.DeaggregateRecords(krs)
	if err != nil {
		panic(err)
	}

	// A byte record with a magic header that does not match 0xF3 0x89 0x9A 0xC2
	// should return a single record.
	assert.Equal(t, 1, len(dars), "Mismatch magic header test should return length of 1.")
}

// This function tests that the DeaggregateRecords function returns the correct number of
// deaggregated records from a single aggregated record.
func TestVariableLengthRecordsReturnsCorrectNumberOfDeaggregatedRecords(t *testing.T) {
	var err error
	var kr *kinesis.Record

	krs := make([]*kinesis.Record, 0)
	dars := make([]*kinesis.Record, 0)

	min := 1
	max := 10
	n := rand.Intn(max - min) + min
	aggData := generateAggregateRecord(n)
	kr = generateKinesisRecord(aggData)
	krs = append(krs, kr)

	dars, err = deagg.DeaggregateRecords(krs)
	if err != nil {
		panic(err)
	}

	// Variable Length Aggregate Record test has aggregaterd records and should return
	// n length.
	assertMsg := fmt.Sprintf("Variable Length Aggregate Record should return length %v.", len(dars))
	assert.Equal(t, n, len(dars), assertMsg)
}

// This function tests the length of the message after magic file header. If length is less than
// the digest size (16 bytes), it is not an aggregated record.
func TestRecordAfterMagicHeaderWithLengthLessThanDigestSizeReturnsSingleRecord(t *testing.T) {
	var err error
	var kr *kinesis.Record

	krs := make([]*kinesis.Record, 0)
	dars := make([]*kinesis.Record, 0)

	min := 1
	max := 10
	n := rand.Intn(max - min) + min
	aggData := generateAggregateRecord(n)
	// Change size of proto message to 15
	reducedAggData := aggData[:19]
	kr = generateKinesisRecord(reducedAggData)

	krs = append(krs, kr)

	dars, err = deagg.DeaggregateRecords(krs)
	if err != nil {
		panic(err)
	}

	// A byte record with length less than 16 after the magic header should return
	// a single record from DeaggregateRecords
	assert.Equal(t, 1, len(dars), "Digest size test should return length of 1.")
}

// This function tests the MD5 Sum at the end of the record by comparing MD5 sum
// at end of proto record with MD5 Sum of Proto message. If they do not match,
// it is not an aggregated record.
func TestRecordWithMismatchMd5SumReturnsSingleRecord(t *testing.T) {
	var err error
	var kr *kinesis.Record

	krs := make([]*kinesis.Record, 0)
	dars := make([]*kinesis.Record, 0)

	min := 1
	max := 10
	n := rand.Intn(max - min) + min
	aggData := generateAggregateRecord(n)
	// Remove last byte from array to mismatch the MD5 sums
	mismatchAggData := aggData[:len(aggData)-1]
	kr = generateKinesisRecord(mismatchAggData)

	krs = append(krs, kr)

	dars, err = deagg.DeaggregateRecords(krs)
	if err != nil {
		panic(err)
	}

	// A byte record with an MD5 sum that does not match with the md5.Sum(record)
	// will be marked as a non-aggregate record and return a single record
	assert.Equal(t, 1, len(dars), "Mismatch md5 sum test should return length of 1.")
}
