/**
 * Kinesis Producer Library Deaggregation Examples for AWS Lambda/Java
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.kinesis.agg;

import java.nio.ByteBuffer;
import java.util.List;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;

@NotThreadSafe
public abstract class KplAggregator
{
	//Kinesis Limits
	//https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
	public static final long KINESIS_MAX_RECORDS_PER_PUT_RECORDS = 500;
	public static final long KINESIS_MAX_BYTES_PER_PUT_RECORDS = 5 * 1048576L; //5 MB
	
	protected KinesisAggRecord currentRecord;
	protected final String streamName;

	public KplAggregator(String streamName) 
	{
		this.streamName = streamName;
		this.currentRecord = new KinesisAggRecord();
	}

	public String getStreamName()
	{
		return this.streamName;
	}

	public int getNumUserRecords()
	{
		return this.currentRecord.getNumUserRecords();
	}
	
	public long getSizeBytes()
	{
		return this.currentRecord.getSizeBytes();
	}
	
	public int getNumKinesisRecords()
	{
		return 1;
	}
	
	public void clear()
	{
		this.currentRecord = new KinesisAggRecord();
	}
	
	public abstract List<PutRecordRequest> extractPutRecordRequests();
	
	public abstract List<PutRecordsRequest> extractPutRecordsRequests();

	public abstract List<ByteBuffer> extractBytes();
	
	public void addUserRecord(String partitionKey, byte[] data)
	{
	    addUserRecord(partitionKey, null, data);
	}

	public abstract void addUserRecord(String partitionKey, String explicitHashKey, byte[] data);
}