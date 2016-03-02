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
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

/**
 * Benefits of aggregation: https://docs.aws.amazon.com/kinesis/latest/dev/kinesis-kpl-concepts.html

 TODO: What's the best way to do Lambda callbacks?  
 
 - Send callback every when we break 1MB?  How do we clear records?
 */
@NotThreadSafe
public class KplBatchAggregator extends KplAggregator
{
	private final List<KinesisAggRecord> completedRecords;
	private final List<KplAggregatorListener> listeners;
	
    public KplBatchAggregator(final String streamName)
	{
    	super(streamName);
    	
    	this.completedRecords = new LinkedList<>();
    	this.listeners = new LinkedList<>();
	}
	
    @Override
	public int getNumUserRecords()
	{
		int numRecords = 0;
		for(KinesisAggRecord record : this.completedRecords)
		{
			numRecords += record.getNumUserRecords();
		}
		
		numRecords += this.currentRecord.getNumUserRecords();
		
		return numRecords;
	}
	
    @Override
	public int getNumKinesisRecords()
	{
		int numRecords = this.completedRecords.size();
		
		//factor in the in-progress record (if there is one)
		if(this.currentRecord.getNumUserRecords() > 0)
		{
			numRecords++;
		}
		
		return numRecords;
	}
	
    @Override
	public long getSizeBytes()
	{
		int estimatedSizeBytes = 0;
		for(KinesisAggRecord record : this.completedRecords)
		{
			estimatedSizeBytes += record.getSizeBytes();
		}
		
		estimatedSizeBytes += this.currentRecord.getSizeBytes();
		
		return estimatedSizeBytes;
	}
	
    @Override
	public List<PutRecordRequest> extractPutRecordRequests()
	{
		List<PutRecordRequest> requests = new LinkedList<>();
		
		for(KinesisAggRecord record : this.completedRecords)
		{
			requests.add(new PutRecordRequest()
						 .withData(ByteBuffer.wrap(record.toRecordBytes()))
						 .withStreamName(this.streamName)
						 .withPartitionKey(record.getPartitionKey())
						 .withExplicitHashKey(record.getExplicitHashKey()));
		}
		
		if(this.currentRecord.getNumUserRecords() > 0)
		{
			requests.add(new PutRecordRequest()
						 .withData(ByteBuffer.wrap(this.currentRecord.toRecordBytes()))
						 .withStreamName(this.streamName)
						 .withPartitionKey(this.currentRecord.getPartitionKey())
						 .withExplicitHashKey(this.currentRecord.getExplicitHashKey()));
		}
		
		return requests;
	}
	
    @Override
	public List<PutRecordsRequest> extractPutRecordsRequests()
	{
		int currentRecords = 0;
		int currentBytes = 0;
		PutRecordsRequest currentRequest = new PutRecordsRequest()
										   .withStreamName(this.streamName);
		
		List<PutRecordsRequest> requests = new LinkedList<>();
		for(KinesisAggRecord record : this.completedRecords)
		{
			if(currentBytes + record.getSizeBytes() > KINESIS_MAX_BYTES_PER_PUT_RECORDS ||
			   currentRecords + 1 > KINESIS_MAX_RECORDS_PER_PUT_RECORDS)
			{
				requests.add(currentRequest);
				currentRequest = new PutRecordsRequest()
								 .withStreamName(this.streamName);
			}
			
			PutRecordsRequestEntry entry = new PutRecordsRequestEntry()
										   .withData(ByteBuffer.wrap(record.toRecordBytes()))
										   .withPartitionKey(record.getPartitionKey())
										   .withExplicitHashKey(record.getExplicitHashKey());
			currentRequest.getRecords().add(entry);
			
			currentBytes += record.getSizeBytes();
			currentRecords += 1;
		}
		
		if(currentRequest.getRecords().size() > 0)
		{
			requests.add(currentRequest);
		}
		
		return requests;
	}
	
    @Override
	public List<ByteBuffer> extractBytes()
	{
		List<ByteBuffer> aggRecords = new LinkedList<>();
		
		for(KinesisAggRecord record : this.completedRecords)
		{
			aggRecords.add(ByteBuffer.wrap(record.toRecordBytes()));
		}
		
		if(this.currentRecord.getNumUserRecords() > 0)
		{
			aggRecords.add(ByteBuffer.wrap(this.currentRecord.toRecordBytes()));
		}
		
		return aggRecords;
	}
	
    @Override
	public void clear()
    {
    	super.clear();
    	
		this.completedRecords.clear();
    }
	
    @Override
	public void addUserRecord(String partitionKey, byte[] data)
	{
        addUserRecord(partitionKey, null, data);
    }
	
    @Override
	public void addUserRecord(String partitionKey, String explicitHashKey, byte[] data)
	{
		boolean success = this.currentRecord.addUserRecord(partitionKey, explicitHashKey, data);
		if(success)
		{
			return;
		}
		
		this.completedRecords.add(this.currentRecord);
		for(KplAggregatorListener listener : this.listeners)
		{
			final String stream = this.streamName;
			listener.recordComplete(stream,
					this.currentRecord.getPartitionKey(),
					this.currentRecord.getExplicitHashKey(),
					ByteBuffer.wrap(this.currentRecord.toRecordBytes()));
		}
		this.currentRecord = new KinesisAggRecord();
    }
}
