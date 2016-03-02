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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

public class KplProgressiveAggregator extends KplAggregator
{
	private final List<KplAggregatorListener> listeners;
	
    public KplProgressiveAggregator(final String streamName)
	{
    	super(streamName);
    	
    	this.listeners = new LinkedList<>();
	}
    
    public void addKplAggregatorListener(final KplAggregatorListener listener)
    {
    	if(!this.listeners.contains(listener))
    	{
    		this.listeners.add(listener);
    	}
    }
    
    public void removeKplAggregatorListener(final KplAggregatorListener listener)
    {
    	if(this.listeners.contains(listener))
    	{
    		this.listeners.remove(listener);
    	}
    }
	
	public List<PutRecordRequest> drainPutRecordRequests()
	{
		PutRecordRequest request =  new PutRecordRequest()
									 .withData(ByteBuffer.wrap(this.currentRecord.toRecordBytes()))
									 .withStreamName(this.streamName)
									 .withPartitionKey(this.currentRecord.getPartitionKey())
									 .withExplicitHashKey(this.currentRecord.getExplicitHashKey());
		
		clear();
		
		return Arrays.asList(request);
	}
	
	public List<PutRecordsRequest> drainPutRecordsRequests()
	{
		PutRecordsRequestEntry entry = new PutRecordsRequestEntry()
										   .withData(ByteBuffer.wrap(this.currentRecord.toRecordBytes()))
										   .withPartitionKey(this.currentRecord.getPartitionKey())
										   .withExplicitHashKey(this.currentRecord.getExplicitHashKey());

		PutRecordsRequest request = new PutRecordsRequest()
									   .withStreamName(this.streamName)
									   .withRecords(Arrays.asList(entry));
		
		clear();
		
		return Arrays.asList(request);
	}
	
	public List<ByteBuffer> drainBytes()
	{
		byte[] recordBytes = this.currentRecord.toRecordBytes();
		
		clear();
		
		return Arrays.asList(ByteBuffer.wrap(recordBytes));
	}
	
	public void addUserRecord(String partitionKey, String explicitHashKey, byte[] data)
	{
		boolean success = this.currentRecord.addUserRecord(partitionKey, explicitHashKey, data);
		if(success)
		{
			return;
		}
		
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
