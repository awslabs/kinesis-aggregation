/**
 * Kinesis Producer Library Aggregation/Deaggregation Examples for AWS Lambda/Java
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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

@NotThreadSafe
public class KplAggregator
{
	private KinesisAggRecord currentRecord;
	private List<Function<KinesisAggRecord,Void>> functionCallbacks;
	
    public KplAggregator()
	{
		this.currentRecord = new KinesisAggRecord();
		this.functionCallbacks = new LinkedList<>();
	}

	public int getNumUserRecords()
	{
		return this.currentRecord.getNumUserRecords();
	}
	
	public long getSizeBytes()
	{
		return this.currentRecord.getSizeBytes();
	}
	
	public void clear()
	{
		this.currentRecord = new KinesisAggRecord();
	}
	
	public void onRecordComplete(Function<KinesisAggRecord,Void> callback)
	{
		if(!this.functionCallbacks.contains(callback))
		{
			this.functionCallbacks.add(callback);
		}
	}
	
	public KinesisAggRecord addUserRecord(String partitionKey, byte[] data)
	{
	    return addUserRecord(partitionKey, null, data);
	}
    
    public KinesisAggRecord clearAndGet()
	{
    	if(getNumUserRecords() == 0)
    	{
    		return null;
    	}
    	
		KinesisAggRecord out = this.currentRecord;
		this.currentRecord = new KinesisAggRecord();
		return out;
	}
	
	public KinesisAggRecord addUserRecord(String partitionKey, String explicitHashKey, byte[] data)
	{
		boolean success = this.currentRecord.addUserRecord(partitionKey, explicitHashKey, data);
		if (success) 
		{
			// we were able to add the current data to the in-flight record
			return null;
		}
		
		final KinesisAggRecord completeRecord = this.currentRecord;
		for(Function<KinesisAggRecord,Void> callback : this.functionCallbacks)
		{
			CompletableFuture.runAsync(() -> { callback.apply(completeRecord); });
		}
		
		return clearAndGet();
	}
	
	public Void streamingAddUserRecord(Stream<PutRecordsRequestEntry> records, Consumer<KinesisAggRecord> consumer)
	{
		records.forEachOrdered(rec ->
		{
			final KinesisAggRecord o = addUserRecord(rec.getPartitionKey(), rec.getExplicitHashKey(), rec.getData().array());
			if (o != null)
			{
				Arrays.asList(o).stream().forEachOrdered(consumer);
			}
		});

		return null;
	}
}
