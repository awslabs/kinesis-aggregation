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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import com.amazonaws.annotation.NotThreadSafe;

@NotThreadSafe
public class KplAggregator
{
	public interface RecordCompleteListener 
	{
		public abstract void recordComplete(KinesisAggRecord aggRecord);
	}
	
	private KinesisAggRecord currentRecord;
	private List<ListenerExecutorPair> listeners;
	
    public KplAggregator()
	{
		this.currentRecord = new KinesisAggRecord();
		this.listeners = new LinkedList<>();
	}

	public int getNumUserRecords()
	{
		return this.currentRecord.getNumUserRecords();
	}
	
	public long getSizeBytes()
	{
		return this.currentRecord.getSizeBytes();
	}
	
	public void clearRecord()
	{
		this.currentRecord = new KinesisAggRecord();
	}
	
	public void clearListeners()
	{
		this.listeners.clear();
	}
	
	public void onRecordComplete(RecordCompleteListener listener)
	{
		onRecordComplete(listener, ForkJoinPool.commonPool());
	}
	
	public void onRecordComplete(RecordCompleteListener listener, Executor executor)
	{
		this.listeners.add(new ListenerExecutorPair(listener, executor));
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
		for(ListenerExecutorPair pair : this.listeners)
		{
			pair.getExecutor().execute(() -> { pair.getListener().recordComplete(completeRecord); });
		}
		
		return clearAndGet();
	}
	
	private class ListenerExecutorPair
	{
		private RecordCompleteListener listener;
		private Executor executor;
		
		public ListenerExecutorPair(RecordCompleteListener listener, Executor executor)
		{
			this.listener = listener;
			this.executor = executor;
		}
		
		public RecordCompleteListener getListener()
		{
			return this.listener;
		}
		
		public Executor getExecutor()
		{
			return this.executor;
		}
	}
}
