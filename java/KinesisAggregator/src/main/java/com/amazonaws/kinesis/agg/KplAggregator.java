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
 */
@NotThreadSafe
public class KplAggregator
{
	//Kinesis Limits
	//https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
	public static final long KINESIS_MAX_RECORDS_PER_PUT_RECORDS = 500;
	public static final long KINESIS_MAX_BYTES_PER_PUT_RECORDS = 5 * 1048576L; //5 MB
	
	private final List<KinesisAggRecord> completedRecords;
	private KinesisAggRecord currentRecord;
	private final List<KplAggregatorListener> listeners;
    private final String streamName;
	
    public KplAggregator(final String streamName)
	{
    	this.streamName = streamName;
    	this.currentRecord = new KinesisAggRecord();
    	this.completedRecords = new LinkedList<>();
    	this.listeners = new LinkedList<>();
	}
    
    public String getStreamName()
    {
    	return this.streamName;
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
	
	public List<PutRecordRequest> generatePutRecordRequests()
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
	
	public List<PutRecordsRequest> generatePutRecordsRequests()
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
	
	public List<ByteBuffer> generateRecordBytes()
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
	
    public void clear()
    {
    	this.currentRecord = new KinesisAggRecord();
		this.completedRecords.clear();
    }
	
    public void addUserRecord(String partitionKey, byte[] data)
	{
        addUserRecord(partitionKey, null, data);
    }
	
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
			listener.recordAvailable(ByteBuffer.wrap(this.currentRecord.toRecordBytes()));
		}
		this.currentRecord = new KinesisAggRecord();
    }
}
