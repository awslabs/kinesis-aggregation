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

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.kinesis.clientlibrary.types.Messages.AggregatedRecord;
import com.amazonaws.services.kinesis.clientlibrary.types.Messages.Record;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.protobuf.ByteString;

/**
 * 
 * Represents a single aggregated Kinesis record.  This Kinesis record is built by adding multiple user
 * records and then serializing them to bytes using the Kinesis Producer Library (KPL) serialization
 * protocol.  This class lifts heavily from the existing KPL C++ libraries found at 
 * https://github.com/awslabs/amazon-kinesis-producer.
 *
 * This class is NOT thread-safe.
 *
 */
@NotThreadSafe
public class KinesisAggRecord
{
	//Serialization protocol constants from https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	private static final byte[] KPL_AGGREGATED_RECORD_MAGIC = new byte[] {(byte)0xf3, (byte)0x89, (byte)0x9a, (byte)0xc2 };
	private static final String KPL_MESSAGE_DIGEST_NAME = "MD5";
	private static final BigInteger UINT_128_MAX = new BigInteger(StringUtils.repeat("FF", 16), 16);
	
	/** The current size of the serialized protocol buffer message. */
    private long protobufSizeBytes;
    /** The set of unique explicit hash keys in the protocol buffer message. */
    private final KeySet explicitHashKeys;
    /** The set of unique partition keys in the protocol buffer message. */
    private final KeySet partitionKeys;
    /** The current builder object by which we're actively constructing a record. */ 
    private AggregatedRecord.Builder aggregatedRecordBuilder;
    /** The message digest to use for calculating MD5 checksums per the protocol specification. */
    private final MessageDigest md5;
    /** The partition key for the entire aggregated record. */
    private String aggPartitionKey;
    /** The explicit hash key for the entire aggregated record. */
    private String aggExplicitHashKey;
    
    /**
     * Construct a new (empty) aggregated Kinesis record.
     */
    public KinesisAggRecord()
	{
		this.aggregatedRecordBuilder = AggregatedRecord.newBuilder();
		this.protobufSizeBytes = 0;
		this.explicitHashKeys = new KeySet();
		this.partitionKeys = new KeySet();
		
		this.aggExplicitHashKey = "";
		this.aggPartitionKey = "";
		
		try 
		{
			this.md5 = MessageDigest.getInstance(KPL_MESSAGE_DIGEST_NAME);
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new IllegalStateException("Could not create an MD5 message digest.", e);
		}
	}
	
    /**
     * Get the current number of user records contained inside this aggregate record.
     * 
     * @return The current number of user records added via the "addUserRecord(...)" method.
     */
	public int getNumUserRecords()
	{
		return this.aggregatedRecordBuilder.getRecordsCount();
	}
	
	/**
	 * Get the current size in bytes of the fully serialized aggregated record.
	 * 
	 * @return The current size in bytes of this message in its serialized form.
	 */
	public long getSizeBytes()
	{
		if(getNumUserRecords() == 0)
		{
			return 0;
		}
		
		return KPL_AGGREGATED_RECORD_MAGIC.length + this.protobufSizeBytes + this.md5.getDigestLength();
	}
	
	/**
	 * Serialize this record to bytes.
	 * 
	 * No side effects (i.e. does not affect the contents of this record object).
	 * 
	 * @return A byte array containing a KPL protocol compatible Kinesis record.
	 */
	public byte[] toRecordBytes()
	{
		if(getNumUserRecords() == 0)
		{
			return new byte[0];
		}
		
		byte[] messageBody = this.aggregatedRecordBuilder.build().toByteArray();
		
		this.md5.reset();
		byte[] messageDigest = this.md5.digest(messageBody);
		
		//The way Java's API works is that write(byte[]) throws IOException on a ByteArrayOutputStream, but write(byte[],int,int) doesn't
		//so that's why we're using the long version of "write" here
		ByteArrayOutputStream baos = new ByteArrayOutputStream(KPL_AGGREGATED_RECORD_MAGIC.length + messageBody.length + this.md5.getDigestLength());
		baos.write(KPL_AGGREGATED_RECORD_MAGIC, 0, KPL_AGGREGATED_RECORD_MAGIC.length);
		baos.write(messageBody, 0, messageBody.length);
		baos.write(messageDigest, 0, messageDigest.length);
		
		return baos.toByteArray();
	}
	
	/**
	 * Clears out all records and metadata from this object so that it can be reused
	 * just like a fresh instance of this object.
	 */
    public void clear()
    {
    	this.md5.reset();
    	this.aggExplicitHashKey = "";
		this.aggPartitionKey = "";
		this.protobufSizeBytes = 0;
    	this.explicitHashKeys.clear();
    	this.partitionKeys.clear();
    	this.aggregatedRecordBuilder = AggregatedRecord.newBuilder();
    }
    
    /**
     * Get the overarching partition key for the entire aggregated record.
     * 
     * @return Actually just returns a constant "a" because aggregated records
     * always set an explicit hash key.
     */
    public String getPartitionKey()
    {
		if (getNumUserRecords() == 0)
		{
			throw new IllegalStateException("Cannot compute partitionKey for empty container");
		}
		else if(getNumUserRecords() == 1)
		{
			return this.aggPartitionKey;
		}
		
	    // We will always set an explicit hash key if we created an aggregated record.
	    // We therefore have no need to set a partition key since the records within
	    // the container have their own parition keys anyway. We will therefore use a
	    // single byte to save space. (comment via original KPL)
	    return "a";
    }
    
    /**
     * Get the overarching explicit hash key for the entire aggregated record.
     * 
     * @return Returns an explicit hash key to use for this record when transmitting
     * it to Kinesis.
     */
    public String getExplicitHashKey()
    {
    	if (getNumUserRecords() == 0)
		{
			throw new IllegalStateException("Cannot compute explicitHashKey for empty container");
		}
    	
    	return this.aggExplicitHashKey;
    }
    
    /**
     * Based on the current size of this aggregated record, calculate what the new size would be
     * if we added another user record with the specified parameters.  Used to determine when
     * this aggregated record is full and can't accept any more user records.
     * 
     * @param partitionKey The partition key of the new record to simulate adding
     * @param explicitHashKey The explicit hash key of the new record to simulate adding
     * @param data The raw data of the new record to simulate adding
     * 
     * @return The new size of this existing record in bytes if a new user record with the specified
     * parameters was added.
     */
    private int calculateNewProtobufSize(String partitionKey, String explicitHashKey, byte[] data)
    {
    	int messageSize = 0;
    	
    	if(!this.partitionKeys.contains(partitionKey))
		{
    		int pkLength = partitionKey.length();
    		messageSize += 1; //(message index + wire type for PK table)
    		messageSize += calculateVarintSize(pkLength);
			messageSize += pkLength;
		}
    	
    	if(!this.explicitHashKeys.contains(explicitHashKey))
    	{
    		int ehkLength = explicitHashKey.length();
    		messageSize += 1; //(message index + wire type for EHK table)
    		messageSize += calculateVarintSize(ehkLength);
			messageSize += ehkLength;
    	}
    	
    	long innerRecordSize = 0;
    	
    	if(partitionKey != null)
		{
    		innerRecordSize += 1; //(message index + wire type for PK index)
    		innerRecordSize += calculateVarintSize(this.partitionKeys.getPotentialIndex(partitionKey));
		}
			
		if(explicitHashKey != null)
		{
			innerRecordSize += 1; //(message index + wire type for EHK index)
			innerRecordSize += calculateVarintSize(this.explicitHashKeys.getPotentialIndex(explicitHashKey));
		}
			
		if(data != null)
		{
			innerRecordSize += 1; //(message index + wire type for record data)
			innerRecordSize += calculateVarintSize(data.length);
			innerRecordSize += data.length;
		}
		
		messageSize += 1; //(message index + wire type for record)
		messageSize += calculateVarintSize(innerRecordSize);
		messageSize += innerRecordSize;
		
        return messageSize;
    }
    
    /**
     * 
     * @param value
     * @return
     * 
     * @see https://developers.google.com/protocol-buffers/docs/encoding#varints
     */
    private int calculateVarintSize(long value)
    {
    	if(value < 0)
    	{
    		throw new IllegalArgumentException("Size values should not be negative.");
    	}
    	
    	int numBitsNeeded = 0;
    	if(value == 0)
    	{
    		numBitsNeeded = 1;
    	}
    	else
    	{
    		while (value > 0)
	    	{
	    	    numBitsNeeded++;
	    	    value = value >> 1;
	    	}
    	}
    	
    	int numVarintBytes = numBitsNeeded / 7;
    	if(numBitsNeeded % 7 > 0)
    	{
    		numVarintBytes += 1;
    	}
    	
    	return numVarintBytes;
    }
	
    /**
	 * Add a new user record to this existing aggregated record if there is enough space
	 * (based on the defined Kinesis limits for a PutRecord call).
	 * 
	 * @param partitionKey The partition key of the new user record to add
	 * @param explicitHashKey The explicit hash key of the new user record to add
	 * @param data The raw data of the new user record to add
	 * 
	 * @return True if the new user record was successfully added to this aggregated record
	 * or false if this aggregated record is too full.
	 */
    public boolean addUserRecord(String partitionKey, String explicitHashKey, byte[] data)
	{
    	validatePartitionKey(partitionKey);
		partitionKey = partitionKey.trim();
        
        explicitHashKey = explicitHashKey != null ? explicitHashKey.trim() : createExplicitHashKey(partitionKey);
        validateExplicitHashKey(explicitHashKey);
        
        validateData(data);       
        
        //Validate new record size won't overflow
        int sizeOfNewRecord = calculateNewProtobufSize(partitionKey, explicitHashKey, data);
        if(getSizeBytes() + sizeOfNewRecord > KinesisLimits.MAX_BYTES_PER_RECORD)
		{
        	return false;									
		}
        
        Record.Builder newRecord = Record.newBuilder()
				        				.setData(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY);
        
        ImmutablePair<Boolean,Long> pkAddResult = this.partitionKeys.add(partitionKey);
        if(pkAddResult.getFirst().booleanValue())
        {
        	this.aggregatedRecordBuilder.addPartitionKeyTable(partitionKey);
        }
        newRecord.setPartitionKeyIndex(pkAddResult.getSecond());
        
        ImmutablePair<Boolean, Long> ehkAddResult = this.explicitHashKeys.add(explicitHashKey);
    	if(ehkAddResult.getFirst().booleanValue())
    	{
    		this.aggregatedRecordBuilder.addExplicitHashKeyTable(explicitHashKey);
    	}
    	newRecord.setExplicitHashKeyIndex(ehkAddResult.getSecond());
        
        this.protobufSizeBytes += sizeOfNewRecord;
        this.aggregatedRecordBuilder.addRecords(newRecord.build());
        
        if(this.aggregatedRecordBuilder.getRecordsCount() == 1)
        {
        	this.aggPartitionKey = partitionKey;
        	this.aggExplicitHashKey = explicitHashKey;
        }
        
        return true;
    }
    
    public PutRecordRequest toPutRecordRequest(String streamName)
    {
    	byte[] recordBytes = toRecordBytes();
    	ByteBuffer bb = ByteBuffer.wrap(recordBytes);
    	return new PutRecordRequest()
    			.withStreamName(streamName)
    			.withExplicitHashKey(getExplicitHashKey())
    			.withPartitionKey(getPartitionKey())
    			.withData(bb);
    }
    
    public PutRecordsRequestEntry toPutRecordsRequestEntry()
    {
    	return new PutRecordsRequestEntry()
    				.withExplicitHashKey(getExplicitHashKey())
    				.withPartitionKey(getPartitionKey())
    				.withData(ByteBuffer.wrap(toRecordBytes()));
    }
	
	private void validateData(final byte[] data)
	{
		if (data != null && data.length > KinesisLimits.MAX_BYTES_PER_RECORD)
        {
            throw new IllegalArgumentException("Data must be less than or equal to " + 
            								   + KinesisLimits.MAX_BYTES_PER_RECORD
            								   + " bytes in size, got " + data.length + " bytes");
        }
	}
	
	private void validatePartitionKey(final String partitionKey)
	{
		if (partitionKey == null) 
        {
            throw new IllegalArgumentException("Partition key cannot be null");
        }
        
        if (partitionKey.length() < KinesisLimits.PARTITION_KEY_MIN_LENGTH || 
        	partitionKey.length() > KinesisLimits.PARTITION_KEY_MAX_LENGTH) 
        {
            throw new IllegalArgumentException("Invalid parition key. Length must be at least "
            		+ KinesisLimits.PARTITION_KEY_MIN_LENGTH + " and at most "
            		+ KinesisLimits.PARTITION_KEY_MAX_LENGTH + ", got " + partitionKey.length());
        }
        
        try 
        {
            partitionKey.getBytes(StandardCharsets.UTF_8);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("Partition key must be valid " + StandardCharsets.UTF_8.displayName());
        }
	}
	
	private void validateExplicitHashKey(final String explicitHashKey)
	{
		if(explicitHashKey == null)
		{
			return;
		}
		
		BigInteger b = null;
        try 
        {
            b = new BigInteger(explicitHashKey);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid explicitHashKey, must be an integer, got " + explicitHashKey);
        }
        
        if (b != null)
        {
            if (b.compareTo(UINT_128_MAX) > 0 || b.compareTo(BigInteger.ZERO) < 0)
            {
                throw new IllegalArgumentException("Invalid explicitHashKey, must be greater or equal to zero and less than or equal to (2^128 - 1), got " + explicitHashKey);
            }
        }
	}
	
	private String createExplicitHashKey(final String partitionKey)
	{
		ByteArrayOutputStream ehkStream = new ByteArrayOutputStream();
		
    	BigInteger hashKey = BigInteger.ZERO;
    	
    	this.md5.reset();
    	byte[] pkDigest = this.md5.digest(partitionKey.getBytes(StandardCharsets.UTF_8));
    	
    	for(int i=0; i < this.md5.getDigestLength(); i++)
    	{
    		BigInteger p = new BigInteger(Byte.toString(pkDigest[i]));
    		p.shiftLeft((16-i-1)*8);
    		hashKey.add(p);
    	}
    	
    	return new String(ehkStream.toByteArray(), StandardCharsets.UTF_8);
	}
	
	private class KeySet
	{
		  private List<String> keys;
		  private Map<String,Long> lookup;
		  private Map<String,Long> counts;
	
		  public KeySet()
		  {
			  this.keys = new LinkedList<>();
			  this.lookup = new TreeMap<>();
			  this.counts = new TreeMap<>();
		  }
		  
		  public Long getPotentialIndex(String s)
		  {
			  Long it = this.lookup.get(s);
			  if(it != null)
			  {
				  return it; 
			  }
			  
			  return Long.valueOf(this.keys.size());
		  }
		  
		  public ImmutablePair<Boolean, Long> add(String s)
		  {
			  Long it = this.lookup.get(s);
			  if(it != null)
			  {
				  this.counts.put(s, counts.get(s) + 1);
				  return new ImmutablePair<>(false, it);
			  }
			  
			  if(!this.lookup.containsKey(s))
			  {
				  this.lookup.put(s, Long.valueOf(this.keys.size()));
			  }
			  
			  if(!this.counts.containsKey(s))
			  {
				  this.counts.put(s, Long.valueOf(1));
			  }
			  
			  this.keys.add(s);
			  return new ImmutablePair<>(true, Long.valueOf(this.keys.size()-1));
		  }
		  
		  public boolean contains(String s)
		  {
			  return s != null && this.lookup.containsKey(s);
		  }
		  
		  public void clear()
		  {
			  this.keys.clear();
			  this.lookup.clear();
			  this.counts.clear();
		  }
	};
	
	private class ImmutablePair<U,V>
	{
		private U first;
		private V second;
		
		public ImmutablePair(U first, V second)
		{
			this.first = first;
			this.second = second;
		}
		
		public U getFirst()
		{
			return this.first;
		}
		
		public V getSecond()
		{
			return this.second;
		}
	}
}
