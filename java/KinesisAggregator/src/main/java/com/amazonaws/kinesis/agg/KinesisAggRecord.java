package com.amazonaws.kinesis.agg;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
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
import com.google.protobuf.ByteString;

@NotThreadSafe
public class KinesisAggRecord
{
	//Kinesis Limits
	//https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html
	public static final long KINESIS_MAX_BYTES_PER_RECORD = 1048576L; //1 MB (1024 * 1024)
	public static final int KINESIS_PARTITION_KEY_MIN_LENGTH = 1;
	public static final int KINESIS_PARTITION_KEY_MAX_LENGTH = 256;
	
	public static final byte[] KPL_AGGREGATED_RECORD_MAGIC = new byte[] {-13, -119, -102, -62 };
	public static final String KPL_MESSAGE_DIGEST_NAME = "MD5";
	
	private static final BigInteger UINT_128_MAX = new BigInteger(StringUtils.repeat("FF", 16), 16);
	
    private long estimatedProtobufSizeBytes;
    private final KeySet explicitHashKeys;
    private final KeySet partitionKeys;
    private AggregatedRecord.Builder aggregatedRecordBuilder;
    private final MessageDigest md5;
    private String aggPartitionKey;
    private String aggExplicitHashKey;
    
    public KinesisAggRecord()
	{
		this.aggregatedRecordBuilder = AggregatedRecord.newBuilder();
		this.estimatedProtobufSizeBytes = 0;
		this.explicitHashKeys = new KeySet();
		this.partitionKeys = new KeySet();
		
		try 
		{
			this.md5 = MessageDigest.getInstance(KPL_MESSAGE_DIGEST_NAME);
		}
		catch (NoSuchAlgorithmException e)
		{
			throw new IllegalStateException("Could not create an MD5 message digest.",e);
		}
	}
	
	public int getNumUserRecords()
	{
		return this.aggregatedRecordBuilder.getRecordsCount();
	}
	
	public long getSizeBytes()
	{
		if(getNumUserRecords() == 0)
		{
			return 0;
		}
		else if(getNumUserRecords() == 1)
		{
			return this.aggregatedRecordBuilder.getRecordsList().get(0).getData().size();
		}
		
		return KPL_AGGREGATED_RECORD_MAGIC.length + this.estimatedProtobufSizeBytes + this.md5.getDigestLength();
	}
	
	public byte[] toRecordBytes()
	{
		if(getNumUserRecords() == 0)
		{
			return new byte[0];
		}
		//If there's only one record in here, don't bother adding the KPL magic/digest overhead...aggregation buys us nothing
		else if(getNumUserRecords() == 1)
		{
			return this.aggregatedRecordBuilder.getRecordsList().get(0).getData().toByteArray();
		}
		
		byte[] messageBody = this.aggregatedRecordBuilder.build().toByteArray();
		
		this.md5.reset();
		byte[] messageDigest = this.md5.digest(messageBody);
		
		//The way Java's API works is that write(byte[]) throws IOException on a ByteArrayOutputStream, but write(byte[],int,int) doesn't
		ByteArrayOutputStream baos = new ByteArrayOutputStream(KPL_AGGREGATED_RECORD_MAGIC.length + messageBody.length + this.md5.getDigestLength());
		baos.write(KPL_AGGREGATED_RECORD_MAGIC, 0, KPL_AGGREGATED_RECORD_MAGIC.length);
		baos.write(messageBody, 0, messageBody.length);
		baos.write(messageDigest, 0, messageDigest.length);
		
		return baos.toByteArray();
	}
	
    public void clear()
    {
    	this.md5.reset();
    	this.estimatedProtobufSizeBytes = 0;
    	this.explicitHashKeys.clear();
    	this.partitionKeys.clear();
    	this.aggregatedRecordBuilder = AggregatedRecord.newBuilder();
    }
    
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
	    // single byte to save space.
	    return "a";
    }
    
    public String getExplicitHashKey()
    {
    	if (getNumUserRecords() == 0)
		{
			throw new IllegalStateException("Cannot compute explicitHashKey for empty container");
		}
    	
    	return this.aggExplicitHashKey;
    }
    
    private int getEstimatedRecordSize(String partitionKey, String explicitHashKey, byte[] data)
    {
    	int estimatedSize = 0;
    	
    	estimatedSize += data.length + 3;
    	
    	if(!this.partitionKeys.contains(partitionKey))
        {
        	estimatedSize += partitionKey.length() + 3;
        }
        estimatedSize += 2;
        
        if(!this.explicitHashKeys.contains(explicitHashKey))
    	{
    		estimatedSize += explicitHashKey.length() + 3;
    	}
    	estimatedSize += 2;
        
        return estimatedSize;
    }
	
    /** Return false if record would overflow max size, return true otherwise; */
	public boolean addUserRecord(String partitionKey, String explicitHashKey, byte[] data)
	{
		validatePartitionKey(partitionKey);
		partitionKey = partitionKey.trim();
        
        explicitHashKey = explicitHashKey != null ? explicitHashKey.trim() : createExplicitHashKey(partitionKey);
        validateExplicitHashKey(explicitHashKey);
        
        validateData(data);       
        
      //Validate new record size won't overflow
        int estimatedSizeOfNewRecord = getEstimatedRecordSize(partitionKey, explicitHashKey, data);
        if(getSizeBytes() + estimatedSizeOfNewRecord > KINESIS_MAX_BYTES_PER_RECORD)
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
        
        this.estimatedProtobufSizeBytes += estimatedSizeOfNewRecord;
        this.aggregatedRecordBuilder.addRecords(newRecord.build());
        
        if(this.aggregatedRecordBuilder.getRecordsCount() == 1)
        {
        	this.aggPartitionKey = partitionKey;
        	this.aggExplicitHashKey = explicitHashKey;
        }
        
        return true;
    }
	
	private void validateData(final byte[] data)
	{
		if (data != null && data.length > KINESIS_MAX_BYTES_PER_RECORD)
        {
            throw new IllegalArgumentException("Data must be less than or equal to " + 
            								   + KINESIS_MAX_BYTES_PER_RECORD
            								   + " bytes in size, got " + data.length + " bytes");
        }
	}
	
	private void validatePartitionKey(final String partitionKey)
	{
		if (partitionKey == null) 
        {
            throw new IllegalArgumentException("Partition key cannot be null");
        }
        
        if (partitionKey.length() < KINESIS_PARTITION_KEY_MIN_LENGTH || partitionKey.length() > KINESIS_PARTITION_KEY_MAX_LENGTH) 
        {
            throw new IllegalArgumentException("Invalid parition key. Length must be at least "
            		+ KINESIS_PARTITION_KEY_MIN_LENGTH + " and at most "
            		+ KINESIS_PARTITION_KEY_MAX_LENGTH + ", got " + partitionKey.length());
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
			  return this.lookup.containsKey(s);
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
