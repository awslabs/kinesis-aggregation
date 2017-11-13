package com.amazonaws.kinesis.agg;

import java.nio.charset.StandardCharsets;

import org.junit.Assert;
import org.junit.Test;

public class RecordAggregatorTest
{
    protected final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
    protected final String REVERSE_ALPHABET = "zyxwvutsrqponmlkjihgfedcba";
    
    @Test
    public void testSingleUserRecord()
    {
        RecordAggregator aggregator = new RecordAggregator();
        
        Assert.assertEquals(0, aggregator.getNumUserRecords());
        
        aggregator.addUserRecord("partition_key", ALPHABET.getBytes(StandardCharsets.UTF_8));        
        Assert.assertEquals(1, aggregator.getNumUserRecords());
        
        AggRecord record = aggregator.clearAndGet();
        Assert.assertNotNull(record);
        Assert.assertEquals(0, aggregator.getNumUserRecords());
        
        Assert.assertEquals(1, record.getNumUserRecords());
    }
}
