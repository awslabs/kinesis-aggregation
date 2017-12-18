/**
 * Kinesis Aggregation/Deaggregation Libraries for Java
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

import java.nio.charset.StandardCharsets;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.kinesis.agg.AggRecord;
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
        
        try
        {
            aggregator.addUserRecord("partition_key", ALPHABET.getBytes(StandardCharsets.UTF_8));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            Assert.fail("Encountered unexpected exception: " + e.getMessage());
        }        
        Assert.assertEquals(1, aggregator.getNumUserRecords());
        
        AggRecord record = aggregator.clearAndGet();
        Assert.assertNotNull(record);
        Assert.assertEquals(0, aggregator.getNumUserRecords());
        
        Assert.assertEquals(1, record.getNumUserRecords());
    }
}
