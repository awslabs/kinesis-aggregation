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
package com.amazonaws.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

/**
 * A sample of how to use the normal Amazon Kinesis client from the AWS SDK to
 * transmit records to Kinesis.
 */
public class SampleNormalProducer
{
    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.println("USAGE: SampleNormalProducer <stream name> <region>");
            System.exit(1);
        }

        String streamName = args[0];
        String regionName = args[1];

        AmazonKinesis producer = ProducerUtils.getKinesisProducer(regionName);

        System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
        List<PutRecordsRequestEntry> entries = new LinkedList<>();
        for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++)
        {
            byte[] data = ProducerUtils.randomData(i, ProducerConfig.RECORD_SIZE_BYTES);
            entries.add(new PutRecordsRequestEntry()
                        .withPartitionKey(ProducerUtils.randomPartitionKey())
                        .withExplicitHashKey(ProducerUtils.randomExplicitHashKey())
                        .withData(ByteBuffer.wrap(data)));
        }

        PutRecordsRequest request = new PutRecordsRequest().withRecords(entries).withStreamName(streamName);

        System.out.println("Sending " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
        producer.putRecords(request);
        System.out.println("Complete.");
    }
}
