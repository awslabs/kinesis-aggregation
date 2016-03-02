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
package com.amazonaws.kinesis.producer;

import java.util.List;

import com.amazonaws.kinesis.agg.KplBatchAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

public class SampleKinesisBatchAggregator
{
	public static void main(String[] args)
	{
		if (args.length != 2) 
		{
			System.err.println("Usage SampleKinesisAggregator <stream name> <region>");
			System.exit(1);
		} 
		
		String streamName = args[0];
		String regionName = args[1];
		
		AmazonKinesis producer = ProducerUtils.getKinesisProducer(regionName);
		KplBatchAggregator aggregator = new KplBatchAggregator(streamName);
		
		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++)
		{
			byte[] data = ProducerUtils.generateData(i, ProducerConfig.RECORD_SIZE);
			//System.out.println(new String(data));
			aggregator.addUserRecord(ProducerConfig.RECORD_TIMESTAMP, ProducerUtils.randomExplicitHashKey(), data);
		}

		List<PutRecordRequest> requests = aggregator.extractPutRecordRequests();
		
		System.out.println("Sending " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for(PutRecordRequest request : requests)
		{
			producer.putRecord(request);
		}
		System.out.println("Complete.");
	}
}
