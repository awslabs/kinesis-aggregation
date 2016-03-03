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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.kinesis.agg.KplProgressiveAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

public class SampleKinesisProgressiveAggregator
{
	private static final ExecutorService executor = Executors.newFixedThreadPool(5);
	
	public static void main(String[] args)
	{
		if (args.length != 2) 
		{
			System.err.println("Usage SampleKinesisProgressiveAggregator <stream name> <region>");
			System.exit(1);
		} 
		
		String streamName = args[0];
		String regionName = args[1];
		
		final AmazonKinesis producer = ProducerUtils.getKinesisProducer(regionName);
		
		final KplProgressiveAggregator aggregator = new KplProgressiveAggregator(streamName);
		aggregator.addKplAggregatorListener((stream, partitionKey, explicitHashKey, data) -> 
		{ 
			executor.submit(() ->
			{
				System.out.println("Submitting record EHK=" + explicitHashKey);
				producer.putRecord(new PutRecordRequest()
									.withStreamName(stream)
									.withPartitionKey(partitionKey)
									.withExplicitHashKey(explicitHashKey)
									.withData(data));
				System.out.println("Completed record EHK=" + explicitHashKey);
			});
		});
		
		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++)
		{
			byte[] data = ProducerUtils.generateData(i, ProducerConfig.RECORD_SIZE);
			//System.out.println(new String(data));
			aggregator.addUserRecord(ProducerConfig.RECORD_TIMESTAMP, ProducerUtils.randomExplicitHashKey(), data);
		}

		List<PutRecordRequest> requests = aggregator.drainPutRecordRequests();
		
		System.out.println("Draining remaining requests...");
		for(PutRecordRequest request : requests)
		{
			System.out.println("Submitting record EHK=" + request.getExplicitHashKey());
			producer.putRecord(request);
			System.out.println("Completed record EHK=" + request.getExplicitHashKey());
		}
		
		System.out.println("Waiting 120 seconds for all transmissions to complete...");
		try {
			executor.shutdown();
			executor.awaitTermination(120, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			//ignore
			e.printStackTrace();
		}
		System.out.println("Transmissions complete.");
	}
}
