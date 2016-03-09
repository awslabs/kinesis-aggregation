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
package com.amazonaws.kinesis.producer;

import com.amazonaws.kinesis.agg.KinesisAggRecord;
import com.amazonaws.kinesis.agg.KplAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;

/**
 * A sample of how to use the KplAggregator to transmit records
 * to Kinesis.
 */
public class SampleAggregatorProducer
{
	/**
	 * Send an aggregated record to Kinesis using the specified producer and stream name.
	 */
	private static void sendRecord(AmazonKinesis producer, String streamName, KinesisAggRecord aggRecord)
	{
		if(aggRecord == null || aggRecord.getNumUserRecords() == 0)
		{
			return;
		}
		
		System.out.println("Submitting record EHK=" + aggRecord.getExplicitHashKey() + " DataSize=" + aggRecord.getSizeBytes());
		producer.putRecord(aggRecord.toPutRecordRequest(streamName));
		System.out.println("Completed record EHK=" + aggRecord.getExplicitHashKey());
	}
	
	/**
	 * Use the callback mechanism and a lambda function to send aggregated records to Kinesis.
	 */
	private static void sendViaCallback(AmazonKinesis producer, String streamName, KplAggregator aggregator)
	{
		aggregator.onRecordComplete((aggRecord) -> 
		{ 
			try
			{
				sendRecord(producer, streamName, aggRecord);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
		});
		
		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++)
		{
			byte[] data = ProducerUtils.generateData(i, ProducerConfig.RECORD_SIZE_BYTES);
			aggregator.addUserRecord(ProducerConfig.RECORD_TIMESTAMP, ProducerUtils.randomExplicitHashKey(), data);
		}

		try
		{
			sendRecord(producer, streamName, aggregator.clearAndGet());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		System.out.println("Transmissions complete.");
	}
	
	/**
	 * Use the synchronous batch mechanism to send aggregated records to Kinesis.
	 */
	private static void sendViaBatch(AmazonKinesis producer, String streamName, KplAggregator aggregator)
	{
		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++)
		{
			byte[] data = ProducerUtils.generateData(i, ProducerConfig.RECORD_SIZE_BYTES);
			KinesisAggRecord aggRecord = aggregator.addUserRecord(ProducerConfig.RECORD_TIMESTAMP, ProducerUtils.randomExplicitHashKey(), data);
			if(aggRecord != null)
			{
				sendRecord(producer, streamName, aggRecord);
			}
		}

		sendRecord(producer, streamName, aggregator.clearAndGet());
		System.out.println("Transmissions complete.");
	}
	
	public static void main(String[] args)
	{
		if (args.length != 2) 
		{
			System.err.println("Usage SampleAggregatorProducer <stream name> <region>");
			System.exit(1);
		} 
		
		String streamName = args[0];
		String regionName = args[1];
		final AmazonKinesis producer = ProducerUtils.getKinesisProducer(regionName);
		final KplAggregator aggregator = new KplAggregator();
		
		sendViaCallback(producer, streamName, aggregator);
		sendViaBatch(producer, streamName, aggregator);
	}
}
