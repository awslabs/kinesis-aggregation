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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;

/**
 * A sample of how to use the RecordAggregator to transmit records to Kinesis.
 */
public class SampleAggregatorProducer {
	/**
	 * Send an aggregated record to Kinesis using the specified producer and
	 * stream name.
	 */
	private static void sendRecord(AmazonKinesis producer, String streamName, AggRecord aggRecord) {
		if (aggRecord == null || aggRecord.getNumUserRecords() == 0) {
			return;
		}

		System.out.println("Submitting record EHK=" + aggRecord.getExplicitHashKey() + " NumRecords="
				+ aggRecord.getNumUserRecords() + " NumBytes=" + aggRecord.getSizeBytes());
		try {
			producer.putRecord(aggRecord.toPutRecordRequest(streamName));
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Completed record EHK=" + aggRecord.getExplicitHashKey());
	}

	/**
	 * Flush out and send any remaining records from the aggregator and then
	 * wait for all pending transmissions to finish.
	 */
	private static void flushAndFinish(AmazonKinesis producer, String streamName, RecordAggregator aggregator) {
		// Do one final flush & send to get any remaining records that haven't
		// triggered a callback yet
		AggRecord finalRecord = aggregator.clearAndGet();
		ForkJoinPool.commonPool().execute(() -> {
			sendRecord(producer, streamName, finalRecord);
		});

		// Wait up to 2 minutes for all the publisher threads to finish
		System.out.println("Waiting for all transmissions to complete...");
		ForkJoinPool.commonPool().awaitQuiescence(2, TimeUnit.MINUTES);
		System.out.println("Transmissions complete.");
	}

	/**
	 * Use the callback mechanism and a lambda function to send aggregated
	 * records to Kinesis.
	 */
	private static void sendViaCallback(AmazonKinesis producer, String streamName, RecordAggregator aggregator) {
		// add a lambda callback to be called when a full record is ready to
		// transmit
		aggregator.onRecordComplete((aggRecord) -> {
			sendRecord(producer, streamName, aggRecord);
		});

		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++) {
			String pk = ProducerUtils.randomPartitionKey();
			String ehk = ProducerUtils.randomExplicitHashKey();
			byte[] data = ProducerUtils.randomData(i, ProducerConfig.RECORD_SIZE_BYTES);
			try {
                aggregator.addUserRecord(pk, ehk, data);
            }
            catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to add user record: " + e.getMessage());
            }
		}

		flushAndFinish(producer, streamName, aggregator);
	}

	/**
	 * Use the synchronous batch mechanism to send aggregated records to
	 * Kinesis.
	 */
	@SuppressWarnings("unused")
	private static void sendViaBatch(AmazonKinesis producer, String streamName, RecordAggregator aggregator) {
		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++) {
			String pk = ProducerUtils.randomPartitionKey();
			String ehk = ProducerUtils.randomExplicitHashKey();
			byte[] data = ProducerUtils.randomData(i, ProducerConfig.RECORD_SIZE_BYTES);

			// addUserRecord returns non-null when a full record is ready to
			// transmit
			try {
                final AggRecord aggRecord = aggregator.addUserRecord(pk, ehk, data);
                if (aggRecord != null) {
                    ForkJoinPool.commonPool().execute(() -> {
                        sendRecord(producer, streamName, aggRecord);
                    });
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to add user record: " + e.getMessage());
            }
		}

		flushAndFinish(producer, streamName, aggregator);
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("USAGE: SampleAggregatorProducer <stream name> <region>");
			System.exit(1);
		}

		String streamName = args[0];
		String regionName = args[1];
		final AmazonKinesis producer = ProducerUtils.getKinesisProducer(regionName);
		final RecordAggregator aggregator = new RecordAggregator();

		sendViaCallback(producer, streamName, aggregator);
		// sendViaBatch(producer, streamName, aggregator);
	}
}
