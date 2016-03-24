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
package com.amazonaws.kinesis.consumer;

import java.util.List;

import com.amazonaws.kinesis.deagg.RecordDeaggregator;
import com.amazonaws.kinesis.deagg.RecordDeaggregator.KinesisUserRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

/**
 * A sample AWS Lambda function to process records that were aggregated via the
 * Kinesis Producer Library of the KinesisAggregator project.
 */
public class KinesisLambdaReceiver implements RequestHandler<KinesisEvent, Void> {
	/**
	 * Handle a Kinesis request and process it using a stream-oriented
	 * processing method.
	 */
	public Void handleRequest(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Received " + event.getRecords().size() + " raw Event Records.");

		// Stream the User Records from the Lambda Event
		RecordDeaggregator.stream(event.getRecords().stream(), userRecord -> {
			// Your User Record Processing Code Here!
			logger.log(String.format("Processing UserRecord %s (%s:%s)", userRecord.getPartitionKey(),
					userRecord.getSequenceNumber(), userRecord.getSubSequenceNumber()));
		});

		return null;
	}

	/**
	 * Handle a Kinesis request and process it using a batch-oriented processing
	 * method.
	 */
	public Void handleRequestWithLists(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Received " + event.getRecords().size() + " raw Event Records.");

		try {
			// process the user records with an anonymous record processor
			// instance
			RecordDeaggregator.processRecords(event.getRecords(), new KinesisUserRecordProcessor() {
				public Void process(List<UserRecord> userRecords) {
					for (UserRecord userRecord : userRecords) {
						// Your User Record Processing Code Here!
						logger.log(String.format("Processing UserRecord %s (%s:%s)", userRecord.getPartitionKey(),
								userRecord.getSequenceNumber(), userRecord.getSubSequenceNumber()));
					}

					return null;
				}
			});
		} catch (Exception e) {
			logger.log(e.getMessage());
		}

		return null;
	}

	/**
	 * Handle a Kinesis request and process it using a bulk processing method.
	 */
	public Void handleRequestBulkList(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("Received " + event.getRecords().size() + " raw Event Records.");

		try {
			List<UserRecord> userRecords = RecordDeaggregator.deaggregate(event.getRecords());
			for (UserRecord userRecord : userRecords) {
				// Your User Record Processing Code Here!
				logger.log(String.format("Processing UserRecord %s (%s:%s)", userRecord.getPartitionKey(),
						userRecord.getSequenceNumber(), userRecord.getSubSequenceNumber()));
			}
		} catch (Exception e) {
			logger.log(e.getMessage());
		}

		return null;
	}

}
