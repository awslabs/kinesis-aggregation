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
package com.amazonaws;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

/**
 * Kinesis Producer Library Deaggregator convenience class
 *
 */
public class KplDeaggregator {
	/**
	 * Method to process a set of Kinesis User Records from a Stream of Kinesis
	 * Event Records using the Java 8 Streams API
	 * 
	 * @param inputStream
	 *            The Kinesis Event Records provided by AWS Lambda
	 * @param streamConsumer
	 *            Instance implementing the Consumer interface to process the
	 *            deaggregated UserRecords
	 * @return Void
	 */
	public Void stream(Stream<KinesisEventRecord> inputStream,
			Consumer<UserRecord> streamConsumer) {
		// convert the event input record set to a List of Record
		List<Record> rawRecords = new LinkedList<>();
		inputStream.forEachOrdered(rec -> {
			rawRecords.add(rec.getKinesis());
		});

		// deaggregate UserRecords from the Kinesis Records
		List<UserRecord> deaggregatedRecords = UserRecord
				.deaggregate(rawRecords);

		deaggregatedRecords.stream().forEachOrdered(streamConsumer);

		return null;
	}

	/**
	 * Interface used by a calling method to call the process function
	 *
	 */
	public interface KinesisUserRecordProcessor {
		public Void process(List<UserRecord> userRecords) throws Exception;
	}

	/**
	 * Method to process a set of Kinesis User Records from a list of Kinesis
	 * Event Records using pre-Streams style API
	 * 
	 * @param inputRecords
	 *            The Kinesis Event Records provided by AWS Lambda
	 * @param processor
	 *            Instance implementing KinesisUserRecordProcessor
	 * @return Void
	 */
	public Void processRecords(List<KinesisEventRecord> inputRecords,
			KinesisUserRecordProcessor processor) throws Exception {
		// extract raw Kinesis Records from input event records
		List<Record> rawRecords = new LinkedList<>();
		for (KinesisEventRecord rec : inputRecords) {
			rawRecords.add(rec.getKinesis());
		}

		// invoke provided processor
		return processor.process(UserRecord.deaggregate(rawRecords));
	}
}
