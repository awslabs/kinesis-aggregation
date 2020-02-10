/**
 * Kinesis Aggregation/Deaggregation Libraries for Java
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.kinesis.deagg;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.kinesis.model.Record;

/**
 * A Kinesis deaggregator convenience class. This class contains
 * a number of static methods that provide different interfaces for
 * deaggregating user records from an existing aggregated Kinesis record. This class
 * is oriented towards deaggregating Kinesis records as provided by AWS Lambda
 * (for other applications, record deaggregation is handled transparently by the
 * Kinesis Consumer Library).
 * 
 * NOTE: Any non-aggregated records passed to any deaggregation methods will be
 * returned unchanged.
 *
 */
public class RecordDeaggregator {
	/**
	 * Interface used by a calling method to call the process function
	 *
	 */
	public interface KinesisUserRecordProcessor {
		public Void process(List<UserRecord> userRecords);
	}

	/**
	 * Method to process a set of Kinesis user records from a Stream of Kinesis
	 * Event Records using the Java 8 Streams API
	 * 
	 * @param inputStream
	 *            The Kinesis Event Records provided by AWS Lambda
	 * @param streamConsumer
	 *            Instance implementing the Consumer interface to process the
	 *            deaggregated UserRecords
	 * @return Void
	 */
	public static Void stream(Stream<KinesisEventRecord> inputStream, Consumer<UserRecord> streamConsumer) {
		// convert the event input record set to a List of Record
		List<Record> rawRecords = new LinkedList<>();
		inputStream.forEachOrdered(rec -> {
			rawRecords.add(rec.getKinesis());
		});

		// deaggregate UserRecords from the Kinesis Records
		List<UserRecord> deaggregatedRecords = UserRecord.deaggregate(rawRecords);
		deaggregatedRecords.stream().forEachOrdered(streamConsumer);

		return null;
	}

	/**
	 * Method to process a set of Kinesis user records from a list of Kinesis
	 * Event Records using pre-Streams style API
	 * 
	 * @param inputRecords
	 *            The Kinesis Event Records provided by AWS Lambda
	 * @param processor
	 *            Instance implementing KinesisUserRecordProcessor
	 * @return Void
	 */
	public static Void processRecords(List<KinesisEventRecord> inputRecords, KinesisUserRecordProcessor processor) {
		// extract raw Kinesis Records from input event records
		List<Record> rawRecords = new LinkedList<>();
		for (KinesisEventRecord rec : inputRecords) {
			rawRecords.add(rec.getKinesis());
		}

		// invoke provided processor
		return processor.process(UserRecord.deaggregate(rawRecords));
	}

	/**
	 * Method to bulk deaggregate a set of Kinesis user records from a list of
	 * Kinesis Event Records.
	 * 
	 * @param inputRecords
	 *            The Kinesis Event Records provided by AWS Lambda
	 * @return A list of Kinesis UserRecord objects obtained by deaggregating
	 *         the input list of KinesisEventRecords
	 */
	public static List<UserRecord> deaggregate(List<KinesisEventRecord> inputRecords) {
		List<UserRecord> outputRecords = new LinkedList<>();
		for (KinesisEventRecord inputRecord : inputRecords) {
			outputRecords.addAll(deaggregate(inputRecord));
		}
		return outputRecords;
	}

	/**
	 * Method to deaggregate a single Kinesis record into one or more
	 * Kinesis user records.
	 * 
	 * @param inputRecord
	 *            The single KinesisEventRecord to deaggregate
	 * @return A list of Kinesis UserRecord objects obtained by deaggregating
	 *         the input KinesisEventRecord
	 */
	public static List<UserRecord> deaggregate(KinesisEventRecord inputRecord) {
		return UserRecord.deaggregate(Arrays.asList(inputRecord.getKinesis()));
	}
}
