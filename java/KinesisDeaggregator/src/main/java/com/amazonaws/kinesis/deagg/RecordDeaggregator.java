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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesisfirehose.model.InvalidArgumentException;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

/**
 * A Kinesis deaggregator convenience class. This class contains a number of
 * static methods that provide different interfaces for deaggregating user
 * records from an existing aggregated Kinesis record. This class is oriented
 * towards deaggregating Kinesis records as provided by AWS Lambda, or through
 * the Kinesis SDK. Parameterise the instance with the required types
 * (supporting
 * com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord
 * or com.amazonaws.services.kinesis.model.Record only)
 * 
 * NOTE: Any non-aggregated records passed to any deaggregation methods will be
 * returned unchanged.
 *
 */
public class RecordDeaggregator<T> {
	/**
	 * Interface used by a calling method to call the process function
	 *
	 */
	public interface KinesisUserRecordProcessor {
		public Void process(List<UserRecord> userRecords);
	}

	private com.amazonaws.services.kinesis.model.Record convertOne(KinesisEventRecord record) {
		KinesisEvent.Record r = record.getKinesis();
		com.amazonaws.services.kinesis.model.Record out = new com.amazonaws.services.kinesis.model.Record()
				.withPartitionKey(r.getPartitionKey()).withEncryptionType(r.getEncryptionType())
				.withApproximateArrivalTimestamp(r.getApproximateArrivalTimestamp())
				.withSequenceNumber(r.getSequenceNumber()).withData(r.getData());

		return out;

	}

	private List<com.amazonaws.services.kinesis.model.Record> convertToKinesis(List<KinesisEventRecord> inputRecords) {
		List<com.amazonaws.services.kinesis.model.Record> response = new ArrayList<>();

		inputRecords.stream().forEachOrdered(record -> {
			response.add(convertOne(record));
		});

		return response;

	}

	@SuppressWarnings("unchecked")
	private List<Record> convertType(List<T> inputRecords) {
		List<Record> records = null;

		if (inputRecords.size() > 0 && inputRecords.get(0) instanceof KinesisEventRecord) {
			records = convertToKinesis((List<KinesisEventRecord>) inputRecords);
		} else if (inputRecords.size() > 0 && inputRecords.get(0) instanceof Record) {
			records = (List<Record>) inputRecords;
		} else {
			if (inputRecords.size() == 0) {
				return new ArrayList<Record>();
			} else {
				throw new InvalidArgumentException("Input Types must be Kinesis Event or Model Records");
			}
		}

		return records;
	}

	/**
	 * Method to process a set of Kinesis user records from a Stream of Kinesis
	 * Event Records using the Java 8 Streams API
	 * 
	 * @param inputStream    The Kinesis Records provided by AWS Lambda or the
	 *                       Kinesis SDK
	 * @param streamConsumer Instance implementing the Consumer interface to process
	 *                       the deaggregated UserRecords
	 * @return Void
	 */
	public Void stream(Stream<T> inputStream, Consumer<UserRecord> streamConsumer) {
		// deaggregate UserRecords from the Kinesis Records

		List<T> streamList = inputStream.collect(Collectors.toList());
		List<UserRecord> deaggregatedRecords = UserRecord.deaggregate(convertType(streamList));
		deaggregatedRecords.stream().forEachOrdered(streamConsumer);

		return null;
	}

	/**
	 * Method to process a set of Kinesis user records from a list of Kinesis
	 * Records using pre-Streams style API
	 * 
	 * @param inputRecords The Kinesis Records provided by AWS Lambda
	 * @param processor    Instance implementing KinesisUserRecordProcessor
	 * @return Void
	 */
	public Void processRecords(List<T> inputRecords, KinesisUserRecordProcessor processor) {
		// invoke provided processor
		return processor.process(UserRecord.deaggregate(convertType(inputRecords)));
	}

	/**
	 * Method to bulk deaggregate a set of Kinesis user records from a list of
	 * Kinesis Event Records.
	 * 
	 * @param inputRecords The Kinesis Records provided by AWS Lambda
	 * @return A list of Kinesis UserRecord objects obtained by deaggregating the
	 *         input list of KinesisEventRecords
	 */
	public List<UserRecord> deaggregate(List<T> inputRecords) {
		List<UserRecord> outputRecords = new LinkedList<>();
		outputRecords.addAll(UserRecord.deaggregate(convertType(inputRecords)));

		return outputRecords;
	}

	/**
	 * Method to deaggregate a single Kinesis record into a List of UserRecords
	 * 
	 * @param inputRecord The Kinesis Record provided by AWS Lambda or Kinesis SDK
	 * @return A list of Kinesis UserRecord objects obtained by deaggregating the
	 *         input list of KinesisEventRecords
	 */
	public List<UserRecord> deaggregate(T inputRecord) {
		return UserRecord.deaggregate(convertType(Arrays.asList(inputRecord)));
	}
}
