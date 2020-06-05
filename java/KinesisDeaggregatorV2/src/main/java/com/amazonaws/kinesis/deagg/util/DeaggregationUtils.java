package com.amazonaws.kinesis.deagg.util;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

import software.amazon.awssdk.core.SdkBytes;

public class DeaggregationUtils {
	public static software.amazon.awssdk.services.kinesis.model.Record convertOne(KinesisEventRecord record) {
		KinesisEvent.Record r = record.getKinesis();
		software.amazon.awssdk.services.kinesis.model.Record out = software.amazon.awssdk.services.kinesis.model.Record
				.builder().partitionKey(r.getPartitionKey()).encryptionType(r.getEncryptionType())
				.approximateArrivalTimestamp(r.getApproximateArrivalTimestamp().toInstant())
				.sequenceNumber(r.getSequenceNumber()).data(SdkBytes.fromByteBuffer(r.getData())).build();

		return out;
	}

	public static List<software.amazon.awssdk.services.kinesis.model.Record> convertToKinesis(
			List<KinesisEventRecord> inputRecords) {
		List<software.amazon.awssdk.services.kinesis.model.Record> response = new ArrayList<>();

		inputRecords.stream().forEachOrdered(record -> {
			response.add(convertOne(record));
		});

		return response;

	}
}
