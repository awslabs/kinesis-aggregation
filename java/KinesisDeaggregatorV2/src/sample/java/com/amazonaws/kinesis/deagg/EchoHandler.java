package com.amazonaws.kinesis.deagg;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class EchoHandler implements RequestHandler<KinesisEvent, Void> {

	@Override
	public Void handleRequest(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();

		// extract the records from the event
		List<KinesisEventRecord> records = event.getRecords();

		logger.log(String.format("Recieved %s Raw Records", records.size()));

		try {
			// now deaggregate the message contents
			List<KinesisClientRecord> deaggregated = new RecordDeaggregator<KinesisEventRecord>().deaggregate(records);
			logger.log(String.format("Received %s Deaggregated User Records", deaggregated.size()));

			deaggregated.stream().forEachOrdered(rec -> {
				logger.log(rec.partitionKey());
			});
		} catch (Exception e) {
			logger.log(e.getMessage());
		}

		return null;
	}
}
