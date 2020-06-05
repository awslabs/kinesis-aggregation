import java.util.List;

import com.amazonaws.kinesis.deagg.RecordDeaggregator;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class EchoHandler implements RequestHandler<KinesisEvent, Void> {

	@Override
	public Void handleRequest(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();

		// extract the records from the event
		List<KinesisEventRecord> records = event.getRecords();

		logger.log(String.format("Recieved %s Raw Records", records.size()));

		// now deaggregate the message contents
		List<UserRecord> deaggregated = new RecordDeaggregator<KinesisEventRecord>().deaggregate(records);
		logger.log(String.format("Received %s Deaggregated User Records", deaggregated.size()));
		
		deaggregated.stream().forEachOrdered(rec -> {
			logger.log(rec.getPartitionKey());
		});

		return null;
	}
}
