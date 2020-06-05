import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.kinesis.deagg.RecordDeaggregator;
import com.amazonaws.kinesis.deagg.RecordDeaggregator.KinesisUserRecordProcessor;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class TestLambdaDeaggregation {
	private static final int c = 10;
	private static Map<String, KinesisEventRecord> checkset = new HashMap<>();
	private static List<KinesisEventRecord> recordList = null;
	private static final RecordDeaggregator<KinesisEventRecord> deaggregator = new RecordDeaggregator<>();
	private static RecordAggregator aggregator = null;
	private static AggRecord aggregated = null;

	private final class TestKinesisUserRecordProcessor
			implements Consumer<KinesisClientRecord>, KinesisUserRecordProcessor {
		private int recordsProcessed = 0;

		public int getCount() {
			return this.recordsProcessed;
		}

		@Override
		public void accept(KinesisClientRecord t) {
			recordsProcessed += 1;
		}

		@Override
		public Void process(List<KinesisClientRecord> userRecords) {
			recordsProcessed += userRecords.size();

			return null;
		}

	}

	/* Verify that a provided set of UserRecords map 1:1 to the original checkset */
	private void verifyOneToOneMapping(List<KinesisClientRecord> userRecords) {
		userRecords.stream().forEachOrdered(userRecord -> {
			// get the original checkset record by ID
			KinesisEventRecord toCheck = checkset.get(userRecord.partitionKey());

			// confirm that toCheck is not null
			assertNotNull("Found Original CheckSet Record", toCheck);

			// confirm that the data is the same
			assertTrue("Data Correct", userRecord.data().compareTo(toCheck.getKinesis().getData()) == 0);
		});
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		aggregator = new RecordAggregator();

		recordList = new LinkedList<>();

		// create 10 random records for testing
		for (int i = 0; i < c; i++) {
			// create trackable id
			String id = UUID.randomUUID().toString();

			// create a kinesis model record
			byte[] data = RandomStringUtils.randomAlphabetic(20).getBytes();

			KinesisEvent.Record r = new KinesisEvent.Record();
			r.withPartitionKey(id).withApproximateArrivalTimestamp(new Date(System.currentTimeMillis()))
					.withData(ByteBuffer.wrap(data));
			KinesisEventRecord ker = new KinesisEventRecord();
			ker.setKinesis(r);
			recordList.add(ker);

			// add the record to the check set
			checkset.put(id, ker);

			// add the record to the aggregated AggRecord // create an aggregated set of
			aggregator.addUserRecord(id, data);
		}

		// get the aggregated data
		aggregated = aggregator.clearAndGet();
		assertEquals("Aggregated Record Count Correct", aggregated.getNumUserRecords(), c);
	}

	@Test
	public void testProcessor() throws Exception {
		// create a counting record processor
		TestKinesisUserRecordProcessor p = new TestKinesisUserRecordProcessor();

		// invoke deaggregation on the static records with this processor
		deaggregator.processRecords(recordList, p);

		assertEquals("Processed Record Count Correct", p.getCount(), recordList.size());
	}

	@Test
	public void testStream() throws Exception {
		// create a counting record processor
		TestKinesisUserRecordProcessor p = new TestKinesisUserRecordProcessor();

		// invoke deaggregation on the static records with this processor
		deaggregator.stream(recordList.stream(), p);

		assertEquals("Processed Record Count Correct", p.getCount(), recordList.size());
	}

	@Test
	public void testList() throws Exception {
		// invoke deaggregation on the static records, returning a List of UserRecord
		List<KinesisClientRecord> records = deaggregator.deaggregate(recordList);

		assertEquals("Processed Record Count Correct", records.size(), recordList.size());
		verifyOneToOneMapping(records);
	}

	@Test
	public void testAggregatedRecord() throws Exception {
		// create a new KinesisEvent.Record from the aggregated data
		KinesisEvent.Record r = new KinesisEvent.Record();
		r.setPartitionKey(aggregated.getPartitionKey());
		r.setApproximateArrivalTimestamp(new Date(System.currentTimeMillis()));
		r.setData(ByteBuffer.wrap(aggregated.toRecordBytes()));
		r.setKinesisSchemaVersion("1.0");
		KinesisEventRecord ker = new KinesisEventRecord();
		ker.setKinesis(r);

		// deaggregate the record
		List<KinesisClientRecord> userRecords = deaggregator.deaggregate(Arrays.asList(ker));

		assertEquals("Deaggregated Count Matches", aggregated.getNumUserRecords(), userRecords.size());
		verifyOneToOneMapping(userRecords);
	}

	@Test
	public void testEmpty() throws Exception {
		// invoke deaggregation on the static records, returning a List of UserRecord
		List<KinesisClientRecord> records = deaggregator.deaggregate(new ArrayList<KinesisEventRecord>());

		assertEquals("Processed Record Count Correct", records.size(), 0);
		verifyOneToOneMapping(records);
	}

	@Test
	public void testOne() throws Exception {
		// invoke deaggregation on the static records, returning a List of UserRecord
		List<KinesisClientRecord> records = deaggregator.deaggregate(recordList.get(0));

		assertEquals("Processed Record Count Correct", records.size(), 1);
		verifyOneToOneMapping(records);
	}
}
