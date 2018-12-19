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
package com.amazonaws.kinesis.agg;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.amazonaws.kinesis.agg.RecordAggregator.RecordCompleteListener;

public class MonteCarloTestAggregation {
	private volatile int successRecords = 0;
	private volatile int failedRecords = 0;

	private final class IncrementCountListener implements RecordCompleteListener {
		private Integer userRecordCount = 0;

		public void recordComplete(AggRecord aggRecord) {
			if (aggRecord.getNumUserRecords() == 0) {
				System.err.println("Received empty Aggregation Record");
			} else {
				this.userRecordCount += aggRecord.getNumUserRecords();
			}
		}

		public Integer getCount() {
			return this.userRecordCount;
		}
	}

	private class TestRunner implements Runnable {
		public TestRunner(int instance, int messagesToSend) {
			this.instance = instance;
			this.messagesToSend = messagesToSend;
		}

		private int instance;
		private int messagesToSend;
		private IncrementCountListener listener = new IncrementCountListener();
		private Integer recordsSuccess = 0;
		private Integer recordsFailed = 0;
		private Integer recordRotations = 0;
		private Charset charset = Charset.forName("UTF-8");

		public int getRecordsSucess() {
			return this.recordsSuccess;
		}

		public int getRecordsFailed() {
			return this.recordsFailed;
		}

		public int getRecordsRotated() {
			return this.listener.getCount();
		}

		private String truncateToFitUtf8ByteLength(String s, int maxBytes) {
			CharsetDecoder decoder = charset.newDecoder();
			if (s == null) {
				return null;
			}
			byte[] sba = s.getBytes(charset);
			if (sba.length <= maxBytes) {
				return s;
			}
			// Ensure truncation by having byte buffer = maxBytes
			ByteBuffer bb = ByteBuffer.wrap(sba, 0, maxBytes);
			CharBuffer cb = CharBuffer.allocate(maxBytes);
			// Ignore an incomplete character
			decoder.onMalformedInput(CodingErrorAction.IGNORE);
			decoder.decode(bb, cb, true);
			decoder.flush(cb);
			return new String(cb.array(), 0, cb.position());
		}

		public void run() {
			Random r = new Random();
			RecordAggregator agg = new RecordAggregator();
			agg.onRecordComplete(listener);
			boolean stop = false;

			int i = 0;
			while (i < messagesToSend && !stop) {
				try {
					i++;
					// generate a random partition key of up to PARTITION_KEY_MAX_LENGTH characters
					int nextLen = new Double(AggRecord.PARTITION_KEY_MAX_LENGTH * r.nextDouble()).intValue();
					byte[] randomPkB = new byte[nextLen];
					r.nextBytes(randomPkB);
					String randomPKey = truncateToFitUtf8ByteLength(new String(randomPkB),
							AggRecord.PARTITION_KEY_MAX_LENGTH);
					if (randomPKey.length() == 0) {
						randomPKey = new Integer(r.nextInt()).toString();
					}

					// generate a random payload
					int targetDataSize = new Double((AggRecord.MAX_BYTES_PER_RECORD
							- AggRecord.AGGREGATION_OVERHEAD_BYTES - randomPKey.getBytes().length) * r.nextDouble())
									.intValue();
					int dataSize = r.nextInt(targetDataSize == 0 ? 1 : targetDataSize);
					byte[] randomData = new byte[dataSize];
					r.nextBytes(randomData);

					// fire the random data into the Aggregator

					AggRecord t = agg.addUserRecord(randomPKey, null, randomData);
					if (t != null) {
						this.recordRotations += 1;
					}
					this.recordsSuccess += 1;
				} catch (Exception e) {
					e.printStackTrace();
					this.recordsFailed += 1;
					// stop the thread
					stop = true;
				}
			}

			// invoke the listener with the last in flight messages
			AggRecord lastInFlight = agg.clearAndGet();
			listener.recordComplete(lastInFlight);

			System.out.println(String.format("%s: %s requested, %s success, %s failed, %s rotations", this.instance,
					this.messagesToSend, this.recordsSuccess, this.recordsFailed, this.recordRotations));
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void monteCarloTest() throws Exception {
		int cpus = Runtime.getRuntime().availableProcessors();
		int threads = cpus - 1;
		System.out.println("Created a test runner pool of " + threads + " threads");
		ExecutorService threadPool = Executors.newFixedThreadPool(threads);
		int totalMessages = 1_000_000;
		List<TestRunner> runners = new ArrayList<TestRunner>(threads);
		List<Future> running = new ArrayList<Future>(threads);

		for (int i = 0; i < threads; i++) {
			TestRunner task = new TestRunner(i, totalMessages / threads);
			runners.add(task);
			running.add(threadPool.submit(task));
		}

		System.out.println("All threads running");

		int runningCount = threads;
		while (runningCount != 0) {
			runningCount = 0;
			for (Future f : running) {
				if (!f.isDone()) {
					runningCount++;
				}
			}
		}

		int countSuccess = 0;
		int countFailed = 0;
		for (TestRunner t : runners) {
			countSuccess += t.getRecordsSucess();
			countFailed += t.getRecordsFailed();
		}

		assertEquals("Correct User Record Count", totalMessages, countSuccess);
		assertEquals("No Failed Records", 0, countFailed);
	}
}