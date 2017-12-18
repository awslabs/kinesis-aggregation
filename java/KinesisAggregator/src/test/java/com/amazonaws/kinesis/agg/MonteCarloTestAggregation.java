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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

import com.amazonaws.kinesis.agg.RecordAggregator.RecordCompleteListener;

public class MonteCarloTestAggregation {
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
		public TestRunner(int instance, int tests) {
			this.instance = instance;
			this.maxTests = tests;
		}

		private int instance;
		private int maxTests;
		private IncrementCountListener listener = new IncrementCountListener();
		private Integer recordsSuccess = 0;
		private Integer recordsFailed = 0;

		public int getRecordsSucess() {
			return this.recordsSuccess;
		}

		public int getRecordsRotated() {
			return this.listener.getCount();
		}

		public void run() {
			Random r = new Random();
			RecordAggregator agg = new RecordAggregator();
			agg.onRecordComplete(listener);

			for (int i = 0; i < maxTests; i++) {
				// generate a random partition key
				int pkeySize = 0;
				while (pkeySize == 0) {
					pkeySize = r.nextInt(AggRecord.PARTITION_KEY_MAX_LENGTH);
				}
				byte[] blob = new byte[pkeySize];
				r.nextBytes(blob);
				String randomPKey = new String(blob);

				// generate a random payload
				int dataSize = r.nextInt(AggRecord.MAX_BYTES_PER_RECORD - AggRecord.AGGREGATION_OVERHEAD_BYTES);
				byte[] randomData = new byte[dataSize];
				r.nextBytes(randomData);

				// fire the random data into the Aggregator
				try {
					agg.addUserRecord(randomPKey, null, randomData);
					this.recordsSuccess += 1;
				} catch (Exception e) {
					e.printStackTrace();
					this.recordsFailed += 1;
				}
			}

			// invoke the listener with the last in flight messages
			AggRecord lastInFlight = agg.clearAndGet();
			listener.recordComplete(lastInFlight);

			System.out.println(String.format("%s: %s requested, %s success, %s failed", this.instance, this.maxTests,
					this.recordsSuccess, this.recordsFailed));
		}
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void monteCarloTest() throws Exception {
		int cpus = Runtime.getRuntime().availableProcessors();
		int threads = cpus - 1;
		System.out.println("Created a test runner pool of " + threads + " threads");
		ExecutorService threadPool = Executors.newFixedThreadPool(threads);
		int totalTests = 1_000_000;
		List<TestRunner> runners = new ArrayList<TestRunner>(threads);
		List<Future> running = new ArrayList<Future>(threads);

		for (int i = 0; i < threads; i++) {
			TestRunner task = new TestRunner(i, totalTests / threads);
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

		System.out.println("Tests Completed");

		int countSuccess = 0;
		int countAgg = 0;
		for (TestRunner t : runners) {
			countSuccess += t.getRecordsSucess();
			countAgg += t.getRecordsRotated();
		}

		org.junit.Assert.assertEquals("Correct User Record Count", countSuccess, countAgg);
	}
}
