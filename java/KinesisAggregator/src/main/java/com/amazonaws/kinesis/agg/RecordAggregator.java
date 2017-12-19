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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

/**
 * A class for taking multiple Kinesis user records and aggregating them into
 * more efficiently-packed records using the Kinesis aggregated record format.
 * 
 * This class is NOT thread-safe.
 * 
 * @see <a href="https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md">https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md</a>
 */
@NotThreadSafe
public class RecordAggregator {
	/**
	 * A listener interface for receiving notifications when this aggregated
	 * record has reached its maximum allowable size.
	 */
	public interface RecordCompleteListener {
		/**
		 * Called when an aggregated record is full and ready to be transmitted
		 * to Kinesis.
		 * 
		 * @param aggRecord
		 *            A complete aggregated record ready to transmit to Kinesis.
		 */
		public abstract void recordComplete(AggRecord aggRecord);
	}

	/** The current aggregated record being constructed. */
	private AggRecord currentRecord;
	/** The list of listeners to notify when a record is complete. */
	private List<ListenerExecutorPair> listeners;

	/**
	 * Construct a new empty record aggregator instance.
	 */
	public RecordAggregator() {
		this.currentRecord = new AggRecord();
		this.listeners = new LinkedList<>();
	}

	/**
	 * @return The number of user records currently contained in this aggregated
	 *         record.
	 */
	public int getNumUserRecords() {
		return this.currentRecord.getNumUserRecords();
	}

	/**
	 * @return The size of this aggregated record in bytes. This value is always
	 *         less than the Kinesis-defined maximum size for a PutRecordRequest
	 *         (i.e. 1MB as of 3/26/2016).
	 */
	public long getSizeBytes() {
		return this.currentRecord.getSizeBytes();
	}

	/**
	 * Clear all the user records from this aggregated record and reset it to an
	 * empty state.
	 * 
	 * NOTE: Will not affect any registered listeners.
	 */
	public void clearRecord() {
		this.currentRecord = new AggRecord();
	}

	/**
	 * Clear all the listeners from this object that were registered with the
	 * onRecordComplete method.
	 */
	public void clearListeners() {
		this.listeners.clear();
	}

	/**
	 * Register a callback method to be notified when there is a full aggregated
	 * record available. Callbacks registered via this method are executed on a
	 * separate thread from the common ForkJoin pool.
	 * 
	 * @param listener
	 *            The listener to receive a callback when there is a complete
	 *            aggregated record available (can be a lambda function).
	 */
	public void onRecordComplete(RecordCompleteListener listener) {
		onRecordComplete(listener, ForkJoinPool.commonPool());
	}

	/**
	 * Register a callback method to be notified when there is a full aggregated
	 * record available and invoke the callback using the specified executor.
	 * 
	 * @param listener
	 *            The listener to receive a callback when there is a complete
	 *            aggregated record available (can be a lambda function).
	 * @param executor
	 *            The executor to use to execute the callback.
	 */
	public void onRecordComplete(RecordCompleteListener listener, Executor executor) {
		this.listeners.add(new ListenerExecutorPair(listener, executor));
	}

	/**
	 * Get the current contents of this aggregated record (whether full or not)
	 * as a single record and then clear the contents of this object so it can
	 * be re-used. This method is useful for flushing the aggregated record when
	 * you need to transmit it before it is full (e.g. you're shutting down or
	 * haven't transmitted in a while).
	 * 
	 * @return This current object as an aggregated record or null if this
	 *         object is currently empty.
	 */
	public AggRecord clearAndGet() {
		if (getNumUserRecords() == 0) {
			return null;
		}

		AggRecord out = this.currentRecord;
		clearRecord();
		return out;
	}

	/**
	 * Add a new user record to this aggregated record (will trigger a callback
	 * via onRecordComplete if aggregated record is full).
	 *
	 * @param userRecord
	 *            The Kinesis user record to add to this aggregated record
	 * @return A AggRecord if this aggregated record is full and ready to be
	 *         transmitted or null otherwise.
	 * @throws Exception If the user record can't be added to the current agg record
	 */
	public AggRecord addUserRecord(UserRecord userRecord) throws Exception {
		if (userRecord == null) {
			throw new IllegalArgumentException("Input user record cannot be null.");
		} else if (!userRecord.getData().hasArray()) {
			throw new IllegalStateException(
					"The addUserRecord method only works for UserRecord objects whose data ByteBuffer "
							+ " has a backing byte[] available.");
		}
		return addUserRecord(userRecord.getPartitionKey(), userRecord.getExplicitHashKey(),
				userRecord.getData().array());
	}

	/**
	 * Add a new user record to this aggregated record (will trigger a callback
	 * via onRecordComplete if aggregated record is full).
	 * 
	 * @param partitionKey
	 *            The partition key of the record to add
	 * @param data
	 *            The record data of the record to add
	 * @return A AggRecord if this aggregated record is full and ready to be
	 *         transmitted or null otherwise.
     * @throws Exception If the user record can't be added to the current agg record
	 */
	public AggRecord addUserRecord(String partitionKey, byte[] data) throws Exception {
		return addUserRecord(partitionKey, null, data);
	}

	/**
	 * Add a new user record to this aggregated record (will trigger a callback
	 * via onRecordComplete if aggregated record is full).
	 * 
	 * @param partitionKey
	 *            The partition key of the record to add
	 * @param explicitHashKey
	 *            The explicit hash key of the record to add
	 * @param data
	 *            The record data of the record to add
	 * @return A AggRecord if this aggregated record is full and ready to be
	 *         transmitted or null otherwise.
     * @throws Exception If the user record can't be added to the current agg record
	 */
	public AggRecord addUserRecord(String partitionKey, String explicitHashKey, byte[] data) throws Exception {
		boolean success = this.currentRecord.addUserRecord(partitionKey, explicitHashKey, data);

		if (success) {
			// we were able to add the current data to the in-flight record
			return null;
		} else {
			// this record is full, let all the listeners know
			final AggRecord completeRecord = this.currentRecord;
			for (ListenerExecutorPair pair : this.listeners) {
				pair.getExecutor().execute(() -> {
					pair.getListener().recordComplete(completeRecord);
				});
			}

			// current record is full; clear it out, make a new empty one and
			// add the new user record
			clearRecord();
			success = this.currentRecord.addUserRecord(partitionKey, explicitHashKey, data);

			if (!success) {
				throw new Exception(String.format("Unable to add User Record %s, %s with data length %s", partitionKey,
						explicitHashKey, data.length));
			}

			return completeRecord;
		}
	}

	/**
	 * A helper class for tracking callbacks that contains a listener for
	 * callbacks and the executor to execute the callback with.
	 */
	private class ListenerExecutorPair {
		/** The listener to use for making a callback. */
		private RecordCompleteListener listener;
		/** The executor to execute the listener callback on. */
		private Executor executor;

		/**
		 * Create a new listener/executor pair.
		 */
		public ListenerExecutorPair(RecordCompleteListener listener, Executor executor) {
			this.listener = listener;
			this.executor = executor;
		}

		/**
		 * @return Get the listener object.
		 */
		public RecordCompleteListener getListener() {
			return this.listener;
		}

		/**
		 * @return Get the executor associated with the listener.
		 */
		public Executor getExecutor() {
			return this.executor;
		}
	}
}
