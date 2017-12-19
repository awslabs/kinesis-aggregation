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

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.annotation.NotThreadSafe;
import com.amazonaws.services.kinesis.clientlibrary.types.Messages.AggregatedRecord;
import com.amazonaws.services.kinesis.clientlibrary.types.Messages.Record;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.protobuf.ByteString;

/**
 * 
 * Represents a single aggregated Kinesis record. This Kinesis record is built
 * by adding multiple user records and then serializing them to bytes using the
 * Kinesis aggregated record format. This class lifts heavily from the existing
 * KPL C++ libraries found at
 * https://github.com/awslabs/amazon-kinesis-producer.
 *
 * This class is NOT thread-safe.
 * 
 * @see <a href="https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md">https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md</a>
 */
@NotThreadSafe
public class AggRecord {
	// Serialization protocol constants via the specification at
	// https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	private static final byte[] AGGREGATED_RECORD_MAGIC = new byte[] { (byte) 0xf3, (byte) 0x89, (byte) 0x9a,
			(byte) 0xc2 };
	protected static final String MESSAGE_DIGEST_NAME = "MD5";
	private static final BigInteger UINT_128_MAX = new BigInteger(StringUtils.repeat("FF", 16), 16);

	// Kinesis Limits
	// (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html)
	protected static final int MAX_BYTES_PER_RECORD = 1024 * 1024; // 1 MB
	protected static final int AGGREGATION_OVERHEAD_BYTES = 256;
	protected static final int PARTITION_KEY_MIN_LENGTH = 1;
	protected static final int PARTITION_KEY_MAX_LENGTH = 256;

	/** The current size of the aggregated protobuf message. */
	private int aggregatedMessageSizeBytes;
	/** The set of unique explicit hash keys in the protocol buffer message. */
	private final KeySet explicitHashKeys;
	/** The set of unique partition keys in the protocol buffer message. */
	private final KeySet partitionKeys;
	/**
	 * The current builder object by which we're actively constructing a record.
	 */
	private AggregatedRecord.Builder aggregatedRecordBuilder;
	/**
	 * The message digest to use for calculating MD5 checksums per the protocol
	 * specification.
	 */
	private final MessageDigest md5;
	/** The partition key for the entire aggregated record. */
	private String aggPartitionKey;
	/** The explicit hash key for the entire aggregated record. */
	private String aggExplicitHashKey;

	/**
	 * Construct a new (empty) aggregated Kinesis record.
	 */
	public AggRecord() {
		this.aggregatedRecordBuilder = AggregatedRecord.newBuilder();
		this.aggregatedMessageSizeBytes = 0;
		this.explicitHashKeys = new KeySet();
		this.partitionKeys = new KeySet();

		this.aggExplicitHashKey = "";
		this.aggPartitionKey = "";

		try {
			this.md5 = MessageDigest.getInstance(MESSAGE_DIGEST_NAME);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException("Could not create an MD5 message digest.", e);
		}
	}

	/**
	 * Get the current number of user records contained inside this aggregate
	 * record.
	 * 
	 * @return The current number of user records added via the
	 *         "addUserRecord(...)" method.
	 */
	public int getNumUserRecords() {
		return this.aggregatedRecordBuilder.getRecordsCount();
	}

	/**
	 * Get the current size in bytes of the fully serialized aggregated record.
	 * 
	 * @return The current size in bytes of this message in its serialized form.
	 */
	public int getSizeBytes() {
		if (getNumUserRecords() == 0) {
			return 0;
		}

		return AGGREGATED_RECORD_MAGIC.length + this.aggregatedMessageSizeBytes + this.md5.getDigestLength();
	}

	/**
	 * Serialize this record to bytes. Has no side effects (i.e. does not affect
	 * the contents of this record object).
	 * 
	 * @return A byte array containing an Kinesis aggregated format-compatible
	 *         Kinesis record.
	 */
	public byte[] toRecordBytes() {
		if (getNumUserRecords() == 0) {
			return new byte[0];
		}

		byte[] messageBody = this.aggregatedRecordBuilder.build().toByteArray();

		this.md5.reset();
		byte[] messageDigest = this.md5.digest(messageBody);

		// The way Java's API works is that write(byte[]) throws IOException on
		// a ByteArrayOutputStream, but
		// write(byte[],int,int) doesn't so that's why we're using the long
		// version of "write" here
		ByteArrayOutputStream baos = new ByteArrayOutputStream(getSizeBytes());
		baos.write(AGGREGATED_RECORD_MAGIC, 0, AGGREGATED_RECORD_MAGIC.length);
		baos.write(messageBody, 0, messageBody.length);
		baos.write(messageDigest, 0, messageDigest.length);

		return baos.toByteArray();
	}

	/**
	 * Clears out all records and metadata from this object so that it can be
	 * reused just like a fresh instance of this object.
	 */
	public void clear() {
		this.md5.reset();
		this.aggExplicitHashKey = "";
		this.aggPartitionKey = "";
		this.aggregatedMessageSizeBytes = 0;
		this.explicitHashKeys.clear();
		this.partitionKeys.clear();
		this.aggregatedRecordBuilder = AggregatedRecord.newBuilder();
	}

	/**
	 * Get the overarching partition key for the entire aggregated record.
	 * 
	 * @return The partition key to use for the aggregated record or null if
	 *         this aggregated record is empty.
	 */
	public String getPartitionKey() {
		if (getNumUserRecords() == 0) {
			return null;
		}

		return this.aggPartitionKey;
	}

	/**
	 * Get the overarching explicit hash key for the entire aggregated record.
	 * 
	 * @return The explicit hash key to use for the aggregated record or null if
	 *         this aggregated record is empty.
	 */
	public String getExplicitHashKey() {
		if (getNumUserRecords() == 0) {
			return null;
		}

		return this.aggExplicitHashKey;
	}

	/**
	 * Based on the current size of this aggregated record, calculate what the
	 * new size would be if we added another user record with the specified
	 * parameters (used to determine when this aggregated record is full and
	 * can't accept any more user records). This calculation is highly dependent
	 * on the Kinesis aggregated message format.
	 * 
	 * @param partitionKey
	 *            The partition key of the new record to simulate adding
	 * @param explicitHashKey
	 *            The explicit hash key of the new record to simulate adding
	 * @param data
	 *            The raw data of the new record to simulate adding
	 * @return The new size of this existing record in bytes if a new user
	 *         record with the specified parameters was added.
	 * @see https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	 */
	private int calculateRecordSize(String partitionKey, String explicitHashKey, byte[] data) {
		int messageSize = 0;

		// has the partition key been added to the table of known PKs yet?
		if (!this.partitionKeys.contains(partitionKey)) {
			int pkLength = partitionKey.length();
			messageSize += 1; // (message index + wire type for PK table)
			messageSize += calculateVarintSize(pkLength); // size of pk length
															// value
			messageSize += pkLength; // actual pk length
		}

		// has the explicit hash key been added to the table of known EHKs yet?
		if (!this.explicitHashKeys.contains(explicitHashKey)) {
			int ehkLength = explicitHashKey.length();
			messageSize += 1; // (message index + wire type for EHK table)
			messageSize += calculateVarintSize(
					ehkLength); /* size of ehk length value */
			messageSize += ehkLength; // actual ehk length
		}

		// remaining calculations are for adding the new record to the list of
		// records

		long innerRecordSize = 0;

		// partition key field
		innerRecordSize += 1; // (message index + wire type for PK index)
		innerRecordSize += calculateVarintSize(this.partitionKeys
				.getPotentialIndex(partitionKey)); /* size of pk index value */

		// explicit hash key field (this is optional)
		if (explicitHashKey != null) {
			innerRecordSize += 1; // (message index + wire type for EHK index)
			innerRecordSize += calculateVarintSize(this.explicitHashKeys.getPotentialIndex(
					explicitHashKey)); /* size of ehk index value */
		}

		// data field
		innerRecordSize += 1; // (message index + wire type for record data)

		innerRecordSize += calculateVarintSize(
				data.length); /* size of data length value */
		innerRecordSize += data.length; // actual data length

		messageSize += 1; // (message index + wire type for record)
		messageSize += calculateVarintSize(
				innerRecordSize); /* size of entire record length value */
		messageSize += innerRecordSize; // actual entire record length

		return messageSize;
	}

	/**
	 * For an integral value represented by a varint, calculate how many bytes
	 * are necessary to represent the value in a protobuf message.
	 * 
	 * @param value
	 *            The value whose varint size will be calculated
	 * @return The number of bytes necessary to represent the input value as a
	 *         varint.
	 * @see https://developers.google.com/protocol-buffers/docs/encoding#varints
	 */
	private int calculateVarintSize(long value) {
		if (value < 0) {
			throw new IllegalArgumentException("Size values should not be negative.");
		}

		int numBitsNeeded = 0;
		if (value == 0) {
			numBitsNeeded = 1;
		} else {
			// shift the value right one bit at a time until
			// there are no more '1' bits left...this counts
			// how many bits we need to represent the number
			while (value > 0) {
				numBitsNeeded++;
				value = value >> 1;
			}
		}

		// varints only use 7 bits of the byte for the actual value
		int numVarintBytes = numBitsNeeded / 7;
		if (numBitsNeeded % 7 > 0) {
			numVarintBytes += 1;
		}

		return numVarintBytes;
	}

	/**
	 * Add a new user record to this existing aggregated record if there is
	 * enough space (based on the defined Kinesis limits for a PutRecord call).
	 * 
	 * @param partitionKey
	 *            The partition key of the new user record to add
	 * @param explicitHashKey
	 *            The explicit hash key of the new user record to add
	 * @param data
	 *            The raw data of the new user record to add
	 * @return True if the new user record was successfully added to this
	 *         aggregated record or false if this aggregated record is too full.
	 */
	public boolean addUserRecord(String partitionKey, String explicitHashKey, byte[] data) {
		// set the explicit hash key for the message to the partition key -
		// required for encoding
		explicitHashKey = explicitHashKey != null ? explicitHashKey : createExplicitHashKey(partitionKey);

		// validate values from the provided message
		validatePartitionKey(partitionKey);
		validateExplicitHashKey(explicitHashKey);
		validateData(data);

		// Validate new record size won't overflow max size for a
		// PutRecordRequest
		int sizeOfNewRecord = calculateRecordSize(partitionKey, explicitHashKey, data);
		if (getSizeBytes() + sizeOfNewRecord > MAX_BYTES_PER_RECORD) {
			return false;
		} else if (sizeOfNewRecord > MAX_BYTES_PER_RECORD) {
			throw new IllegalArgumentException(
					"Input record (PK=" + partitionKey + ", EHK=" + explicitHashKey + ", SizeBytes=" + sizeOfNewRecord
							+ ") is larger than the maximum size before Aggregation encoding of "
							+ (MAX_BYTES_PER_RECORD - AGGREGATION_OVERHEAD_BYTES) + " bytes");
		}

		Record.Builder newRecord = Record.newBuilder()
				.setData(data != null ? ByteString.copyFrom(data) : ByteString.EMPTY);

		ExistenceIndexPair pkAddResult = this.partitionKeys.add(partitionKey);
		if (pkAddResult.getFirst().booleanValue()) {
			this.aggregatedRecordBuilder.addPartitionKeyTable(partitionKey);
		}
		newRecord.setPartitionKeyIndex(pkAddResult.getSecond());

		ExistenceIndexPair ehkAddResult = this.explicitHashKeys.add(explicitHashKey);
		if (ehkAddResult.getFirst().booleanValue()) {
			this.aggregatedRecordBuilder.addExplicitHashKeyTable(explicitHashKey);
		}
		newRecord.setExplicitHashKeyIndex(ehkAddResult.getSecond());

		this.aggregatedMessageSizeBytes += sizeOfNewRecord;
		this.aggregatedRecordBuilder.addRecords(newRecord.build());

		// if this is the first record, we use its partition key and hash key
		// for the entire agg record
		if (this.aggregatedRecordBuilder.getRecordsCount() == 1) {
			this.aggPartitionKey = partitionKey;
			this.aggExplicitHashKey = explicitHashKey;
		}

		return true;
	}

	/**
	 * Convert the aggregated data in this record into a single
	 * PutRecordRequest. This method has no side effects (i.e. it will not clear
	 * the current contents of the aggregated record).
	 * 
	 * @param streamName
	 *            The Kinesis stream name where this PutRecordRequest will be
	 *            sent.
	 * @return A PutRecordRequest containing all the current data in this
	 *         aggregated record.
	 */
	public PutRecordRequest toPutRecordRequest(String streamName) {
		byte[] recordBytes = toRecordBytes();
		ByteBuffer bb = ByteBuffer.wrap(recordBytes);
		return new PutRecordRequest().withStreamName(streamName).withExplicitHashKey(getExplicitHashKey())
				.withPartitionKey(getPartitionKey()).withData(bb);
	}

	/**
	 * Convert the aggregated data in this record into a single
	 * PutRecordsRequestEntry. This method has no side effects (i.e. it will not
	 * clear the current contents of the aggregated record).
	 * 
	 * @return A PutRecordsRequestEntry containing all the current data in this
	 *         aggregated record that can be sent to Kinesis via a
	 *         PutRecordsRequest.
	 */
	public PutRecordsRequestEntry toPutRecordsRequestEntry() {
		return new PutRecordsRequestEntry().withExplicitHashKey(getExplicitHashKey())
				.withPartitionKey(getPartitionKey()).withData(ByteBuffer.wrap(toRecordBytes()));
	}

	/**
	 * Validate the data portion of an input Kinesis user record.
	 * 
	 * @param data
	 *            A byte array containing Kinesis user record data.
	 */
	private void validateData(final byte[] data) {
		final int maxAllowableDataLength = MAX_BYTES_PER_RECORD - AGGREGATED_RECORD_MAGIC.length
				- this.md5.getDigestLength();
		if (data != null && data.length > (maxAllowableDataLength)) {
			throw new IllegalArgumentException("Data must be less than or equal to " + maxAllowableDataLength
					+ " bytes in size, got " + data.length + " bytes");
		}
	}

	/**
	 * Validate the partition key of an input Kinesis user record.
	 * 
	 * @param partitionKey
	 *            The string containing the input partition key to validate.
	 */
	private void validatePartitionKey(final String partitionKey) {
		if (partitionKey == null) {
			throw new IllegalArgumentException("Partition key cannot be null");
		}

		if (partitionKey.length() < PARTITION_KEY_MIN_LENGTH || partitionKey.length() > PARTITION_KEY_MAX_LENGTH) {
			throw new IllegalArgumentException(
					"Invalid partition key. Length must be at least " + PARTITION_KEY_MIN_LENGTH + " and at most "
							+ PARTITION_KEY_MAX_LENGTH + ", got length of " + partitionKey.length());
		}

		try {
			partitionKey.getBytes(StandardCharsets.UTF_8);
		} catch (Exception e) {
			throw new IllegalArgumentException("Partition key must be valid " + StandardCharsets.UTF_8.displayName());
		}
	}

	/**
	 * Validate the explicit hash key of an input Kinesis user record.
	 * 
	 * @param explicitHashKey
	 *            The string containing the input explicit hash key to validate.
	 */
	private void validateExplicitHashKey(final String explicitHashKey) {
		if (explicitHashKey == null) {
			return;
		}

		BigInteger b = null;
		try {
			b = new BigInteger(explicitHashKey);
			if (b.compareTo(UINT_128_MAX) > 0 || b.compareTo(BigInteger.ZERO) < 0) {
				throw new IllegalArgumentException(
						"Invalid explicitHashKey, must be greater or equal to zero and less than or equal to (2^128 - 1), got "
								+ explicitHashKey);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid explicitHashKey, must be an integer, got " + explicitHashKey);
		}
	}

	/**
	 * Calculate a new explicit hash key based on the input partition key
	 * (following the algorithm from the original KPL).
	 * 
	 * @param partitionKey
	 *            The partition key to seed the new explicit hash key with
	 * @return An explicit hash key based on the input partition key generated
	 *         using an algorithm from the original KPL.
	 */
	private String createExplicitHashKey(final String partitionKey) {
		BigInteger hashKey = BigInteger.ZERO;

		this.md5.reset();
		byte[] pkDigest = this.md5.digest(partitionKey.getBytes(StandardCharsets.UTF_8));

		for (int i = 0; i < this.md5.getDigestLength(); i++) {
			BigInteger p = new BigInteger(String.valueOf((int) pkDigest[i] & 0xFF)); // convert
																						// to
																						// unsigned
																						// integer
			BigInteger shifted = p.shiftLeft((16 - i - 1) * 8);
			hashKey = hashKey.add(shifted);
		}

		return hashKey.toString(10);
	}

	/**
	 * A class for tracking unique partition keys or explicit hash keys for an
	 * aggregated Kinesis record. Also assists in keeping track of indexes for
	 * their locations in the protobuf tables.
	 */
	private class KeySet {
		/** The list of unique keys in this keyset. */
		private List<String> keys;
		/** A lookup mapping of key to index in protobuf table. */
		private Map<String, Long> lookup;

		/**
		 * Create a new empty keyset.
		 */
		public KeySet() {
			this.keys = new LinkedList<>();
			this.lookup = new TreeMap<>();
		}

		/**
		 * If the input key were added to this KeySet, determine what its
		 * resulting index would be.
		 * 
		 * @param s
		 *            The input string to potentially add to the KeySet
		 * 
		 * @return The table index that this string would occupy if it were
		 *         added to this KeySet.
		 */
		public Long getPotentialIndex(String s) {
			Long it = this.lookup.get(s);
			if (it != null) {
				return it;
			}

			return Long.valueOf(this.keys.size());
		}

		/**
		 * Add a new key to this keyset.
		 * 
		 * @param s
		 *            The key to add to the keyset.
		 * 
		 * @return A pair of <boolean,long>. The boolean is true if this key is
		 *         not already in this keyset, false otherwise. The long
		 *         indicates the index of the key.
		 */
		public ExistenceIndexPair add(String s) {
			Long it = this.lookup.get(s);
			if (it != null) {
				return new ExistenceIndexPair(false, it);
			}

			if (!this.lookup.containsKey(s)) {
				this.lookup.put(s, Long.valueOf(this.keys.size()));
			}

			this.keys.add(s);
			return new ExistenceIndexPair(true, Long.valueOf(this.keys.size() - 1));
		}

		/**
		 * @return True if this keyset contains the input key, false otherwise.
		 */
		public boolean contains(String s) {
			return s != null && this.lookup.containsKey(s);
		}

		/**
		 * Clear all the contents of this keyset.
		 */
		public void clear() {
			this.keys.clear();
			this.lookup.clear();
		}
	};

	/**
	 * A helper class for use with the KeySet that indicates whether or not a
	 * key exists in the KeySet and what its saved index would be.
	 */
	private class ExistenceIndexPair {
		private Boolean first;
		private Long second;

		public ExistenceIndexPair(Boolean first, Long second) {
			this.first = first;
			this.second = second;
		}

		public Boolean getFirst() {
			return this.first;
		}

		public Long getSecond() {
			return this.second;
		}
	}
}
