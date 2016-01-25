/**
 * Kinesis Producer Library Deaggregation Examples for AWS Lambda/Java
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
package com.amazonaws;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

public class SampleNormalProducer {
	private static final Random RANDOM = new Random();
	private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
	private static final String TIMESTAMP = Long.toString(System
			.currentTimeMillis());
	private static final int DATA_SIZE = 128;
	private static final int TOTAL_RECORDS = 500;

	private static String randomExplicitHashKey() {
		return new BigInteger(128, RANDOM).toString(10);
	}

	private static ByteBuffer generateData(long sequenceNumber, int totalLen) {
		StringBuilder sb = new StringBuilder();
		sb.append("NORMAL ");
		sb.append(Long.toString(sequenceNumber));
		sb.append(" ");
		while (sb.length() < totalLen) {
			sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
		}
		sb.append("\n");

		return ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
	}

	private static AmazonKinesis getKinesisProducer(String region) {
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxConnections(1);
		config.setConnectionTimeout(60000);
		config.setSocketTimeout(60000);

		AmazonKinesis producer = new AmazonKinesisClient(
				new DefaultAWSCredentialsProviderChain(), config);
		producer.setRegion(Region.getRegion(Regions.fromName(region)));

		return producer;
	}

	public static void run(String streamName, String regionName)
			throws Exception {
		final AmazonKinesis producer = getKinesisProducer(regionName);

		System.out.println("Creating " + TOTAL_RECORDS + " records...");
		List<PutRecordsRequestEntry> entries = new LinkedList<>();
		for (int i = 1; i <= TOTAL_RECORDS; i++) {
			entries.add(new PutRecordsRequestEntry()
					.withPartitionKey(TIMESTAMP)
					.withExplicitHashKey(randomExplicitHashKey())
					.withData(generateData(i, DATA_SIZE)));
		}

		PutRecordsRequest request = new PutRecordsRequest()
				.withRecords(entries).withStreamName(streamName);

		System.out.println("Sending " + TOTAL_RECORDS + " records...");
		producer.putRecords(request);
		System.out.println("Complete.");
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception(
					"Usage SampleNormalProducer <stream name> <region>");
		} else {
			SampleNormalProducer.run(args[0], args[1]);
		}
	}
}
