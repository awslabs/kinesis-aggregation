package com.amazonaws.kinesis.producer;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;

public class ProducerUtils
{
	//Use this is you want to send the same records every time (useful for testing)
	//private static final Random RANDOM = new Random(0); 
	//Use this to send random records
	private static final Random RANDOM = new Random();
	
	private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyz";
	
	public static String randomExplicitHashKey() 
	{
		return new BigInteger(128, RANDOM).toString(10);
	}

	public static byte[] generateData(long sequenceNumber, int totalLen) 
	{
		StringBuilder sb = new StringBuilder();
		sb.append("RECORD ");
		sb.append(Long.toString(sequenceNumber));
		sb.append(" ");
		while (sb.length() < totalLen)
		{
			sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
		}
		sb.append("\n");

		return sb.toString().getBytes(StandardCharsets.UTF_8);
	}

	public static AmazonKinesis getKinesisProducer(String region) 
	{
		ClientConfiguration config = new ClientConfiguration();
		config.setMaxConnections(1);
		config.setConnectionTimeout(60000);
		config.setSocketTimeout(60000);

		AmazonKinesis producer = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain(), config);
		producer.setRegion(Region.getRegion(Regions.fromName(region)));

		return producer;
	}
}
