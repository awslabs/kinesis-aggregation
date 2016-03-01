package com.amazonaws.kinesis.producer;

public class ProducerConfig 
{
	public static final String RECORD_TIMESTAMP = Long.toString(System.currentTimeMillis());
	public static final int RECORD_SIZE = 128;
	public static final int RECORDS_TO_TRANSMIT = 25;
}
