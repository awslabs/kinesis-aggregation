package com.amazonaws.kinesis.producer;

import java.util.List;

import com.amazonaws.kinesis.agg.KplBatchAggregator;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

public class SampleKinesisBatchAggregator
{
	public static void main(String[] args)
	{
		if (args.length != 2) 
		{
			System.err.println("Usage SampleKinesisAggregator <stream name> <region>");
			System.exit(1);
		} 
		
		String streamName = args[0];
		String regionName = args[1];
		
		AmazonKinesis producer = ProducerUtils.getKinesisProducer(regionName);
		KplBatchAggregator aggregator = new KplBatchAggregator(streamName);
		
		System.out.println("Creating " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for (int i = 1; i <= ProducerConfig.RECORDS_TO_TRANSMIT; i++)
		{
			byte[] data = ProducerUtils.generateData(i, ProducerConfig.RECORD_SIZE);
			//System.out.println(new String(data));
			aggregator.addUserRecord(ProducerConfig.RECORD_TIMESTAMP, ProducerUtils.randomExplicitHashKey(), data);
		}

		List<PutRecordRequest> requests = aggregator.extractPutRecordRequests();
		
		System.out.println("Sending " + ProducerConfig.RECORDS_TO_TRANSMIT + " records...");
		for(PutRecordRequest request : requests)
		{
			producer.putRecord(request);
		}
		System.out.println("Complete.");
	}
}
