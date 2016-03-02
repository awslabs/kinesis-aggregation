package com.amazonaws.kinesis.agg;

import java.nio.ByteBuffer;

public interface KplAggregatorListener 
{
	public abstract void recordComplete(String streamName, String paritionKey, 
										 String explicitHashKey, ByteBuffer data);
}
