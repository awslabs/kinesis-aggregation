package com.amazonaws.kinesis.agg;

import java.nio.ByteBuffer;

public interface KplAggregatorListener 
{
	public abstract void recordAvailable(final ByteBuffer recordBytes);
}
