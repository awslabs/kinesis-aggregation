package com.amazonaws.kinesis.deagg;

import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

/**
 * Interface used by a calling method to call the process function
 *
 */
public interface KinesisUserRecordProcessor 
{
	public Void process(List<UserRecord> userRecords);
}
