/**
 * Kinesis Producer Library Aggregation/Deaggregation Examples for AWS Lambda/Java
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

public class KinesisLimits
{
	//Kinesis Limits
	//https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html
	//https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
	public static final long MAX_RECORDS_PER_PUT_RECORDS = 500;
	public static final long MAX_BYTES_PER_PUT_RECORDS = 5242880L; //5 MB (5 * 1024 * 1024)
	public static final long MAX_BYTES_PER_RECORD = 1048576L; //1 MB (1024 * 1024)
	public static final int PARTITION_KEY_MIN_LENGTH = 1;
	public static final int PARTITION_KEY_MAX_LENGTH = 256;
}
