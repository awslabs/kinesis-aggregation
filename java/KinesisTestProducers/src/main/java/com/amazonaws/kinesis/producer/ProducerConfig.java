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
package com.amazonaws.kinesis.producer;

/**
 * A helper class for configuring the Kinesis sample producer behavior.
 */
public class ProducerConfig
{
    /** The size of each record that is transmitted. */
    public static final int RECORD_SIZE_BYTES = 1024;
    
    /** The number of records to send per application run. */
    public static final int RECORDS_TO_TRANSMIT = 1024;
}
