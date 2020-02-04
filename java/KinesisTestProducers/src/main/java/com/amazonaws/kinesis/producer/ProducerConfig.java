/**
 * Kinesis Aggregation/Deaggregation Libraries for Java
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
