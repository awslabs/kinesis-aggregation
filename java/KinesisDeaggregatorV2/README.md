# Kinesis Java Record Deaggregator for AWS V2 SDK's

This library provides a set of convenience functions to perform in-memory record deaggregation that is compatible with the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) used by the Kinesis Producer Library (KPL) and the KinesisAggregator module. This module can be used in any Java-based application that receives aggregated Kinesis records, including applications running on AWS Lambda.

This module is compatible with the V2 AWS SDK's.

## Record Deaggregation

The `RecordDeaggregator` is the class that does the work of extracting individual Kinesis user records from aggregated Kinesis Records received by AWS Lambda or directly through the Kinesis Java SDK. This class provide multiple ways to deaggregate records: stream-based, list-based, batch-based and single record.

### Creating a Deaggregator

There are two supported base classes that can be used for Deaggregation, `com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord` and `software.amazon.awssdk.services.kinesis.model.Record`. These support Lambda based access, and Kinesis V2 SDK access respectively. Use of any other base class will throw an `InvalidArgumentsException`.  

This project uses Java Generics to handle these different types correctly. To create a Lambda compliant Deaggregator, use:

```
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
...
RecordDeaggregator<KinesisEventRecord> deaggregator = new RecordDeaggregator<>();
```

and for the Kinesis SDK:

```
import software.amazon.awssdk.services.kinesis.model.Record;
...
RecordDeaggregator<Record> deaggregator = new RecordDeaggregator<>();
```

### Stream-based Deaggregation

The following examples demonstrate functions to create a new instance of the `RecordDeaggregator` class and then provide it code to run on each extracted UserRecord. For example, using Java 8 Streams:

```
deaggregator.stream(
    event.getRecords().stream(),
    userRecord -> {
        // Your User Record Processing Code Here!
        logger.log(String.format("Processing UserRecord %s (%s:%s)",
                userRecord.partitionKey(),
                userRecord.sequenceNumber(),
                userRecord.subSequenceNumber()));
    }
);
```

In this invocation, we are extracting the KinesisEventRecords from the Event provided by AWS Lambda, and converting them to a Stream. We then provide a lambda function which iterates over the extracted user records.  You should provide your own application-specific logic in place of the provided `logger.log()` call.

### List-based Deaggregation

You can also achieve the same functionality using Lists rather than Java Streams via the `RecordDeaggregator.KinesisUserRecordProcessor` interface:

```
try {
    // process the user records with an anonymous record processor
    // instance
    deaggregator.processRecords(event.getRecords(),
            new RecordDeaggregator.KinesisUserRecordProcessor() {
                public Void process(List<KinesisClientRecord> userRecords) {
                    for (KinesisClientRecord userRecord : userRecords) {
                        // Your User Record Processing Code Here!
                        logger.log(String.format(
                                "Processing UserRecord %s (%s:%s)",
                                userRecord.partitionKey(),
                                userRecord.sequenceNumber(),
                                userRecord.subSequenceNumber()));
                    }

                    return null;
                }
            });
} catch (Exception e) {
    logger.log(e.getMessage());
}
```

As with the previous example, you should provide your own application-specific logic in place of the provided `logger.log()` call.

### Batch-based Deaggregation

For those whole prefer simple method call and response mechanisms, the `RecordDeaggregator` provides a `deaggregate` method that takes in a list of aggregated Kinesis records and deaggregates them synchronously in bulk. For example:

```
try {
    List<KinesisClientRecord> userRecords = deaggregator.deaggregate(event.getRecords());
    for (KinesisClientRecord userRecord : userRecords) {
        // Your User Record Processing Code Here!
        logger.log(String.format("Processing KinesisClientRecord %s (%s:%s)", 
                                    userRecord.partitionKey(), 
                                    userRecord.sequenceNumber(),
                                    userRecord.subSequenceNumber()));
    }
} catch (Exception e) {
    logger.log(e.getMessage());
}
```

As with the previous example, you should provide your own application-specific logic in place of the provided `logger.log()` call.

### Single Record Deaggregation

In some cases, it can also be beneficial to be able to deaggregate a single Kinesis aggregated record at a time.  The `RecordDeaggregator` provides a single static `deaggregate` method that takes in a single aggregated Kinesis record, deaggregates it and returns one or more Kinesis user records as a result.  For example:

```
KinesisEventRecord singleRecord = ...;
try {
    List<KinesisClientRecord> userRecords = deaggregator.deaggregate(singleRecord);
    for (KinesisClientRecord userRecord : userRecords) {
        // Your User Record Processing Code Here!
        logger.log(String.format("Processing UserRecord %s (%s:%s)", 
                                    userRecord.partitionKey(), 
                                    userRecord.pequenceNumber(),
                                    userRecord.subSequenceNumber()));
    }
} catch (Exception e) {
    logger.log(e.getMessage());
}
```

As with the previous example, you should provide your own application-specific logic in place of the provided `logger.log()` call.

### Handling Non-Aggregated Records

The record deaggregation methods in `RecordDeaggregator` can handle both records in the standard Kinesis aggregated record format as well as Kinesis records in arbitrary user-defined formats.  If you pass records to the `RecordDeaggregator` that follow the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md), they will be deaggregated into one or more Kinesis user records per the encoding rules.  If you pass records to the `RecordDeaggregator` that are not actually aggregated records, they will be returned unchanged as Kinesis user records.  You may also mix aggregated and non-aggregated records in the same deaggregation call.

## Sample Code

This project includes a set of sample code to help you create a Lambda function that leverages deaggregation. Both of the below contents are provided in the `src/sample/java` folder.

### EchoHandler.java

```
package com.amazonaws.kinesis.deagg;

import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

import software.amazon.kinesis.retrieval.KinesisClientRecord;

public class EchoHandler implements RequestHandler<KinesisEvent, Void> {

	@Override
	public Void handleRequest(KinesisEvent event, Context context) {
		LambdaLogger logger = context.getLogger();

		// extract the records from the event
		List<KinesisEventRecord> records = event.getRecords();

		logger.log(String.format("Recieved %s Raw Records", records.size()));

		try {
			// now deaggregate the message contents
			List<KinesisClientRecord> deaggregated = new RecordDeaggregator<KinesisEventRecord>().deaggregate(records);
			logger.log(String.format("Received %s Deaggregated User Records", deaggregated.size()));

			deaggregated.stream().forEachOrdered(rec -> {
				logger.log(rec.partitionKey());
			});
		} catch (Exception e) {
			logger.log(e.getMessage());
		}

		return null;
	}
}
```

This class will output the size of the received batch from Kinesis, and then deaggregate the user records and output the count of those records, along with each Partition Key recieved.

If you would like to test this functionality, create a new Java 8 Lambda function with the above code and required dependencies. You can then use the below TestEvent to show the functionality of the deaggregating Lambda:

### SampleLambdaEvent.json

```
{
  "Records": [
    {
      "kinesis": {
        "partitionKey": "partitionKey-03",
        "kinesisSchemaVersion": "1.0",
        "data": "84mawgoDYWJjEicxOTE0MTU2NTgzNDQxNTg3NjYxNjgwMzE0NzMyNzc5MjI4MDM1NzAaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2JhGiAIABAAGhphYmNkZWZnaGlqa2xtbm9wcXJzdHV2d3h5ehogCAAQABoaenl4d3Z1dHNycXBvbm1sa2ppaGdmZWRjYmEaIAgAEAAaGmFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6GiAIABAAGhp6eXh3dnV0c3JxcG9ubWxramloZ2ZlZGNiYRogCAAQABoaYWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoaIAgAEAAaGnp5eHd2dXRzcnFwb25tbGtqaWhnZmVkY2Jh0I8WvwEDJiGD4YsiKIfUOw==",
        "sequenceNumber": "49545115243490985018280067714973144582180062593244200961",
        "approximateArrivalTimestamp": 1428537600
      },
      "eventSource": "aws:kinesis",
      "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
      "invokeIdentityArn": "arn:aws:iam::EXAMPLE",
      "eventVersion": "1.0",
      "eventName": "aws:kinesis:record",
      "eventSourceARN": "arn:aws:kinesis:EXAMPLE",
      "awsRegion": "us-east-1"
    }
  ]
}
```

This file contains an event that simulates an Aggregated Kinesis Event enclosing 100 User Records. The payload of this message is alternating lower case alpha in forward, then backward order.

----

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
