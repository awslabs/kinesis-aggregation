# Kinesis Java Record Deaggregator

This library provides a set of convenience functions to perform in-memory record deaggregation that is compatible with the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) used by the Kinesis Producer Library (KPL) and the KinesisAggregator module. This module can be used in any Java-based application that receives aggregated Kinesis records, including applications running on AWS Lambda.

## Record Deaggregation

The `RecordDeaggregator` is the class that does the work of extracting individual Kinesis user records from aggregated Kinesis Records received by AWS Lambda or directly through the Kinesis Java SDK. This class provide multiple ways to deaggregate records: stream-based, list-based, batch-based and single record.

### Creating a Deaggregator

There are two supported base classes that can be used for Deaggregation, `com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord` and `com.amazonaws.services.kinesis.model.Record`. These support Lambda based access, and Kinesis SDK access respectively. Use of any other base class will throw an `InvalidArgumentsException`.  

This project uses Java Generics to handle these different types correctly. To create a Lambda compliant Deaggregator, use:

```
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
...
RecordDeaggregator<KinesisEventRecord> deaggregator = new RecordDeaggregator<>();
```

and for the Kinesis SDK:

```
import com.amazonaws.services.kinesis.model.Record;
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
                userRecord.getPartitionKey(),
                userRecord.getSequenceNumber(),
                userRecord.getSubSequenceNumber()));
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
                public Void process(List<UserRecord> userRecords) {
                    for (UserRecord userRecord : userRecords) {
                        // Your User Record Processing Code Here!
                        logger.log(String.format(
                                "Processing UserRecord %s (%s:%s)",
                                userRecord.getPartitionKey(),
                                userRecord.getSequenceNumber(),
                                userRecord.getSubSequenceNumber()));
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
    List<UserRecord> userRecords = deaggregator.deaggregate(event.getRecords());
    for (UserRecord userRecord : userRecords) {
        // Your User Record Processing Code Here!
        logger.log(String.format("Processing UserRecord %s (%s:%s)", 
                                    userRecord.getPartitionKey(), 
                                    userRecord.getSequenceNumber(),
                                    userRecord.getSubSequenceNumber()));
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
    List<UserRecord> userRecords = deaggregator.deaggregate(singleRecord);
    for (UserRecord userRecord : userRecords) {
        // Your User Record Processing Code Here!
        logger.log(String.format("Processing UserRecord %s (%s:%s)", 
                                    userRecord.getPartitionKey(), 
                                    userRecord.getSequenceNumber(),
                                    userRecord.getSubSequenceNumber()));
    }
} catch (Exception e) {
    logger.log(e.getMessage());
}
```

As with the previous example, you should provide your own application-specific logic in place of the provided `logger.log()` call.

### Handling Non-Aggregated Records

The record deaggregation methods in `RecordDeaggregator` can handle both records in the standard Kinesis aggregated record format as well as Kinesis records in arbitrary user-defined formats.  If you pass records to the `RecordDeaggregator` that follow the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md), they will be deaggregated into one or more Kinesis user records per the encoding rules.  If you pass records to the `RecordDeaggregator` that are not actually aggregated records, they will be returned unchanged as Kinesis user records.  You may also mix aggregated and non-aggregated records in the same deaggregation call.

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
