# Kinesis Java Record Aggregator

This library provides a set of convenience functions to perform in-memory record aggregation that is compatible with the same [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) used by the full Kinesis Producer Library (KPL).  The focus of this module is purely record aggregation though; if you want the load balancing and other useful features of the KPL, you will still need to leverage the full Kinesis Producer Library.

## Record Aggregation

The `RecordAggregator` is the class that does the work of accepting individual Kinesis user records and aggregating them into a single aggregated Kinesis record (using the same Google Protobuf format as the full Kinesis Producer Library). 

The `RecordAggregator` class provides two interfaces for aggregating records: batch-based and callback-based.

### Batch-based Aggregation

The batch-based aggregation method involves adding records one at a time to the `RecordAggregator` and checking the response to determine when a full aggregated record is available.  The `addUserRecord` method returns `null` when there is room for more records in the existing aggregated record or it returns an `AggRecord` object when a full record is available for transmission.

A sample implementation of batch-based aggregation is shown below.

```
for (int i = 0; i < numRecordsToTransmit; i++)
{
    String pk = /* get record partition key */;
    String ehk = /* get record explicit hash key */;
    byte[] data = /* get record data */;

    AggRecord aggRecord = aggregator.addUserRecord(pk, ehk, data);
    if (aggRecord != null)
    {
        ForkJoinPool.commonPool().execute(() ->
        {
            kinesisClient.putRecord(aggRecord.toPutRecordRequest("myStreamName"));
        });
    }
}
```

The `ForkJoinPool.commonPool().execute()` method above executes the actual transmission to Amazon Kinesis on a separate thread from the Java 8 shared `ForkJoinPool`. 

You can find a full working sample of batch-based aggregation in the `SampleAggregatorProducer.java` class in the `KinesisTestProducers` project.

### Callback-based Aggregation

For those that prefer more asynchronous programming models, the callback-based aggregation method involves registering a callback function (which can be a Java 8 lambda function) via the `onRecordComplete` function that will be notified when an aggregated record is available.

A sample implementation of callback-based aggregation is shown below.

```
aggregator.onRecordComplete((aggRecord) ->
{
    kinesisClient.putRecord(aggRecord.toPutRecordRequest("myStreamName"));
});

for (int i = 0; i <= numRecordsToTransmit; i++)
{
    String pk = /* get record partition key */;
    String ehk = /* get record explicit hash key */;
    byte[] data = /* get record data */;
    
   aggregator.addUserRecord(pk, ehk, data);
}
```

By default, the `RecordAggregator` will use a new thread from the Java 8 shared `ForkJoinPool` to execute the callback function, but you may also supply your own `ExecutorService` to the `onRecordComplete` method if you want tighter control over the thread pool being used.

You can find a full working sample of batch-based aggregation in the `SampleAggregatorProducer.java` class in the `KinesisTestProducers` project.

### Other Implementation Details

When using the batch-based and callback-based aggregation methods, it is important to note that you're only given an `AggRecord` object (via return value or callback) when the `RecordAggregator` object has a full record (i.e. as close to the 1MB PutRecord limit as possible).  There are certain scenarios, however, where you want to be able to flush records to Kinesis before the aggregated record is 100% full.  Some example scenarios include flushing records at application shutdown or making sure that records get flushed every N minutes.

To solve this problem, the `RecordAggregator` object provides a method called `clearAndGet` that will return an aggregated record that contains all the existing records in the `RecordAggregator` as a `AggRecord` object (even if it's not completely full).

Record aggregation works by providing lists of the partition and explicit hash keys that index a table of records. This list indexing has an overhead, which we have determined is approximately 256 bytes. Records which exceed the Kinesis maximum record size of 1MB minus this encoding overhead will be rejected and throw an `IllegalArgumentException` on the call to `RecordAggregator.addUserRecord()`.


----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.
