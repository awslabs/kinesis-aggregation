# Java Kinesis Producer Library Deaggregation Module

This set of examples uses the UserRecord deaggregation capability that is implemented within the [Java KCL](https://github.com/awslabs/amazon-kinesis-client), but as a standalone module that you can run within AWS Lambda. 

## Components

### Test Producers

The [KinesisTestProducers](KinesisTestProducers) project provides a simple way to inject some KPL aggregated data into a Stream, so that you can process it later. There is support for KPL and non-KPL producers.

### Lambda Consumers

The [KinesisLambdaTestConsumer](KinesisLambdaTestConsumer) project provides an example AWS Lambda Java project that shows how to use the `amazon-kinesis-client` UserRecord calss for deaggregation.

## Usage

To use this module, you create a Java Lambda function as normal, and then instantiate a [KplDeaggregator](KinesisLambdaTestConsumer/src/main/java/com/amazonaws/KplDeaggregator.java) instance. With this instance, you can use either a Java 8 Streams API function, or you can also perform deaggregation with the List API.

### Streams

With the Streams API approach, you provide a `Stream<KinesisEventRecord>` item, which can be created with the Lambda item `event.getRecords().stream()`. You also then provide a `Consumer<UserRecord>` instance to process the data. In the example, this is implemented as:


```
		// Stream the User Records from the Lambda Event
		deaggregator.stream(event.getRecords().stream(), userRecord -> {
			// Your User Record Processing Code Here!
				logger.log(String.format("Processing UserRecord %s (%s:%s)",
						userRecord.getPartitionKey(),
						userRecord.getSequenceNumber(),
						userRecord.getSubSequenceNumber()));
			});
```

For this interface, the Lambda handler will be specified as `com.amazonaws.KinesisLambdaReceiver`.

### Lists

If you'd prefer to work with Java Lists rather than Streams, you can also call the deaggregation interface `processRecords`. In this model you specify a `List<KinesisEventRecord>`, and an implementation of a `KplDeaggregator.KinesisUserRecordProcessor`, which implements the interface:

```
public Void process(List<UserRecord> userRecords)
```

For example:

```
deaggregator.processRecords(event.getRecords(),
					new KplDeaggregator.KinesisUserRecordProcessor() {
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
```

For this interface, the Lambda handler will be specified as `com.amazonaws.KinesisLambdaReceiver::handleRequestsWithLists`.

## Build & Deploy

This project is provided as a Maven Module, so you can just execute a top level `maven install` and the producers and consumers will be built into their respective `target` directories, or you can alternatively just deploy the artefacts provided in the project `dist` directories:

Producers: [`KinesisTestProducers-1.0.0-dev.jar`](KinesisTestProducers/dist/KinesisTestProducers-1.0.0-dev.jar)

Consumers: [`KinesisLambdaTestConsumer-1.0.0-dev.jar`](KinesisLambdaTestConsumer/dist/KinesisLambdaTestConsumer-1.0.0-dev.jar)
 
----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.