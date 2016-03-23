# Kinesis Producer Library Compatible Java Record Deaggregator

This library provides a set of convenience functions to perform record deaggregatoin that is compatible with the Google Protobuf format used by the Kinesis Producer Library or the KPL Aggregator module.  This module can be used in any Java-based application that receives aggregated Kinesis records, including applications running on AWS Lambda.

## KPL Deaggregator

The KPL Deaggregator is the class that does the work of extracting KPL encoded UserRecords from our Kinesis Records. In both our functions we create a new instance of the class, and then provide it the code to run on each extracted UserRecord. For example, using Java 8 Streams:

```
        deaggregator.stream(
                event.getRecords().stream(),
                userRecord -> {
                    // Your User Record Processing Code Here!
                    logger.log(String.format("Processing UserRecord %s (%s:%s)",
                            userRecord.getPartitionKey(),
                            userRecord.getSequenceNumber(),
                            userRecord.getSubSequenceNumber()));
                });
```

In this invocation, we are extracting the Kinesis Records from the Event provided by AWS Lambda, and converting them to a Stream. We then provide a lambda function which iterates over the extracted user records, and you can type in your own logic instead of the `logger.log()` call.

Using Lists rather than Java Streams, this same funcitonality can be provided by implementing the `KplDeaggregator.KinesisUserRecordProcessor` interface:

```
        try {
            // process the user records with an anonymous record processor
            // instance
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
        } catch (Exception e) {
            logger.log(e.getMessage());
        }
```

## Batch Deaggregation

For those whole prefer simple method call and response mechanisms, the `KplDeaggregator` also provides two static `deaggregate` methods that either take in a single aggregated Kinesis record or a list of aggregated Kinesis records and deaggregate them synchronously in bulk.

For example:

```
try
{
    List<UserRecord> userRecords = KplDeaggregator.deaggregate(event.getRecords());
    for (UserRecord userRecord : userRecords)
    {
        // Your User Record Processing Code Here!
        logger.log(String.format("Processing UserRecord %s (%s:%s)", 
                                    userRecord.getPartitionKey(), 
                                    userRecord.getSequenceNumber(),
                                    userRecord.getSubSequenceNumber()));
    }
}
catch (Exception e)
{
    logger.log(e.getMessage());
}
```

## Instructions for Use

1. Run Maven->Install to build the project
2. Create a new Lambda function in your AWS account
3. Skip blueprint selection
4. Choose Java 8 as the runtime
5. Choose the built file (from step #2) KinesisLambdaTestConsumer-1.0-dev.jar as the code for the function (NOT the KinesisLambdaTestConsumer-0.0.1.jar file).
6. Choose com.amazonaws.KinesisLambdaReceiver as the Handler
7. Set the default batch size as required for your Stream throughput
8. Set the Role, Memory and Timeout appropriately.
9. Connect your new Lambda function to the Kinesis stream you'll be publishing to

## IAM Role

This is a sample IAM policy for the Lambda execution role:

```
 {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": [
        "*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetRecords",
        "kinesis:GetShardIterator",
        "kinesis:DescribeStream",
        "kinesis:ListStreams",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.