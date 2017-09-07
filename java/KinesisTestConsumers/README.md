# Kinesis Java Record Deaggregator Test Consumers

This project contains a sample AWS Lambda function that will read events from Kinesis and perform the necessary deaggregation of any aggregated records. It provides a few different examples of how to process Kinesis data in a Java-based AWS Lambda function which you can use as a template to build new applications on AWS Lambda that are capable of handling Kinesis aggregated records.

## Lambda Function

The code that provides our Lambda function is `KinesisLambdaReceiver.java`, which implements examples for the various deaggregation methods available in the associated `KinesisDeaggregator` project.  There are sample methods for processing aggregated records using stream-based, list-based and batch-based methods.

## Record Deaggregator

For more details on how to use the `RecordDeaggregator` object in these examples, see the `KinesisDeaggregator` module in this repository.

## Instructions for Use

1. Run Maven->Install to build the project
2. Create a new Lambda function in your AWS account
3. Skip blueprint selection
4. Choose Java 8 as the runtime
5. Choose the built file (from step #2) KinesisLambdaTestConsumers-1.0-lambda.jar as the code for the function (NOT the KinesisLambdaTestConsumers-1.0.jar file).
6. Choose com.amazonaws.KinesisLambdaReceiver as the Handler
7. Set the default batch size as required for your Kinesis stream throughput
8. Set the Role, Memory and Timeout appropriately.
9. Connect your new Lambda function to the Kinesis stream you'll be reading from

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