# Example Kinesis Producer Classes for Testing

## Runnable Classes
`SampleNormalProducer.java` - Uses a standard Kinesis PutRecords call to send 500 records to the Kinesis stream in a single batch using PutRecords

`SampleKPLProducer.java` - Uses the Kinesis Producer Library to send 500 records to the Kinesis stream as Aggregated Protobuf Encoded messages

## Run Instructions

1. Build the project with Maven: `mvn install`
2. The producers rely on DefaultAWSCredentialsProvider to set permissions.  You must supply them with credentials that have Put* access to the Kinesis stream you specify.  The easiest thing to do is pass in the following Java VM arguments with the proper values filled in:
```
-Daws.accessKeyId=<your_access_key>
-Daws.secretKey=<your_secret_key>
```
3.  Run either `java -cp target/KinesisTestProducers-1.0.0-dev.jar com.amazonaws.SampleNormalProducer` or `java -cp target/KinesisTestProducers-1.0.0-dev.jar com.amazonaws.SampleKPLProducer`

## Sample Record

The sample records sent by these applications look like this:

```
KPL 69 wajbxagglpzpbuxfoeoxznhwlrrxhsnnfjkprznvxedqfxpqucfbwaiudhkgzbzmdamsjkezcfrrredlfndbudldudfipzkariuzfbtkebwdqliankaqafzxs
```

The prefix will indicate which producer generated the record.

----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.