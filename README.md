# Kinesis Producer Library Aggregation & Deaggregation Modules for AWS Lambda

The Amazon Kinesis Producer Library (KPL) gives you the ability to write data to Amazon Kinesis with a highly efficient, asyncronous delivery model that can improve performance. The KPL is extremely powerful, but is currently only available as a Java API wrapper around a C++ executable which may not be suitable for all deployment environments. Similarly, the powerful Kinesis Client Library (KCL) provides automatic deaggregation of KPL aggregated records, but not all Kinesis consumer applications, such as those running on AWS Lambda, are currently capable of leveraging this deaggregation capability.

![KPL Message Format](kpl-message-format.png)

The components in this project give you the ability to process and create KPL compatible serialised data within AWS Lambda, in Java, Node.js and Python. These components can also be used as part of the Kinesis Client Library a [multi-lang KCL application](https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/multilang/package-info.java). 

## Aggregation

One of the main advantages of the KPL is its ability to use record aggregation to increase payload size and improve throughput. While it is not a replacement for the full power of the KPL, this library gives you the ability to easily and efficiently aggregate multiple user records into larger aggregated records that make more efficient use of available bandwidth and reduce cost. This data is encoded using Google Protocol Buffers, and returned to the calling function for subsequent use. You can then publish to Kinesis and the data is compatible with consumers using the KCL or these Deaggregation modules.

![Processing Model](aggregation.png)

## Deaggregation

The components in this library allow you to efficiently deaggregate protocol buffer encoded aggregated records in any application, including AWS Lambda.
 
![Processing Model](processing.png)

### Language Specific Implementations

AWS Lambda supports Java, Node.js and Python as programming languages. We have included support for those languages so that you can create and process UserRecords via standalone modules. Documentation is provided for each language:

| Language | Location |
:--- | :--- 
| Java | [java](java/) |
| Node.js Javascript | [node.js](node/) |
| Python | [python](python/) |

----

Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

	http://aws.amazon.com/asl/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License.