# Go Kinesis Deaggregation Modules

The Kinesis Deaggregation Modules for Go provide the ability to do in-memory deaggregation of standard Kinesis user records using the [Kinesis Aggregated Record Format](https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md) to allow for more efficient transmission of records.

There are 2 versions based upon the AWS SDK version you are using:

| SDK | Project |
| --- | ------- |
|Version 0 | [v0](.) |
|Version 2 | [v2](v2) |

## Installation

Version 0 depends on `github.com/aws/aws-sdk-go`, in order to install
```
go get github.com/awslabs/kinesis-aggregation/go
```

Version 2 depends on `github.com/aws/aws-sdk-go-v2`, in order to install

```
go get github.com/awslabs/kinesis-aggregation/go/v2
```
