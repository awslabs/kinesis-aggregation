

## Build & Deploy a Lambda Function to process Kinesis Records

One easy way to get started processing Kinesis Data is to use AWS Lambda. By extending the [index.js](index.js) file, you can take advantage of KPL deaggregation features without having to write the boilerplate code. To do this, fork the GitHub codebase to a new project, select whether you want to build on the exampleSync or exampleAsync interfaces, and write your Kinesis processing code as normal. You can use ```node test.js``` to test your code (including both aggregated protobuf formatted data, as well as non-aggregated plain Kinesis records). Give your function a name and version number in [package.json](package.json) and then when you are ready to run from AWS Lambda, use:

```./build.js```

This will build your code, with the required dependencies, as ```package.json.name```-```package.json.version```.zip. You can then configure AWS Lambda as normal with handler name ```index.example(A)Sync``` or with the new name you added in your code. You can continue to modify and test locally, and push new versions directly to AWS Lambda by using:

```./build.js true```

which requires a local install of the [AWS Command Line Interface](https://aws.amazon.com/cli) and which invokes ```aws lambda upload-function-code``` directly. When you are finally happy with your AWS Lambda module, consider changing ```common.js.debug``` to false to reduce the amount of output messages generated in CloudWatch Logging.