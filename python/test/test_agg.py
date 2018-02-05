# Kinesis Aggregation/Deaggregation Libraries for Python
#
# Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

from __future__ import print_function
import aws_kinesis_agg.aggregator as agg
import aws_kinesis_agg.deaggregator as deagg
import base64
import unittest

ALPHABET = b'abcdefghijklmnopqrstuvwxyz'

# See https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-kinesis-streams
def create_kinesis_lambda_record(pk, data):

    return {
        "Records": [
        {
          "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
          "eventVersion": "1.0",
          "kinesis": {
            "partitionKey": pk,
            "data": base64.b64encode(data),
            "kinesisSchemaVersion": "1.0",
            "sequenceNumber": "49545115243490985018280067714973144582180062593244200961"
          },
          "invokeIdentityArn": 'identity-arn',
          "eventName": "aws:kinesis:record",
          "eventSourceARN": 'kinesis-event-arn',
          "eventSource": "aws:kinesis",
          "awsRegion": "us-east-1"
        }
      ]
    }

class RecordAggregatorTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single_user_record_as_string(self):

        # TODO:
        # API defines PK as a string and data as blob
        # https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html
        #
        # We should follow that convention

        input_pk = 'partition_key'
        input_data = ALPHABET

        aggregator = agg.RecordAggregator()
        self.assertEqual(0,aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

        # TODO: Have a separate test for passing in bytes directly

        result = aggregator.add_user_record(input_pk, input_data)
        if result is not None:
            self.fail('Agg record reporting as full when it should not.')

        agg_record = aggregator.clear_and_get()
        if not agg_record:
            self.fail('Failed to extract aggregated record.')

        self.assertEqual(1, agg_record.get_num_user_records(),
                         'Improper number of user records in agg record.')
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'Agg record is not empty after clear_and_get() call.')

        pk, ehk, data = agg_record.get_contents()

        print('PK = %s' % pk)
        print('EHK = %s' % ehk)
        print('Data = %s' % data)

        # self.assertEqual(input_pk, pk, 'Partition key mismatch.')

        event = create_kinesis_lambda_record(pk, data)
        records = deagg.deaggregate_records(event['Records'])

        for record in records:
            print('Record = %s' % record)

        self.assertEqual(1, len(records))

        record = records[0]
        output_pk = record['kinesis']['partitionKey']

        output_data = record['kinesis']['data']
        output_data = base64.b64decode(output_data)

        self.assertEqual(input_pk, output_pk,
                         'Input and output partition keys do not match.')
        self.assertEqual(input_data, output_data,
                         'Input and output record data does not match.')

    def test_single_user_record_as_bytes(self):

        pass

if __name__ == '__main__':
    unittest.main()
