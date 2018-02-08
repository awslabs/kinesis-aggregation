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

# See https://docs.aws.amazon.com/lambda/latest/dg/eventsources.html#eventsources-kinesis-streams
# for where the structure of this record comes from
def create_kinesis_lambda_record(pk, ehk, data):

    return {
        "Records": [
        {
          "eventID": "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
          "eventVersion": "1.0",
          "kinesis": {
            "partitionKey": pk,
            "explicitHashKey" : ehk,
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

class EndToEndTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # TODO: Have a separate test for passing in bytes directly

    def test_single_user_record_as_str(self):

        input_pk = 'partition_key'
        input_data = 'abcdefghijklmnopqrstuvwxyz'

        aggregator = agg.RecordAggregator()
        self.assertEqual(0,aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

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

        intermediate_pk, intermediate_ehk, intermediate_data = agg_record.get_contents()

        self.assertEqual(input_pk, intermediate_pk,
                         'Intermediate PK and input PK do not match.')

        # No input EHK is specified, but calculating one should be deterministic based on PK
        self.assertEqual('166672149269223421180636453361785199783724579696249764508579298334598714607',
                         intermediate_ehk,
                         'Calculated explicit hash key does not match.')

        # NOTE: intermediate_data is a fully aggregated record, not just the raw input data at this point

        event = create_kinesis_lambda_record(intermediate_pk, intermediate_ehk, intermediate_data)
        records = deagg.deaggregate_records(event['Records'])

        self.assertEqual(1, len(records))

        record = records[0]
        output_pk = record['kinesis']['partitionKey']
        output_ehk = record['kinesis']['explicitHashKey']
        output_data = base64.b64decode(record['kinesis']['data'])

        self.assertEqual(input_pk, output_pk,
                         'Input and output partition keys do not match.')
        self.assertEqual(intermediate_ehk, output_ehk,
                         'Intermediate and output EHK do not match.')
        self.assertEqual(input_data, output_data.decode('utf-8'),
                         'Input and output record data does not match.')

    def test_single_user_record_as_bytes(self):

        input_pk = 'partition_key'
        input_data = b'abcdefghijklmnopqrstuvwxyz'

        aggregator = agg.RecordAggregator()
        self.assertEqual(0,aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

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

        intermediate_pk, intermediate_ehk, intermediate_data = agg_record.get_contents()

        self.assertEqual(input_pk, intermediate_pk,
                         'Intermediate PK and input PK do not match.')

        # No input EHK is specified, but calculating one should be deterministic based on PK
        self.assertEqual('166672149269223421180636453361785199783724579696249764508579298334598714607',
                         intermediate_ehk,
                         'Calculated explicit hash key does not match.')

        # NOTE: intermediate_data is a fully aggregated record, not just the raw input data at this point

        event = create_kinesis_lambda_record(intermediate_pk, intermediate_ehk, intermediate_data)
        records = deagg.deaggregate_records(event['Records'])

        self.assertEqual(1, len(records))

        record = records[0]
        output_pk = record['kinesis']['partitionKey']
        output_ehk = record['kinesis']['explicitHashKey']
        output_data = base64.b64decode(record['kinesis']['data'])

        self.assertEqual(input_pk, output_pk,
                         'Input and output partition keys do not match.')
        self.assertEqual(intermediate_ehk, output_ehk,
                         'Intermediate and output EHK do not match.')
        self.assertEqual(input_data, output_data,
                         'Input and output record data does not match.')


if __name__ == '__main__':
    unittest.main()
