# Kinesis Aggregation/Deaggregation Libraries for Python
#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
                "explicitHashKey": ehk,
                "data": base64.b64encode(data),
                "kinesisSchemaVersion": "1.0",
                "sequenceNumber": "49545115243490985018280067714973144582180062593244200961",
                "approximateArrivalTimestamp": 1518133507.063
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

    def test_single_user_record_as_str(self):

        input_pk = 'partition_key'
        input_data = 'abcdefghijklmnopqrstuvwxyz'

        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
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
        self.assertIsNone(intermediate_ehk, 'Calculated explicit hash key should be None.')

        event = create_kinesis_lambda_record(intermediate_pk, intermediate_ehk, intermediate_data)
        records = deagg.deaggregate_records(event['Records'])

        self.assertEqual(1, len(records))

        record = records[0]
        output_pk = record['kinesis']['partitionKey']
        output_ehk = record['kinesis']['explicitHashKey']
        output_data = base64.b64decode(record['kinesis']['data'])

        self.assertEqual(input_pk, output_pk,
                         'Input and output partition keys do not match.')
        self.assertIsNone(output_ehk, 'Output EHK should be None.')
        self.assertEqual(input_data, output_data.decode('utf-8'),
                         'Input and output record data does not match.')

    def test_single_user_record_as_bytes(self):

        input_pk = 'partition_key'
        input_data = b'abcdefghijklmnopqrstuvwxyz'

        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
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
        self.assertIsNone(intermediate_ehk, 'Calculated explicit hash key should be None.')

        event = create_kinesis_lambda_record(intermediate_pk, intermediate_ehk, intermediate_data)
        records = deagg.deaggregate_records(event['Records'])

        self.assertEqual(1, len(records))

        record = records[0]
        output_pk = record['kinesis']['partitionKey']
        output_ehk = record['kinesis']['explicitHashKey']
        output_data = base64.b64decode(record['kinesis']['data'])

        self.assertEqual(input_pk, output_pk,
                         'Input and output partition keys do not match.')
        self.assertIsNone(output_ehk, 'Output EHK should be None.')
        self.assertEqual(input_data, output_data,
                         'Input and output record data does not match.')

    def test_single_user_record_with_ehk(self):

        input_pk = 'partition_key'
        input_data = 'abcdefghijklmnopqrstuvwxyz'
        input_ehk = '339606600942967391854603552402021847292'

        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

        result = aggregator.add_user_record(partition_key=input_pk,
                                            data=input_data,
                                            explicit_hash_key=input_ehk)
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
        self.assertEqual(intermediate_ehk, input_ehk,
                         'Intermediate EHK and input EHK do not match.')

        event = create_kinesis_lambda_record(intermediate_pk, intermediate_ehk, intermediate_data)
        records = deagg.deaggregate_records(event['Records'])

        self.assertEqual(1, len(records))

        record = records[0]
        output_pk = record['kinesis']['partitionKey']
        output_ehk = record['kinesis']['explicitHashKey']
        output_data = base64.b64decode(record['kinesis']['data'])

        self.assertEqual(input_pk, output_pk,
                         'Input and output partition keys do not match.')
        self.assertEqual(input_ehk, output_ehk,
                         'Input and output explicit hash keys do not match.')
        self.assertEqual(input_data, output_data.decode('utf-8'),
                         'Input and output record data does not match.')

    def test_multiple_records(self):

        inputs = [
            ('partition_key1', 'abcdefghijklmnopqrstuvwxyz'),
            ('partition_key2', 'zyxwvutsrqponmlkjihgfedcba'),
            ('partition_key3', 'some_third_data_string')
        ]

        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

        for i in range(0, len(inputs)):
            result = aggregator.add_user_record(partition_key=inputs[i][0], data=inputs[i][1])
            if result is not None:
                self.fail('Agg record reporting as full when it should not.')
            self.assertEqual(i+1, aggregator.get_num_user_records(),
                             'New aggregator reported incorrect number of records.')

        agg_record = aggregator.clear_and_get()
        if not agg_record:
            self.fail('Failed to extract aggregated record.')

        self.assertEqual(len(inputs), agg_record.get_num_user_records(),
                         'Improper number of user records in agg record.')
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'Agg record is not empty after clear_and_get() call.')

        intermediate_pk, intermediate_ehk, intermediate_data = agg_record.get_contents()

        self.assertEqual(inputs[0][0], intermediate_pk, 'Intermediate PK and input PK do not match.')

        event = create_kinesis_lambda_record(intermediate_pk, intermediate_ehk, intermediate_data)
        records = deagg.deaggregate_records(event['Records'])

        self.assertEqual(len(inputs), len(records))

        for i in range(0, len(records)):
            record = records[i]
            output_pk = record['kinesis']['partitionKey']
            output_ehk = record['kinesis']['explicitHashKey']
            output_data = base64.b64decode(record['kinesis']['data'])

            self.assertEqual(inputs[i][0], output_pk,
                             'Input and output partition keys do not match.')
            self.assertIsNone(output_ehk,
                              'Explicit hash key should be None.')
            self.assertEqual(inputs[i][1], output_data.decode('utf-8'),
                             'Input and output record data does not match.')


if __name__ == '__main__':
    unittest.main()
