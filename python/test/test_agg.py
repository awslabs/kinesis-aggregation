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
from aws_kinesis_agg import MAX_BYTES_PER_RECORD
import aws_kinesis_agg.aggregator as agg
import base64
import unittest

# This is an aggregated record generated by the actual KPL library (with explicitly specified EHK values)
# (see com.amazonaws.kinesis.producer.SampleKPLProducer in the Java part of this project)
expected_agg_record_with_ehks = \
    "84mawgokZmMwM2RkODgtM2U3OS00NDhhLWIwMWEtN2NmMWJkNDdiNzg0CiRjYWU0MWIxYy1lYTYxLTQz" + \
    "ZjItOTBiZS1iODc1NWViZjg4ZTIKJGQ0OTA2OTBjLWU3NGQtNGRiMi1hM2M4LWQ4ZjJmMTg0ZmQyMwokYzkyNGJjMDktYjg1ZS00N2" + \
    "YxLWIzMmUtMzM2NTIyZWU1M2M4EiYzODQ4NjQ5NTg2NzUwODM5OTA3ODE1OTcyMzg0NjA1MTgwNzAyMBInMTkzNzg3NjAwMDM3Njgx" + \
    "NzA2OTUyMTQzMzU3MDcxOTE2MzUyNjA0EicyNjY4ODA0MzY5NjQ5MzI0MjQyNjU0NjY5MTY3MzQwNjg2ODQ0MzkSJzMzOTYwNjYwMD" + \
    "k0Mjk2NzM5MTg1NDYwMzU1MjQwMjAyMTg0NzI5MhomCAAQABogUkVDT1JEIDIyIHBlZW9iaGN6YnpkbXNrYm91cGd5cQoaJggBEAEa" + \
    "IFJFQ09SRCAyMyB1c3dreGZ0eHJvZXVzc2N4c2pobm8KGiYIAhACGiBSRUNPUkQgMjQgY2FzZWhkZ2l2ZmF4ZXVzdGx5c3p5ChomCA" + \
    "MQAxogUkVDT1JEIDI1IG52ZmZ2cG11b2dkb3BqaGFtZXZyawpRwVPQ3go0yp4Y6kvM0q3V"

expected_agg_record_no_ehks = \
    '84mawgokNzhmZWIxMmEtZGJhMC00NWNhLWE5MWUtYmIxZjJmMTgxOWI0CiRjMmE3NTc4Ny00NjliLTQzMTAtODQwZC1kNDg2ZGNhN2' + \
    'ViNWUKJDY1MGU5MzYyLTU3MmItNDQyNy1iM2ZjLTEzNTQ5ZDdlNWFlNQokMWQ4ZDk2MDAtMDBiNy00NmYzLWE5ODMtZGU4MzQ3NzU0' + \
    'MGMwCiRmMmU3MThhMC0zODliLTQ5NGEtYjc5Ni0zMzU1YjA3NTY5Y2MKJDk2NWI4ZGE0LWE2YmQtNDc1NS04MWM5LWU3MTgxYWI3ZG' + \
    'M5YQokZDY5M2M1ZjAtNTc3Mi00NmM5LThkODYtMDhjNzA1NWJkYjc1CiQ4ZDkyNDc2Yi1lYjg5LTRlODEtOTlmYi1jMmJhNjNhZDZk' + \
    'OTAaJAgAGiBSRUNPUkQgMTggaWtscnBzdnloeG5lcm9kZmVoZXJ1ChokCAEaIFJFQ09SRCAxOSBsbm1udWR5aHZwdmNncm9rZm16ZG' + \
    'gKGiQIAhogUkVDT1JEIDIwIHBqcXJjZmJ4YWZ6ZG5kem1iZ2FuZQoaJAgDGiBSRUNPUkQgMjEgYWFmcmV5cm50d3V3eGxpeWdrYmhr' + \
    'ChokCAQaIFJFQ09SRCAyMiBxc2FhZGhic2dtdnFnZGlmZ3V5b3UKGiQIBRogUkVDT1JEIDIzIHdqb2tvaXd2enpobGZ0bHpocWZqaw' + \
    'oaJAgGGiBSRUNPUkQgMjQgem9wY2FzY3J0aGJicG5qaXd1aGpiChokCAcaIFJFQ09SRCAyNSB0eGJidm5venJ3c2JveHVvbXFib3EK' + \
    'vV1rrLdU+Sy2v8xkgZ5YaA=='


class RecordAggregatorTest(unittest.TestCase):

    def test_multirouting(self):
        stream_name = "IanTest"
        region = 'eu-west-1'

        def processor(user_record):
            print("Callback Received for Record %s" % user_record.get_partition_key())

        agg_manager = agg.AggregationManager(stream_name=stream_name, region_name=region,
                                             refresh_shard_frequency_count=1, processing_callback=processor)

        # make sure we don't just have a single shard stream
        self.assertNotEqual(1, len(agg_manager._shard_list))

        # generate an aggregator for the first shard (hash = 0)
        first_agg = agg_manager.get_record_aggregator(partition_key='abc', explicit_hash_key=0)

        # generate an aggregator for the last shard (hash = max_hash)
        second_agg = agg_manager.get_record_aggregator(partition_key='abc',
                                                       explicit_hash_key=340282366920938463463374607431768211455)

        # check that the destinations of the two record aggregators are different
        self.assertNotEqual(first_agg._destination_shard, second_agg._destination_shard)

    def test_single_record_agg_matches_real_kpl_with_ehks(self):

        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

        result = aggregator.add_user_record(partition_key='fc03dd88-3e79-448a-b01a-7cf1bd47b784',
                                            explicit_hash_key='38486495867508399078159723846051807020',
                                            data='RECORD 22 peeobhczbzdmskboupgyq\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(1, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='cae41b1c-ea61-43f2-90be-b8755ebf88e2',
                                            explicit_hash_key='193787600037681706952143357071916352604',
                                            data='RECORD 23 uswkxftxroeusscxsjhno\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(2, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='d490690c-e74d-4db2-a3c8-d8f2f184fd23',
                                            explicit_hash_key='266880436964932424265466916734068684439',
                                            data='RECORD 24 casehdgivfaxeustlyszy\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(3, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='c924bc09-b85e-47f1-b32e-336522ee53c8',
                                            explicit_hash_key='339606600942967391854603552402021847292',
                                            data='RECORD 25 nvffvpmuogdopjhamevrk\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(4, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        agg_record = aggregator.clear_and_get()
        self.assertEqual(0, aggregator.get_num_user_records(), 'Cleared aggregator reported non-empty content.')

        pk, ehk, data = agg_record.get_contents()

        self.assertEqual('fc03dd88-3e79-448a-b01a-7cf1bd47b784', pk,
                         'Agg record partition key does not match partition key of first user record (it should).')
        self.assertEqual('38486495867508399078159723846051807020', ehk,
                         'Agg record explicit hash key does not match EHK of first user record (it should).')
        self.assertEqual(expected_agg_record_with_ehks, base64.b64encode(data).decode('utf-8'),
                         'Agg record data does not match aggregated data from KPL (it should).')

    def test_single_record_agg_matches_real_kpl_no_ehks(self):

        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

        result = aggregator.add_user_record(partition_key='78feb12a-dba0-45ca-a91e-bb1f2f1819b4',
                                            data='RECORD 18 iklrpsvyhxnerodfeheru\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(1, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='c2a75787-469b-4310-840d-d486dca7eb5e',
                                            data='RECORD 19 lnmnudyhvpvcgrokfmzdh\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(2, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='650e9362-572b-4427-b3fc-13549d7e5ae5',
                                            data='RECORD 20 pjqrcfbxafzdndzmbgane\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(3, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='1d8d9600-00b7-46f3-a983-de83477540c0',
                                            data='RECORD 21 aafreyrntwuwxliygkbhk\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(4, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='f2e718a0-389b-494a-b796-3355b07569cc',
                                            data='RECORD 22 qsaadhbsgmvqgdifguyou\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(5, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='965b8da4-a6bd-4755-81c9-e7181ab7dc9a',
                                            data='RECORD 23 wjokoiwvzzhlftlzhqfjk\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(6, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='d693c5f0-5772-46c9-8d86-08c7055bdb75',
                                            data='RECORD 24 zopcascrthbbpnjiwuhjb\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(7, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        result = aggregator.add_user_record(partition_key='8d92476b-eb89-4e81-99fb-c2ba63ad6d90',
                                            data='RECORD 25 txbbvnozrwsboxuomqboq\n')
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(8, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        agg_record = aggregator.clear_and_get()
        self.assertEqual(0, aggregator.get_num_user_records(), 'Cleared aggregator reported non-empty content.')

        pk, ehk, data = agg_record.get_contents()

        self.assertEqual('78feb12a-dba0-45ca-a91e-bb1f2f1819b4', pk,
                         'Agg record partition key does not match partition key of first user record (it should).')
        self.assertIsNone(ehk, 'Agg record explicit hash key should be None.')
        self.assertEqual(expected_agg_record_no_ehks, base64.b64encode(data).decode('utf-8'),
                         'Agg record data does not match aggregated data from KPL (it should).')

    def test_single_record_too_big(self):
        aggregator = agg.RecordAggregator()
        with self.assertRaises(ValueError):
            aggregator.add_user_record(partition_key='pk', data=bytes(2 * MAX_BYTES_PER_RECORD))

        # 200 KB exceeds configured max_size of 100 KB
        aggregator = agg.RecordAggregator(max_size=100 * 1024)
        with self.assertRaises(ValueError):
            aggregator.add_user_record(partition_key='pk', data=bytes(200 * 1024))

    def test_max_size(self):
        aggregator = agg.RecordAggregator(max_size=300 * 1024)
        # first 200K fits
        result = aggregator.add_user_record(partition_key='pk', data=bytes(200 * 1024))
        if result:
            self.fail('Unexpectedly received a full agg record.')
        self.assertEqual(1, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

        # another 200K triggers full record
        result = aggregator.add_user_record(partition_key='pk', data=bytes(200 * 1024))
        if not result:
            self.fail('Unexpectedly failed to receive a full agg record.')
        self.assertEqual(1, aggregator.get_num_user_records(), 'Aggregator reported improper number of records.')

    def test_invalid_max_size(self):
        with self.assertRaises(ValueError):
            agg.RecordAggregator(max_size=2 * MAX_BYTES_PER_RECORD)

    def test_decompression(self):
        aggregator = agg.RecordAggregator()
        self.assertEqual(0, aggregator.get_num_user_records(),
                         'New aggregator reported non-empty content.')

        result = aggregator.add_user_record(partition_key='fc03dd88-3e79-448a-b01a-7cf1bd47b784',
                                            explicit_hash_key='38486495867508399078159723846051807020',
                                            data='RECORD 22 peeobhczbzdmskboupgyq\n')

        result = aggregator.add_user_record(partition_key='cae41b1c-ea61-43f2-90be-b8755ebf88e2',
                                            explicit_hash_key='193787600037681706952143357071916352604',
                                            data='RECORD 23 uswkxftxroeusscxsjhno\n')

        result = aggregator.add_user_record(partition_key='d490690c-e74d-4db2-a3c8-d8f2f184fd23',
                                            explicit_hash_key='266880436964932424265466916734068684439',
                                            data='RECORD 24 casehdgivfaxeustlyszy\n')

        result = aggregator.add_user_record(partition_key='c924bc09-b85e-47f1-b32e-336522ee53c8',
                                            explicit_hash_key='339606600942967391854603552402021847292',
                                            data='RECORD 25 nvffvpmuogdopjhamevrk\n')

        agg_record = aggregator.clear_and_get()
        pk, ehk, data = agg_record.get_contents()
        self.assertIsNotNone(pk)
        self.assertIsNotNone(data)

        # check that we can decompress the agg record back into real records
        record_count = 0
        for r in agg_record.decompress():
            record_count += 1

        self.assertEqual(4, record_count)

if __name__ == '__main__':
    unittest.main()
