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
import base64
import unittest

class RecordAggregatorTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_single_record_agg_matches_real_kpl(self):

        pass


if __name__ == '__main__':

    unittest.main()
