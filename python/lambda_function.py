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

from aws_kinesis_agg.deaggregator import deaggregate_records, iter_deaggregate_records
import base64
import six


def lambda_bulk_handler(event, context):
    """A Python AWS Lambda function to process Kinesis aggregated
    records in a bulk fashion."""
    
    raw_kinesis_records = event['Records']
    
    # Deaggregate all records in one call
    user_records = deaggregate_records(raw_kinesis_records)
    
    # Iterate through deaggregated records
    for record in user_records:        
        
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        six.print_('%s' % payload)
    
    return 'Successfully processed {} records.'.format(len(user_records))


def lambda_generator_handler(event, context):
    """A Python AWS Lambda function to process Kinesis aggregated
    records in a generator-based fashion."""
    
    raw_kinesis_records = event['Records']
    record_count = 0
    
    # Deaggregate all records using a generator function
    for record in iter_deaggregate_records(raw_kinesis_records):   
             
        # Kinesis data in Python Lambdas is base64 encoded
        payload = base64.b64decode(record['kinesis']['data'])
        six.print_('%s' % payload)
        record_count += 1
        
    return 'Successfully processed {} records.'.format(record_count)
