# Kinesis Aggregation/Deaggregation Libraries for Python
# 
# Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved. 
# 
# Licensed under the Amazon Software License (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
# 
#  http://aws.amazon.com/asl/
# 
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

import six
import sys

# Verify that we have access to boto
try:
    import boto3
    import botocore.config
except ImportError:
    six.print_("The 'boto3' module is required to run this script. Use 'pip install boto3' to get it.", file=sys.stderr)
    sys.exit(1)

import random
import uuid

import aws_kinesis_agg.aggregator
    
# Used for generating random record bodies
ALPHABET = 'abcdefghijklmnopqrstuvwxyz'

kinesis_client = None
stream_name = None


def get_random_record(seq_num=0, desired_len=50):
    """Generate a random record to send to Kinesis.
    
    Args:
        seq_num - The sequence number to include in the data body. (int)
        desired_len - The total size (in bytes) of the desired record body. (int)
    Returns:
        A semi-random string of the form "RECORD <seq_num> <random_alphabet_chars>"
        to use to populate the body of a Kinesis record. (str)"""
    
    global ALPHABET
    
    partition_key = str(uuid.uuid4())
    explicit_hash_key = str(uuid.uuid4().int)
    
    raw_data = 'RECORD %d ' % seq_num
    while len(raw_data) < (desired_len-1):
        raw_data += ALPHABET[random.randrange(0, len(ALPHABET))]
        raw_data += '\n'
    
    return partition_key, explicit_hash_key, raw_data


def init_kinesis_client(region):
    """Create a boto3 Kinesis client for the given reason.
    
    Args:
        region_name - The name of the AWS region the Kinesis client will be configured for (e.g. us-east-1) (str)
    Returns:
        A boto3 Kinesis client object configured for the input region."""
    
    global kinesis_client
    
    config = botocore.config.Config()
    config.region_name = region
    config.connection_timeout = 60
    config.read_timeout = 60
    
    kinesis_client = boto3.client('kinesis', config=config)


def send_record(agg_record):
    """Send the input aggregated record to Kinesis via the PutRecord API.
    
    Args:
        agg_record - The aggregated record to send to Kinesis. (AggRecord)"""
    
    global kinesis_client, stream_name
    
    if agg_record is None:
        return
    
    partition_key, explicit_hash_key, raw_data = agg_record.get_contents()
    
    six.print_('Submitting record with EHK=%s NumRecords=%d NumBytes=%d' %
               (explicit_hash_key, agg_record.get_num_user_records(), agg_record.get_size_bytes()))
    try:
        kinesis_client.put_record(StreamName=stream_name,
                                  Data=raw_data,
                                  PartitionKey=partition_key,
                                  ExplicitHashKey=explicit_hash_key)
    except Exception as e:
        six.print_('Transmission Failed: %s' % e, file=sys.stderr)
    else:
        six.print_('Completed record with EHK=%s' % ehk)
    
    
if __name__ == '__main__':
        
    # For details on how to supply AWS credentials to boto3, see:
    # https://boto3.readthedocs.org/en/latest/guide/configuration.html
    
    RECORD_SIZE_BYTES = 1024
    RECORDS_TO_TRANSMIT = 1024
    
    if len(sys.argv) != 3:
        six.print_("USAGE: python kinesis_publisher.py <stream name> <region>", file=sys.stderr)
        sys.exit(1)
        
    stream_name = sys.argv[1]
    region_name = sys.argv[2]
    
    init_kinesis_client(region_name)
    kinesis_agg = aws_kinesis_agg.aggregator.RecordAggregator()
    kinesis_agg.on_record_complete(send_record)
    
    six.print_('Creating %d records...' % RECORDS_TO_TRANSMIT)
    for i in range(1, RECORDS_TO_TRANSMIT+1):
        
        pk, ehk, data = get_random_record(i, RECORD_SIZE_BYTES)
        kinesis_agg.add_user_record(pk, data, ehk)
    
    # Do one final flush & send to get any remaining records that haven't triggered a callback yet
    send_record(kinesis_agg.clear_and_get())
