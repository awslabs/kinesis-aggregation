#Kinesis Aggregation/Deaggregation Libraries for Python
#
#Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved. 
#
#Licensed under the Amazon Software License (the "License").
#You may not use this file except in compliance with the License.
#A copy of the License is located at
#
# http://aws.amazon.com/asl/
#
#or in the "license" file accompanying this file. This file is distributed
#on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#express or implied. See the License for the specific language governing
#permissions and limitations under the License.

from __future__ import print_function

import aws_kinesis_agg
import base64
import collections
import google.protobuf.message
from . import kpl_pb2
import hashlib
from io import StringIO
import sys


def _create_user_record(ehks, pks, mr, r, sub_seq_num):
    '''Given a protobuf message record, generate a new Kinesis user record
    
    ehks - The list of explicit hash keys from the protobuf message (list of str)
    pks - The list of partition keys from the protobuf message (list of str)
    mr - A single deaggregated message record from the protobuf message (dict)
    r - The original aggregated kinesis record containing the protobuf message record (dict)
    sub_seq_num - The current subsequence number within the aggregated protobuf message (int)
    
    return value - A Kinesis user record created from a message record in the protobuf message (dict)'''
    
    explicit_hash_key = None                            
    if ehks and ehks[mr.explicit_hash_key_index] is not None:
        explicit_hash_key = ehks[mr.explicit_hash_key_index]
    partition_key = pks[mr.partition_key_index]
    
    new_record = {}
    new_record['kinesis'] = {}
    
    #Copy all the metadata from the original record (except the data-specific stuff)
    for key, value in r.items():
        if key != 'kinesis':
            new_record[key] = value
    new_record['kinesis']['kinesisSchemaVersion'] = r['kinesis']['kinesisSchemaVersion']
    new_record['kinesis']['sequenceNumber'] = r['kinesis']['sequenceNumber']
    
    #Fill in the new values        
    new_record['kinesis']['explicitHashKey'] = explicit_hash_key
    new_record['kinesis']['partitionKey'] = partition_key
    new_record['kinesis']['subSequenceNumber'] = sub_seq_num
    new_record['kinesis']['aggregated'] = True
    
    #We actually re-base64-encode the data because that's how Lambda Python normally delivers records
    new_record['kinesis']['data'] = base64.b64encode(mr.data)
    
    return new_record


def _get_error_string(r, message_data, ehks, pks, ar):
    '''Generate a detailed error message for when protobuf parsing fails.
    
    r - The original aggregated kinesis record containing the protobuf message record (dict)
    message_data - The raw aggregated data from the protobuf message (binary str)
    ehks - The list of explicit hash keys from the protobuf message (list)
    pks - The list of partition keys from the protobuf message (list)
    ar - The protobuf aggregated record that was being parsed when the error occurred (dict)
    
    return value - A detailed error string (str)'''
    
    error_buffer = StringIO()
    
    error_buffer.write('Unexpected exception during deaggregation, record was:\n')
    error_buffer.write('PKS:\n')
    for pk in pks:
        error_buffer.write('%s\n' % (pk))
    error_buffer.write('EHKS:\n')
    for ehk in ehks:
        error_buffer.write('%s\n' % (ehk))
    for mr in ar.records:
        error_buffer.write('Record: [hasEhk=%s, ehkIdex=%d, pkIdx=%d, dataLen=%d]\n' %
                        (ehks and ehks[mr.explicit_hash_key_index] is not None,
                        mr.explicit_hash_key_index,
                        mr.partition_key_index,
                        len(mr.data)))
    error_buffer.write('Sequence number: %s\n' % (r['kinesis']['sequenceNumber']))
    error_buffer.write('Raw data: %s\n' % (base64.b64encode(message_data)))
    
    return error_buffer.getvalue()


def deaggregate_records(records):
    '''Given a set of Kinesis records, deaggregate any records that were packed using the
    Kinesis Producer Library into individual records.  This method will be a no-op for any
    records that are not aggregated (but will still return them).
    
    records - The list of raw Kinesis records to deaggregate. (list of dict)
    
    return value - A list of Kinesis user records greater than or equal to the size of the 
    input record list. (list of dict)'''
    
    #Use the existing generator function to deaggregate all the records
    return_records = []
    return_records.extend(iter_deaggregate_records(records))        
    return return_records


def iter_deaggregate_records(records):
    '''Generator function - Given a set of Kinesis records, deaggregate them one at a time
    using the Kinesis aggregated message format.  This method will not affect any
    records that are not aggregated (but will still return them).
    
    records - The list of raw Kinesis records to deaggregate. (list of dict)
    
    return value - Each yield returns a single Kinesis user record. (dict)'''
    
    #We got a single record...try to coerce it to a list
    if isinstance(records, collections.Mapping):
        records = [records]
        
    for r in records:
        is_aggregated = True
        sub_seq_num = 0
        
        #Decode the incoming data
        raw_data = r['kinesis']['data']
        decoded_data = base64.b64decode(raw_data)
        
        #Verify the magic header
        data_magic = None
        if(len(decoded_data) >= len(aws_kinesis_agg.MAGIC)):
            data_magic = decoded_data[:len(aws_kinesis_agg.MAGIC)]
        else:
            is_aggregated = False
        
        decoded_data_no_magic = decoded_data[len(aws_kinesis_agg.MAGIC):]
        
        if aws_kinesis_agg.MAGIC != data_magic or len(decoded_data_no_magic) <= aws_kinesis_agg.DIGEST_SIZE:
            is_aggregated = False
            
        if is_aggregated:            
            
            #verify the MD5 digest
            message_digest = decoded_data_no_magic[-aws_kinesis_agg.DIGEST_SIZE:]
            message_data = decoded_data_no_magic[:-aws_kinesis_agg.DIGEST_SIZE]
            
            md5_calc = hashlib.md5()
            md5_calc.update(message_data)
            calculated_digest = md5_calc.digest()
            
            if message_digest != calculated_digest:            
                is_aggregated = False            
            else:                            
                #Extract the protobuf message
                try:    
                    ar = kpl_pb2.AggregatedRecord()
                    ar.ParseFromString(message_data)
                    
                    pks = ar.partition_key_table
                    ehks = ar.explicit_hash_key_table
                    
                    try:                    
                        #Split out all the aggregated records into individual records    
                        for mr in ar.records:                                                    
                            new_record = _create_user_record(ehks, pks, mr, r, sub_seq_num)
                            sub_seq_num += 1
                            yield new_record
                        
                    except Exception as e:        
                        
                        error_string = _get_error_string(r, message_data, ehks, pks, ar)
                        print('ERROR: %s\n%s' % (str(e), error_string), file=sys.stderr)
                    
                except google.protobuf.message.DecodeError:                    
                    is_aggregated = False
        
        if not is_aggregated:
            yield r
    
    return
