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

import google.protobuf.message
import kpl_pb2
import md5
import threading

#KPL protocol-specific constants
KPL_MAGIC = '\xf3\x89\x9a\xc2'
KPL_DIGEST_SIZE = md5.digest_size
#Kinesis Limits
#(https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html)
MAX_BYTES_PER_RECORD = 1048576 # 1 MB (1024 * 1024)


def _calculate_varint_size(value):
    
    if value < 0:
        raise ValueError("Size values should not be negative.")
    
    num_bits_needed = 0
    
    if value == 0:
        num_bits_needed = 1
    else:
        while value > 0:
            num_bits_needed += 1
            value = value >> 1
        
    num_varint_bytes = num_bits_needed / 7
    if num_bits_needed % 7 > 0:
        num_varint_bytes += 1
        
    return num_varint_bytes
    
    return 0
    
    
class KeySet(object):
    
    def __init__(self):
        
        self.keys = []
        self.lookup = {}
        self.counts = {}
        
    def get_potential_index(self, key):
        
        if key in self.lookup:
            return self.lookup[key]
        return len(self.keys)
    
    def add_key(self, key):
        
        if key in self.lookup:
            self.counts[key] = self.counts[key] + 1
            return (False, self.lookup[key])
    
        if not key in self.lookup:
            self.lookup[key] = len(self.keys)
            
        if not key in self.counts:
            self.counts[key] = 1
            
        self.keys.append(key)
        return (True, len(self.keys) - 1)
    
    def contains(self, key):
        return key is not None and key in self.lookup
    
    def clear(self):
        del self.keys[:]
        self.lookup.clear()
        self.counts.clear()


#Not thread-safe
class Aggregator(object):
    
    def __init__(self):
        
        self.current_record = KinesisAggRecord()
        self.callbacks = []
    
    def on_record_complete(self, callback):
        
        if not callback in self.callbacks:
            self.callbacks.append(callback)
            
    def get_num_user_records(self):
        
        return self.current_record.get_num_user_records()
    
    def get_size_bytes(self):
        
        return self.current_record.get_size_bytes()
    
    def clear_record(self):
        
        self.current_record = KinesisAggRecord()
    
    def clear_callbacks(self):
        
        del self.callbacks[:]
    
    def clear_and_get(self):
        
        if self.get_num_user_records() == 0:
            return None
        
        out_record = self.current_record
        self.current_record = KinesisAggRecord()
        return out_record
    
    def add_user_record(self, partition_key, data, explicit_hash_key = None):
        
        success = self.current_record.add_user_record(partition_key, data, explicit_hash_key)
        if success:
            #we were able to add the current data to the in-flight record
            return None
        
        out_record = self.current_record
        for callback in self.callbacks:
            threading.Thread(target=callback, args=(out_record,)).start()
        
        return self.clear_and_get()
    
    
class KinesisAggRecord(object):
    
    def __init__(self):
        
        self.agg_record = kpl_pb2.AggregatedRecord()
        self._agg_partition_key = ''
        self._agg_explicit_hash_key = ''
        self._agg_size_bytes = 0
        self.partition_keys = KeySet()
        self.explicit_hash_keys = KeySet()
        
        
    def get_num_user_records(self):
        
        return len(self.agg_record.records)


    def get_size_bytes(self):
        
        global KPL_MAGIC, KPL_DIGEST_SIZE
        
        return len(KPL_MAGIC) + self._agg_size_bytes + KPL_DIGEST_SIZE
    
    
    def _serialize_to_bytes(self):
        
        global KPL_MAGIC
        
        message_body = self.agg_record.SerializeToString()
        
        md5_calc = md5.new()
        md5_calc.update(message_body)
        calculated_digest = md5_calc.digest()
        
        return KPL_MAGIC + message_body + calculated_digest
    
    
    def clear(self):
        
        self.agg_record = kpl_pb2.AggregatedRecord()
        self._agg_partition_key = ''
        self._agg_explicit_hash_key = ''
        self._agg_size_bytes = 0
        self.partition_keys.clear()
        self.explicit_hash_keys.clear()
    
    
    def get_contents(self):
        
        agg_bytes = self._serialize_to_bytes()
        return (self._agg_partition_key, self._agg_explicit_hash_key, agg_bytes)
    
    
    def get_partition_key(self):
        
        return self._agg_partition_key
    
    
    def get_explicit_hash_key(self):
        
        return self._agg_explicit_hash_key
    
    
    def _calculate_record_size(self, partition_key, data, explicit_hash_key = None):
        
        message_size = 0
        
        #has the partition key been added to the table of known PKs yet?
        if not self.partition_keys.contains(partition_key):
            pk_length = len(partition_key)
            message_size += 1
            message_size += _calculate_varint_size(pk_length)
            message_size += len(partition_key)
            
        #has the explicit hash key been added to the table of known EHKs yet?
        if explicit_hash_key is not None and not self.explicit_hash_keys.contains(explicit_hash_key):
            ehk_length = len(explicit_hash_key)
            message_size += 1
            message_size += _calculate_varint_size(ehk_length)
            message_size += ehk_length
            
        inner_record_size = 0
        
        #partition key field
        inner_record_size += 1
        inner_record_size += _calculate_varint_size(self.partition_keys.get_potential_index(partition_key))
        
        #explicit hash key field (this is optional)
        if explicit_hash_key is not None:
            inner_record_size += 1
            inner_record_size += _calculate_varint_size(self.explicit_hash_keys.get_potential_index(explicit_hash_key))
        
        #data field
        inner_record_size += 1
        inner_record_size += _calculate_varint_size(len(data))
        inner_record_size += len(data)
        
        message_size += 1
        message_size += _calculate_varint_size(inner_record_size)
        message_size += inner_record_size
        
        return message_size
    
    
    def add_user_record(self, partition_key, data, explicit_hash_key = None):
        
        global MAX_BYTES_PER_RECORD
        
        partition_key = partition_key.strip()
        explicit_hash_key = explicit_hash_key.strip() if explicit_hash_key is not None else self._create_explicit_hash_key(partition_key)
        
        size_of_new_record = self._calculate_record_size(partition_key, data, explicit_hash_key)
        if self.get_size_bytes() + size_of_new_record > MAX_BYTES_PER_RECORD:
            return False
        
        record = self.agg_record.records.add()
        record.data = data
        
        pk_add_result = self.partition_keys.add_key(partition_key)
        if pk_add_result[0]:
            self.agg_record.partition_key_table.append(partition_key)
        record.partition_key_index = pk_add_result[1]
    
        ehk_add_result = self.explicit_hash_keys.add_key(explicit_hash_key)
        if ehk_add_result[0]:
            self.agg_record.explicit_hash_key_table.append(explicit_hash_key)
        record.explicit_hash_key_index = ehk_add_result[1]
        
        self._agg_size_bytes += size_of_new_record
        
        if len(self.agg_record.records) == 1:
            self._agg_partition_key = partition_key
            self._agg_explicit_hash_key = explicit_hash_key
            
        return True
    
    
    def _create_explicit_hash_key(self, partition_key):
        
        global KPL_DIGEST_SIZE
        
        hash_key = 0
        
        md5_calc = md5.new()
        md5_calc.update(partition_key)
        pk_digest = md5_calc.hexdigest()
        
        for i in range(0, KPL_DIGEST_SIZE):
            p = int(pk_digest, 16)
            p << (16 - i - 1) * 8
            hash_key += p
        
        return str(p)
    
    