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
from __future__ import division

import aws_kinesis_agg.kpl_pb2
import hashlib
import six
import threading
import copy

def _calculate_varint_size(value):
    """For an integral value represented by a varint, calculate how many bytes 
    are necessary to represent the value in a protobuf message.
    (see https://developers.google.com/protocol-buffers/docs/encoding# varints)
     
    Args:
        value (int) - The value whose varint size will be calculated
    Returns:
        The number of bytes necessary to represent the input value as a varint. (int)"""
    
    if value < 0:
        raise ValueError("Size values should not be negative.")
    
    num_bits_needed = 0
    
    if value == 0:
        num_bits_needed = 1
    else:
        # shift the value right one bit at a time until
        # there are no more '1' bits left...this counts
        # how many bits we need to represent the number
        while value > 0:
            num_bits_needed += 1
            value = value >> 1
        
    # varints only use 7 bits of the byte for the actual value
    num_varint_bytes = num_bits_needed // 7
    if num_bits_needed % 7 > 0:
        num_varint_bytes += 1
        
    return num_varint_bytes
    
    
class KeySet(object):
    """A class for tracking unique partition keys or explicit hash keys for an
    aggregated Kinesis record. Also assists in keeping track of indexes for
    their locations in the protobuf tables."""
    
    def __init__(self):
        """Create a new, empty KeySet."""
        
        self.keys = []
        self.lookup = {}

    def get_potential_index(self, key):
        """If the input key were added to this KeySet, determine what
        its resulting index would be.
        
        Args:
            key (str) - The key whose index should be calculated
        Returns:
            The integer index that this key would occupy if added to the KeySet. (int)
        """
        
        if key in self.lookup:
            return self.lookup[key]
        return len(self.keys)

    def add_key(self, key):
        """Add a new key to this KeySet.
        
        Args:
            key (str) - The key to add.
        Returns:
            A tuple of (bool,int). The bool is true if this key is not 
            already in the KeySet or false otherwise. The int indicates
            the index of the key."""
        
        if key in self.lookup:
            return False, self.lookup[key]
    
        if key not in self.lookup:
            self.lookup[key] = len(self.keys)
            
        self.keys.append(key)
        return True, len(self.keys) - 1

    def contains(self, key):
        """Check if this KeySet contains the input key.
        
        Args:
            key (str) - The key whose existence in the KeySet should be checked.
        Returns:
            True if the input key exists in this KeySet, False otherwise."""
        
        return key is not None and key in self.lookup

    def clear(self):
        """Clear all existing data from this KeySet and reset it to empty."""
        
        del self.keys[:]
        self.lookup.clear()


# Not thread-safe
class RecordAggregator(object):
    """An object to ingest Kinesis user records and optimally aggregate
    them into aggregated Kinesis records.
    
    NOTE: This object is not thread-safe."""
    
    def __init__(self):
        """Create a new empty aggregator."""
        
        self.current_record = AggRecord()
        self.callbacks = []

    def on_record_complete(self, callback, execute_on_new_thread=True):
        """A method to register a callback that will be notified (on
        a separate thread) when a fully-packed record is available.
        
        Args:
            callback - A function handle or callable object that will be called
            on a separate thread every time a new aggregated record is available
            (function or callable object).
            
            execute_on_new_thread - True if callbacks should be executed on a new
            thread, False if it should be executed on the calling thread. Defaults
            to True. (boolean)"""
        
        if callback not in self.callbacks:
            self.callbacks.append((callback, execute_on_new_thread))

    def get_num_user_records(self):
        """Returns:
            The number of user records currently aggregated in this aggregated record. (int)"""
        
        return self.current_record.get_num_user_records()

    def get_size_bytes(self):
        """Returns:
            The total number of bytes in this aggregated record (based on the size of the
            serialized record. (int)"""
        
        return self.current_record.get_size_bytes()

    def clear_record(self):
        """Clear all the user records from this aggregated record and reset it to an
        empty state."""
        
        self.current_record = AggRecord()

    def clear_callbacks(self):
        """Clear all the callbacks from this object that were registered with the
        on_record_complete method."""
        
        del self.callbacks[:]

    def clear_and_get(self):
        """Get the current contents of this aggregated record (whether full or not)
        as a single record and then clear the contents of this object so it can
        be re-used.  This method is useful for flushing the aggregated record when
        you need to transmit it before it is full (e.g. you're shutting down or
        haven't transmitted in a while).
        
        Returns:
            A partially-filled AggRecord or None if the aggregator is empty. (AggRecord)"""
        
        if self.get_num_user_records() == 0:
            return None
        
        out_record = self.current_record
        self.clear_record()
        return out_record

    def add_user_record(self, partition_key, data, explicit_hash_key=None):
        """Add a new user record to this aggregated record (will trigger a callback
        via onRecordComplete if aggregated record is full).
           
        Args:
            partition_key (bytes or str) - The partition key of the record to add. If pk is a
                                  string type, it will be encoded to bytes using utf-8.
            data (bytes or str) - The raw data of the record to add. If data is a string type,
                                  it will be encoded to bytes using utf-8.
            explicit_hash_key (str) - The explicit hash key of the record to add (optional)
                                      If ehk is a string type, it will be encoded to bytes
                                      using utf-8.
        Returns:
            A AggRecord if this aggregated record is full and ready to
            be transmitted or null otherwise. (AggRecord)"""

        # Attempt to add to the current aggregated record
        success = self.current_record.add_user_record(partition_key, data, explicit_hash_key)
        if success:
            # we were able to add the current data to the in-flight record
            return None
        
        # If we hit this point, aggregated record is full
        # Call all the callbacks (potentially on a separate thread)
        out_record = copy.deepcopy(self.current_record)
        for (callback, execute_on_new_thread) in self.callbacks:
            if execute_on_new_thread:
                threading.Thread(target=callback, args=(out_record,)).start()
            else:
                callback(out_record)
        
        # Current record is full so clear it out, make a new empty one and add the user record
        self.clear_record()
        self.current_record.add_user_record(partition_key, data, explicit_hash_key)
        
        return out_record

    
class AggRecord(object):
    """Represents a single aggregated Kinesis record. This Kinesis record is built
    by adding multiple user records and then serializing them to bytes using the
    Kinesis aggregated record format. This class lifts heavily from the existing 
    KPL C++ libraries found at https://github.com/awslabs/amazon-kinesis-producer.
    
    This class is NOT thread-safe.
    
    For more details on the Kinesis aggregated record format, see:
    https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md"""
    
    def __init__(self):
        """Create a new empty aggregated record."""
        
        self.agg_record = aws_kinesis_agg.kpl_pb2.AggregatedRecord()
        self._agg_partition_key = ''
        self._agg_explicit_hash_key = ''
        self._agg_size_bytes = 0
        self.partition_keys = KeySet()
        self.explicit_hash_keys = KeySet()

    def get_num_user_records(self):
        """Returns:
            The current number of user records added via the "addUserRecord(...)" method. (int)"""
        
        return len(self.agg_record.records)

    def get_size_bytes(self):
        """Returns:
            The current size in bytes of this message in its serialized form. (int)"""
        
        return len(aws_kinesis_agg.MAGIC) + self._agg_size_bytes + aws_kinesis_agg.DIGEST_SIZE

    def _serialize_to_bytes(self):
        """Serialize this record to bytes.  Has no side effects (i.e. does not affect the contents of this record object).
        
        Returns: 
            A byte array containing a aggregated Kinesis record. (binary str)"""
        
        message_body = self.agg_record.SerializeToString()

        md5_calc = hashlib.md5()
        md5_calc.update(message_body)
        calculated_digest = md5_calc.digest()
        
        return aws_kinesis_agg.MAGIC + message_body + calculated_digest

    def clear(self):
        """Clears out all records and metadata from this object so that it can be
        reused just like a fresh instance of this object."""
        
        self.agg_record = aws_kinesis_agg.kpl_pb2.AggregatedRecord()
        self._agg_partition_key = ''
        self._agg_explicit_hash_key = ''
        self._agg_size_bytes = 0
        self.partition_keys.clear()
        self.explicit_hash_keys.clear()

    def get_contents(self):
        """Get the contents of this aggregated record as members that can be used
        to call the Kinesis PutRecord or PutRecords API.  Note that this method does
        not affect the contents of this object (i.e. it has no side effects).
        
        Returns:
            A tuple of (partition key, explicit hash key, binary data) that represents
            the contents of this aggregated record. (str,str,binary str)"""
        
        agg_bytes = self._serialize_to_bytes()
        return self._agg_partition_key, self._agg_explicit_hash_key, agg_bytes

    def get_partition_key(self):
        """Get the overarching partition key for the entire aggregated record.
        
        Returns: 
            The partition key to use for the aggregated record or None if this record is empty. (str)"""
        
        if self.get_num_user_records() == 0:
            return None
        
        return self._agg_partition_key

    def get_explicit_hash_key(self):
        """Get the overarching explicit hash key for the entire aggregated record.
        
        Returns: 
            The explicit hash key to use for the aggregated record or None if this record is empty. (str)"""
        
        if self.get_num_user_records() == 0:
            return None
        
        return self._agg_explicit_hash_key

    def _calculate_record_size(self, partition_key, data, explicit_hash_key=None):
        """Based on the current size of this aggregated record, calculate what the
        new size would be if we added another user record with the specified
        parameters (used to determine when this aggregated record is full and
        can't accept any more user records).  This calculation is highly dependent
        on the Kinesis message aggregation format.
     
        Args:
            partition_key - The partition key of the new record to simulate adding (str)
            explicit_hash_key - The explicit hash key of the new record to simulate adding (str) (optional)
            data - The raw data of the new record to simulate adding (binary str)
        Returns:
            The new size of this existing record in bytes if a new user
            record with the specified parameters was added. (int)"""
        
        message_size = 0
        
        # has the partition key been added to the table of known PKs yet?
        if not self.partition_keys.contains(partition_key):
            pk_length = len(partition_key)
            message_size += 1
            message_size += _calculate_varint_size(pk_length)
            message_size += pk_length
            
        # has the explicit hash key been added to the table of known EHKs yet?
        if explicit_hash_key is not None and not self.explicit_hash_keys.contains(explicit_hash_key):
            ehk_length = len(explicit_hash_key)
            message_size += 1
            message_size += _calculate_varint_size(ehk_length)
            message_size += ehk_length
            
        # remaining calculations are for adding the new record to the list of records
            
        inner_record_size = 0
        
        # partition key field
        inner_record_size += 1
        inner_record_size += _calculate_varint_size(self.partition_keys.get_potential_index(partition_key))
        
        # explicit hash key field (this is optional)
        if explicit_hash_key is not None:
            inner_record_size += 1
            inner_record_size += _calculate_varint_size(self.explicit_hash_keys.get_potential_index(explicit_hash_key))
        
        # data field
        inner_record_size += 1
        inner_record_size += _calculate_varint_size(len(data))
        inner_record_size += len(data)
        
        message_size += 1
        message_size += _calculate_varint_size(inner_record_size)
        message_size += inner_record_size
        
        return message_size

    def add_user_record(self, partition_key, data, explicit_hash_key=None):
        """Add a new user record to this existing aggregated record if there is
        enough space (based on the defined Kinesis limits for a PutRecord call).
        
        Args:
            partition_key - The partition key of the new user record to add (bytes)
            explicit_hash_key - The explicit hash key of the new user record to add (bytes)
            data - The raw data of the new user record to add (bytes)
        Returns:
            True if the new user record was successfully added to this
            aggregated record or false if this aggregated record is too full."""

        if isinstance(partition_key, six.string_types):
            partition_key_bytes = partition_key.encode('utf-8')
        else:
            partition_key_bytes = partition_key

        if explicit_hash_key is not None and isinstance(explicit_hash_key, six.string_types):
            explicit_hash_key_bytes = explicit_hash_key.encode('utf-8')
        elif explicit_hash_key is None:
            explicit_hash_key_bytes = None
            # explicit_hash_key_bytes = AggRecord._create_explicit_hash_key(partition_key_bytes)
            # explicit_hash_key = explicit_hash_key_bytes.decode('utf-8')
        else:
            explicit_hash_key_bytes = explicit_hash_key

        if isinstance(data, six.string_types):
            data_bytes = data.encode('utf-8')
        else:
            data_bytes = data

        # Validate new record size won't overflow max size for a PutRecordRequest
        size_of_new_record = self._calculate_record_size(partition_key_bytes, data_bytes, explicit_hash_key_bytes)
        if size_of_new_record > aws_kinesis_agg.MAX_BYTES_PER_RECORD:
            raise ValueError('Input record (PK=%s, EHK=%s, SizeBytes=%d) too big to fit inside a single agg record.' %
                             (partition_key, explicit_hash_key, size_of_new_record))
        elif self.get_size_bytes() + size_of_new_record > aws_kinesis_agg.MAX_BYTES_PER_RECORD:
            return False
        
        record = self.agg_record.records.add()
        record.data = data_bytes
        
        pk_add_result = self.partition_keys.add_key(partition_key)
        if pk_add_result[0]:
            self.agg_record.partition_key_table.append(partition_key)
        record.partition_key_index = pk_add_result[1]

        if explicit_hash_key:
            ehk_add_result = self.explicit_hash_keys.add_key(explicit_hash_key)
            if ehk_add_result[0]:
                self.agg_record.explicit_hash_key_table.append(explicit_hash_key)
            record.explicit_hash_key_index = ehk_add_result[1]
        
        self._agg_size_bytes += size_of_new_record
        
        # if this is the first record, we use its partition key and hash key for the entire agg record
        if len(self.agg_record.records) == 1:
            self._agg_partition_key = partition_key
            self._agg_explicit_hash_key = explicit_hash_key
            
        return True

    @staticmethod
    def _create_explicit_hash_key(partition_key_bytes):
        """Calculate a new explicit hash key based on the input partition key
        (following the algorithm from the original KPL).
    
        Args:
            partition_key The partition key to seed the new explicit hash key with (bytes)
        Returns:
            An explicit hash key based on the input partition key generated
            using an algorithm from the original KPL."""
        
        hash_key = 0
        
        md5_calc = hashlib.md5()
        md5_calc.update(partition_key_bytes)
        pk_digest = md5_calc.digest()
        
        for i in range(0, aws_kinesis_agg.DIGEST_SIZE):
            p = int(pk_digest[i].encode('hex'), 16)
            p <<= (16 - i - 1) * 8
            hash_key += p

        return str(hash_key).encode('utf-8')
