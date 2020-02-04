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

import hashlib

# Message aggregation protocol-specific constants
# (https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md)
MAGIC = b'\xf3\x89\x9a\xc2'
DIGEST_SIZE = hashlib.md5().digest_size

# Kinesis Limits
# (https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html)
MAX_BYTES_PER_RECORD = 1024*1024  # 1 MB
