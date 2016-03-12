import sys

#Verify the user's Python version is recent enough
is_python_version_ok = True
if sys.version_info.major != 2:
    is_python_version_ok = False
elif sys.version_info.minor < 7:
    is_python_version_ok = False
if not is_python_version_ok:
    print>>sys.stderr,'You must be running Python version 2.7 or greater to run this script (and not Python 3.x). Your current version is %d.%d.%d.' %\
                      (sys.version_info.major, sys.version_info.minor, sys.version_info.micro)
    sys.exit(1)

#Verify that we have access to boto
try:
    import boto3
except ImportError:
    print>>sys.stderr,"The 'boto3' module is required to run this script. Use 'pip install boto3' to get it."
    sys.exit(1)
import botocore.config
    
import random
import uuid

import aws_kpl_agg.aggregator
    
ALPHABET = 'abcdefghijklmnopqrstuvwxyz'

kinesis_client = None
stream_name = None
    
def get_random_record(seq_num=0, desired_len=50):
    
    global ALPHABET
    
    pk = str(uuid.uuid4())
    ehk = str(uuid.uuid4().int)
    
    data = 'RECORD %d ' % (seq_num)
    while len(data) < (desired_len-1):
        data += ALPHABET[random.randrange(0,len(ALPHABET))]
    data += '\n'
    
    return (pk,ehk,data)

def init_kinesis_client(region_name):
    
    global kinesis_client
    
    config = botocore.config.Config()
    config.region_name = region_name
    config.connection_timeout = 60
    config.read_timeout = 60
    
    kinesis_client = boto3.client('kinesis', config=config)

def send_record(agg_record):
    
    global kinesis_client, stream_name
    
    if agg_record is None:
        return
    
    pk, ehk, data = agg_record.get_contents()
    
    print 'Submitting record with EHK=%s' % (ehk)
    try:
        kinesis_client.put_record(StreamName=stream_name,
                                  Data=data,
                                  PartitionKey=pk,
                                  ExplicitHashKey=ehk)
    except Exception as e:
        print>>sys.stderr,'Transmission Failed: %s' % (e)
    else:
        print 'Completed record with EHK=%s' % (ehk)
    
if __name__ == '__main__':
        
    #For details on how to supply AWS credentials to boto3, see:
    #https://boto3.readthedocs.org/en/latest/guide/configuration.html
    
    RECORDS_TO_TRANSMIT = 1024
    RECORD_SIZE_BYTES = 1024

    if len(sys.argv) != 3:
        print>>sys.stderr,"python kinesis_publisher.py <stream name> <region>"
        sys.exit(1)
        
    stream_name = sys.argv[1]
    region_name = sys.argv[2]
    
    init_kinesis_client(region_name)
    kinesis_agg = aws_kpl_agg.aggregator.Aggregator()
    kinesis_agg.on_record_complete(send_record)
    
    print 'Creating %d records...' % (RECORDS_TO_TRANSMIT)
    for i in range(1,RECORDS_TO_TRANSMIT+1):
        
        pk, ehk, data = get_random_record(i, RECORD_SIZE_BYTES)
        kinesis_agg.add_user_record(pk, data, ehk)
        
    send_record(kinesis_agg.clear_and_get())
    