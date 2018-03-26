# Change Log

# (current) 4.0.2

- integrate [don't spuriously add ExplicitHashKey values to user records](https://github.com/awslabs/kinesis-aggregation/pull/49)

# 4.0.1

- fix utf-8 issue on aggregate record with string data

# 4.0.0

- Issue fixed for nodejs:
    - https://github.com/awslabs/kinesis-aggregation/issues/30: nodejs kinesis encoded record size limit
    - https://github.com/awslabs/kinesis-aggregation/issues/16: obsolete documentation

- Discovered bug on aggregation:
    - encoded field name was incorrect:
        - 'Partition' for 'partition', 
        - 'Data' for 'data'
        - 'ExplicitHashKey' for 'explicitHashKey'
    - RecordAggregator : invalid computing size RecordAggregator
    - aggregate function did not limit size of 1Mo
    - terminaison problem with previously onReadyCallback and afterPutAggregatedRecords on agregate : Lambda terminate before ending,
    when doing some async stuff in onReadyCallback (named encodedRecordHandler now). 

- Nodejs Project Basics:
    - package node project with common folder: (lib for source, test for test, and example for ..)
    - add test unit integration
    - update documentation
    - remove java minded stuff
    - simplify use of common/constant/source code 
    - update examples
    - remove dead code, obsolete code/doc
    - add documentation on function
    - use const/let rather than var keyword
    - use forEach rather than map when results array is not needed
    - use resulting array of map (avoid unecessary array creation)


# previously 3.0.0
 - Unknown
 