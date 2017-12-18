/**
 * Kinesis Aggregation/Deaggregation Libraries for Java
 *
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.kinesis.forwarder;

import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.kinesis.agg.AggRecord;
import com.amazonaws.kinesis.agg.RecordAggregator;
import com.amazonaws.kinesis.deagg.RecordDeaggregator;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

/**
 * A sample AWS Lambda function to receive records from one Kinesis stream, aggregate
 * them and forward them to another Kinesis stream (potentially in a different AWS account).
 */
public class LambdaAggregatingForwarder implements RequestHandler<KinesisEvent, Void>
{
    //Change these values to specify the information about your destination stream
    private static final String DESTINATION_STREAM_NAME = "MyDestinationStream";
    private static final Regions DESTINATION_STREAM_REGION = Regions.US_EAST_1;
    
    //Default values for Kinesis client to the destination stream (tune these to your use case)
    private static final int DESTINATION_CONNECTION_TIMEOUT = 10000;
    private static final int DESTINATION_SOCKET_TIMEOUT = 60000;
    
    private final AmazonKinesis kinesisForwarder;
    private final RecordAggregator aggregator;

    /**
     * One-time initialization of resources for this Lambda function.
     */
    public LambdaAggregatingForwarder()
    {
        this.aggregator = new RecordAggregator();
        
        /*
         * If the Kinesis stream you're forwarding to is in the same account as this AWS Lambda function, you can just give the IAM Role executing
         * this function permissions to publish to the stream and DefaultAWSCredentialsProviderChain() will take care of it.  
         * 
         * If you're publishing to a Kinesis stream in another AWS account, it's trickier.  Kinesis doesn't currently provide cross-account publishing
         * permissions. You must create an IAM role in the AWS account with the DESTINATION stream that has permissions to publish to that stream
         * (fill that IAM role's ARN in for "<RoleToAssumeARN>" below) and then the IAM role executing this Lambda function must be given permission
         * to assume the role "<RoleToAssumeARN>" from the other AWS account.  
         */
        AWSCredentialsProvider provider = new DefaultAWSCredentialsProviderChain();
        //AWSCredentialsProvider provider = new STSAssumeRoleSessionCredentialsProvider(new DefaultAWSCredentialsProviderChain(), "<RoleToAssumeARN>", "KinesisForwarder");
        
        //Set max conns to 1 since we use this client serially
        ClientConfiguration kinesisConfig = new ClientConfiguration();
        kinesisConfig.setMaxConnections(1);
        kinesisConfig.setProtocol(Protocol.HTTPS);
        kinesisConfig.setConnectionTimeout(DESTINATION_CONNECTION_TIMEOUT);
        kinesisConfig.setSocketTimeout(DESTINATION_SOCKET_TIMEOUT);
        
        this.kinesisForwarder = new AmazonKinesisClient(provider, kinesisConfig);
        this.kinesisForwarder.setRegion(Region.getRegion(DESTINATION_STREAM_REGION));
    }
    
    /**
     * Check if the input aggregated record is complete and if so, forward it to the
     * configured destination Kinesis stream.
     * 
     * @param logger The LambdaLogger from the input Context
     * @param aggRecord The aggregated record to transmit or null if the record isn't full yet.
     */
    private void checkAndForwardRecords(LambdaLogger logger, AggRecord aggRecord)
    {
        if(aggRecord == null)
        {
            return;
        }
        
        logger.log("Forwarding " + aggRecord.getNumUserRecords() + " as an aggregated record.");
        
        PutRecordRequest request = aggRecord.toPutRecordRequest(DESTINATION_STREAM_NAME);
        try
        {
            PutRecordResult result = this.kinesisForwarder.putRecord(request);
            logger.log("Successfully published record Seq #" + result.getSequenceNumber() + " to shard " + result.getShardId());
        }
        catch(Exception e)
        {
            logger.log("ERROR: Failed to forward Kinesis records to destination stream: " + e.getMessage());
            return;
        }
    }
    
    public Void handleRequest(KinesisEvent input, Context context)
    {
        LambdaLogger logger = context.getLogger();
        logger.log("Received " + input.getRecords().size() + " raw Kinesis records.");
        
        try
        {
            //Allows us to receive and process Kinesis aggregated records, but can also process normal
            //non-aggregated records without an issue (deaggregation is a no-op in the latter scenario)
            List<UserRecord> userRecords = RecordDeaggregator.deaggregate(input.getRecords());
            
            logger.log("Received " + userRecords.size() + " deaggregated Kinesis records.");
            
            for (UserRecord userRecord : userRecords) 
            {
                try
                {
                    AggRecord aggRecord = this.aggregator.addUserRecord(userRecord);
                    checkAndForwardRecords(logger, aggRecord);
                }
                catch(Exception e)
                {
                    logger.log("[ERROR] Could not add user record: " + e.getMessage());
                }
            }
            
            checkAndForwardRecords(logger, this.aggregator.clearAndGet());
        } 
        catch (Exception e) 
        {
            logger.log("Lambda function encountered fatal error: " + e.getMessage());
        }

        return null;
    }
}
