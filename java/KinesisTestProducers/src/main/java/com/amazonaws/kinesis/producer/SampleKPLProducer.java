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
package com.amazonaws.kinesis.producer;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A sample of how to use the Kinesis Producer Library (KPL) to transmit records to
 * Kinesis (adapted from samples in https://github.com/awslabs/amazon-kinesis-producer)
 */
public class SampleKPLProducer
{
    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1);
    private static final int RECORDS_PER_SECOND = 25;
    private static final int SECONDS_TO_RUN = (int) Math.round(Math.ceil(ProducerConfig.RECORDS_TO_TRANSMIT / RECORDS_PER_SECOND));

    /**
     * Given an AWS region, generate a configured Kinesis Producer.
     */
    private static KinesisProducer getKinesisProducer(String region)
    {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        config.setRegion(region);
        config.setMaxConnections(1);
        config.setRequestTimeout(60000);
        config.setRecordMaxBufferedTime(15000);
        config.setAggregationEnabled(true);

        return new KinesisProducer(config);
    }

    /**
     * Execute the given task the given number of times at the specified rate.
     */
    private static void executeAtTargetRate(final ScheduledExecutorService exec, final Runnable task, final AtomicLong counter, 
                                            final int durationSeconds, final int ratePerSecond)
    {
        exec.scheduleWithFixedDelay(new Runnable()
        {
            final long startTime = System.nanoTime();

            @Override
            public void run()
            {
                double secondsRun = (System.nanoTime() - startTime) / 1e9;
                double targetCount = Math.min(durationSeconds, secondsRun) * ratePerSecond;

                while (counter.get() < targetCount)
                {
                    counter.getAndIncrement();
                    try
                    {
                        task.run();
                    }
                    catch (Exception e)
                    {
                        System.err.println("Error running task: " + e.getMessage());
                        e.printStackTrace();
                        System.exit(1);
                    }
                }

                if (secondsRun >= durationSeconds)
                {
                    exec.shutdown();
                }
            }
        }, 0, 1, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws Exception
    {
        if (args.length != 2)
        {
            System.err.println("Usage SampleKPLProducer <stream name> <region>");
            System.exit(1);
        }

        String streamName = args[0];
        String regionName = args[1];

        final KinesisProducer producer = getKinesisProducer(regionName);

        final AtomicLong sequenceNumber = new AtomicLong(0);
        final AtomicLong completed = new AtomicLong(0);

        final FutureCallback<UserRecordResult> callback = new FutureCallback<UserRecordResult>()
        {
            @Override
            public void onFailure(Throwable t)
            {
                if (t instanceof UserRecordFailedException)
                {
                    Attempt last = Iterables.getLast(((UserRecordFailedException) t).getResult().getAttempts());
                    System.err.println(String.format("Record failed to put - %s : %s", last.getErrorCode(), last.getErrorMessage()));
                }
                System.err.println("Exception during put: " + t.getMessage());
                t.printStackTrace();
                System.exit(1);
            }

            @Override
            public void onSuccess(UserRecordResult result)
            {
                completed.getAndIncrement();
            }
        };

        final Runnable putOneRecord = new Runnable()
        {
            @Override
            public void run()
            {
                byte[] data = ProducerUtils.randomData(sequenceNumber.get(), ProducerConfig.RECORD_SIZE_BYTES);
                ListenableFuture<UserRecordResult> f = producer.addUserRecord(streamName, ProducerUtils.randomPartitionKey(),
                        ProducerUtils.randomExplicitHashKey(), ByteBuffer.wrap(data));
                Futures.addCallback(f, callback);
            }
        };

        EXECUTOR.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                long put = sequenceNumber.get();
                long total = RECORDS_PER_SECOND * SECONDS_TO_RUN;
                double putPercent = 100.0 * put / total;
                long done = completed.get();
                double donePercent = 100.0 * done / total;
                System.out.println(String.format("Put %d of %d so far (%.2f %%), %d have completed (%.2f %%)", put, total, putPercent, done, donePercent));
            }
        }, 1, 1, TimeUnit.SECONDS);

        System.out.println(String.format("Starting puts... will run for %d seconds at %d records per second", SECONDS_TO_RUN, RECORDS_PER_SECOND));

        executeAtTargetRate(EXECUTOR, putOneRecord, sequenceNumber, SECONDS_TO_RUN, RECORDS_PER_SECOND);

        EXECUTOR.awaitTermination(SECONDS_TO_RUN + 1, TimeUnit.SECONDS);

        System.out.println("Waiting for remaining puts to finish...");
        producer.flushSync();
        System.out.println("All records complete.");

        producer.destroy();
        System.out.println("Finished.");
    }
}
