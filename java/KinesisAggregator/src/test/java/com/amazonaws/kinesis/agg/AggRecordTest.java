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
package com.amazonaws.kinesis.agg;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class AggRecordTest {

    private static final BigInteger MAX_VALID_HASHKEY = new BigInteger(StringUtils.repeat("FF", 16), 16);

    private static final BigInteger MIN_VALID_HASHKEY = BigInteger.ZERO;

    @Parameterized.Parameters
    public static Collection<Object> data() {
        return Arrays.asList(new Object[]{
                        "abcd",
                        "LBpUxDst3pfL2wFS0WMh" +
                        "4HVTvxhD04WbZdKFkJyQxmqlIDgoqj18g6kODL57cUHYk6EAk8hGwyuBMzJ102rRthyGdWgw5iyiOLu6" +
                        "Gihs47AEEChW7KBIcTR8CzI7tY9QOuY5rV0SlPN3sl9GxIeVNi6y8FT0PrWWR05a2rMK81PFGYIYlviS" +
                        "XgJDolOiH13Zh3JiTmtA2jJMhdDx1Tab3Gr3itWq8kg6UavBBgAUtCnQUxhVpatNMrl28W0dKaqZ",
                        new String(new byte[]{ 66, 69, 84, 0, 0, 0, 0, 46, 69, -17, -65, -67, -17, -65, -67 })
                }
        );
    }

    private final String partitionKey;

    public AggRecordTest(final String partitionKey) {
        this.partitionKey = partitionKey;
    }

    @Test
    public void shouldGenerateValidHashKeys() {
        final String expectedHashKeyHex = DigestUtils.md5Hex(partitionKey);
        final String expectedHashKeyDecimal = new BigInteger(expectedHashKeyHex, 16).toString(10);

        final AggRecord record = new AggRecord();
        record.addUserRecord(partitionKey, null, "dummy data".getBytes());

        Assert.assertThat(new BigInteger(expectedHashKeyDecimal).compareTo(MIN_VALID_HASHKEY) >= 0, is(true));

        Assert.assertThat(new BigInteger(expectedHashKeyDecimal).compareTo(MAX_VALID_HASHKEY) <= 0, is(true));

        Assert.assertThat(record.getExplicitHashKey(), equalTo(expectedHashKeyDecimal));
    }

}
