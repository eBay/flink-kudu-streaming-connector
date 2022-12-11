/*
 * Copyright 2022 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ebay.streaming.utils;

import com.ebay.kudu.streaming.configuration.StreamingColumn;
import com.ebay.kudu.streaming.utils.StreamingKeySorter;
import com.ebay.streaming.convertor.UserType;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class StreamingKeySorterTest {
    @Test
    public void testCompareOffsets() {
        List<StreamingColumn> streamingColumns = Lists.newArrayList();

        StreamingColumn one = new StreamingColumn("col_1", "col1", Long.class, 1);
        StreamingColumn two = new StreamingColumn("col_2", "col2", String.class, 2);

        streamingColumns.add(one);
        streamingColumns.add(two);

        String[] key1 = {"-1", "b"};
        String[] key2 = {"1", "a"};
        Assert.assertTrue(StreamingKeySorter.compareOffsets(key1, key2, streamingColumns) < 0);

        String[] key3 = {"1", "b"};
        String[] key4 = {"0", "a"};
        Assert.assertTrue(StreamingKeySorter.compareOffsets(key3, key4, streamingColumns) > 0);

        String[] key5 = {"1", "b"};
        String[] key6 = {"1", "a"};
        Assert.assertTrue(StreamingKeySorter.compareOffsets(key5, key6, streamingColumns) > 0);

        String[] key7 = {"1", "a"};
        String[] key8 = {"1", "a"};
        Assert.assertTrue(StreamingKeySorter.compareOffsets(key7, key8, streamingColumns) == 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompareOffsetsWithIllegalType() {
        List<StreamingColumn> streamingColumns = Lists.newArrayList();

        StreamingColumn one = new StreamingColumn("col_1", "col1", Long.class, 1);
        StreamingColumn two = new StreamingColumn("col_2", "col2", UserType.class, 2);

        streamingColumns.add(one);
        streamingColumns.add(two);

        String[] key1 = {"1", "{}"};
        String[] key2 = {"1", "{}"};
        StreamingKeySorter.compareOffsets(key1, key2, streamingColumns);
    }
}
