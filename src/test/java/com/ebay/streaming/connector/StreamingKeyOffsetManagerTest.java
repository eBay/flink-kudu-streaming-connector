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
package com.ebay.streaming.connector;

import com.ebay.kudu.streaming.configuration.StreamingColumn;
import com.ebay.kudu.streaming.connector.StreamingLocalEventsManager;
import com.ebay.streaming.convertor.UserType;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class StreamingKeyOffsetManagerTest {
    @Test
    public void testUpdate() throws Exception {
        List<StreamingColumn> streamingColumns = Lists.newArrayList();

        StreamingColumn one = new StreamingColumn("name_col", "name", String.class, 1);
        StreamingColumn two = new StreamingColumn("id_col", "id", Long.class, 2);

        streamingColumns.add(one);
        streamingColumns.add(two);

        UserType row = new UserType();
        row.setId(1l);
        row.setName("hello_world");
        row.setAge(1);

        UserType row2 = new UserType();
        row2.setId(2l);
        row2.setName("hello_world");
        row2.setAge(1);

        UserType row3 = new UserType();
        row3.setId(1l);
        row3.setName("hello_world2");
        row3.setAge(1);

        StreamingLocalEventsManager<UserType> streamingLocalEventsManager =
                new StreamingLocalEventsManager<>(Arrays.asList(one, two));

        Assert.assertEquals("0|-1", streamingLocalEventsManager.getCurrentHWMStr());

        streamingLocalEventsManager.update(row);
        Assert.assertEquals("hello_world|1", streamingLocalEventsManager.getCurrentHWMStr());

        streamingLocalEventsManager.update(row2);
        Assert.assertEquals("hello_world|2", streamingLocalEventsManager.getCurrentHWMStr());

        Assert.assertEquals(2, streamingLocalEventsManager.getSortedLocalEvents().size());
        Iterator<UserType> itr = streamingLocalEventsManager.getSortedLocalEvents().iterator();
        long id = 1l;
        while (itr.hasNext()) {
            Assert.assertEquals(Long.valueOf(id++), itr.next().getId());
        }

        streamingLocalEventsManager.update(row3);
        Assert.assertEquals("hello_world2|1", streamingLocalEventsManager.getCurrentHWMStr());
    }
}
