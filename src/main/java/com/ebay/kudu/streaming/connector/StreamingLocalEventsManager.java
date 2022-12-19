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
package com.ebay.kudu.streaming.connector;

import com.ebay.kudu.streaming.configuration.StreamingColumn;
import com.ebay.kudu.streaming.utils.StreamingKeySorter;
import org.apache.kudu.shaded.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * This is to manage the local buffered events in sorted order for {@link KuduStreamingRunningMode#INCREMENTAL}
 *
 * @param <T> The mapped Java type against the Kudu table.
 */
public class StreamingLocalEventsManager<T> implements Serializable {
    private String initialHWM;

    /**
     * The manager will be initialized with the user configured lowerKey and upperKey;
     */
    private String userConfiguredLowerKey;
    private String userConfiguredUpperKey;

    private List<StreamingColumn> streamingColumns;

    private TreeSet<T> localEvents = new TreeSet<>(new EventComparator());

    private static final String STREAMING_KEY_DELIMITER_RE = "\\|";
    private static final String STREAMING_KEY_DELIMITER = "|";

    private static final String GET_METHOD_PREFIX = "get";

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingLocalEventsManager.class);

    private class EventComparator implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            String[] part1 = getStreamingKeyParts(o1);
            String[] part2 = getStreamingKeyParts(o2);
            return StreamingKeySorter.compareOffsets(part1, part2, streamingColumns);
        }
    }

    private String[] getStreamingKeyParts(T row) {
        String[] newParts = new String[streamingColumns.size()];
        for (int i = 0; i < streamingColumns.size(); i++) {
            StreamingColumn sc = streamingColumns.get(i);
            String methodName =
                    Joiner.on("")
                            .join(GET_METHOD_PREFIX,
                                    sc.getFieldName().substring(0, 1).toUpperCase(),
                                    sc.getFieldName().substring(1));
            try {
                String part = String.valueOf(row.getClass().getDeclaredMethod(methodName).invoke(row));
                newParts[i] = part;
            } catch (Exception e) {
                LOGGER.error("Fail to get the streaming key dynamically.");
                throw new IllegalArgumentException(e);
            }
        }
        return newParts;
    }

    private String buildRangeKeyTemplate(String longTypeKey, String stringTypeKey) {
        StringBuffer key = new StringBuffer();
        for (int i = 0; i < streamingColumns.size(); i++) {
            StreamingColumn sc = streamingColumns.get(i);
            if (sc.getFieldType() == Long.class) {
                key.append(longTypeKey);
            } else if (sc.getFieldType() == String.class) {
                key.append(stringTypeKey);
            }
            if (i < streamingColumns.size() - 1) {
                key.append(STREAMING_KEY_DELIMITER);
            }
        }
        return key.toString();
    }

    private String buildDefaultLowerStreamingKey() {
        return buildRangeKeyTemplate(String.valueOf(Long.MIN_VALUE), "0");
    }

    private String buildDefaultUpperStreamingKey() {
        return buildRangeKeyTemplate(String.valueOf(Long.MAX_VALUE), "z");
    }

    public StreamingLocalEventsManager(List<StreamingColumn> streamingColumns,
                                       String userConfiguredLowerKey,
                                       String userConfiguredUpperKey
    ) {
        this.streamingColumns = streamingColumns;
        if (userConfiguredLowerKey == null) {
            this.userConfiguredLowerKey = buildDefaultLowerStreamingKey();
        } else {
            this.userConfiguredLowerKey = userConfiguredLowerKey;
        }

        if (userConfiguredUpperKey == null) {
            this.userConfiguredUpperKey = buildDefaultUpperStreamingKey();
        } else {
            this.userConfiguredUpperKey = userConfiguredUpperKey;
        }
        this.initialHWM = this.userConfiguredLowerKey;
    }

    public void update(T row) throws Exception {
        localEvents.add(row);
    }

    public void setInitialHWM(String initialHWM) {
        this.initialHWM = initialHWM;
    }

    public String[] getCurrentHWM() {
        if (localEvents.isEmpty()) {
            return initialHWM.split(STREAMING_KEY_DELIMITER_RE);
        } else {
            T lastOne = localEvents.last();
            return getStreamingKeyParts(lastOne);
        }
    }

    public String getCurrentHWMStr() {
        return Joiner.on(STREAMING_KEY_DELIMITER).join(getCurrentHWM());
    }

    public TreeSet<T> getSortedLocalEvents() {
        return localEvents;
    }

    public void next() {
        initialHWM = getCurrentHWMStr();
        localEvents.clear();
    }

    public String[] getUserConfiguredUpperKey() {
        return userConfiguredUpperKey.split(STREAMING_KEY_DELIMITER_RE);
    }
}
