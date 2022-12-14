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
package com.ebay.kudu.streaming.utils;

import com.ebay.kudu.streaming.configuration.StreamingColumn;

import java.util.List;

public class StreamingKeySorter {
    public static int compareOffsets(String[] one, String[] two, List<StreamingColumn> streamingColumns) {
        for (int i = 0; i < one.length; i++) {
            StreamingColumn columnDetail = streamingColumns.get(i);
            int r;
            if (columnDetail.getFieldType() == Long.class || columnDetail.getFieldType() == Integer.class) {
                Long a = Long.valueOf(one[i]);
                Long b = Long.valueOf(two[i]);
                r = a.compareTo(b);
            } else if (columnDetail.getFieldType() == String.class) {
                r = one[i].compareTo(two[i]);
            } else {
                throw new IllegalArgumentException("Unsupported field type: " + columnDetail.getFieldType() + " for field: " + columnDetail.getFieldName());
            }

            if (r != 0) {
                return r;
            }
        }
        return 0;
    }
}
