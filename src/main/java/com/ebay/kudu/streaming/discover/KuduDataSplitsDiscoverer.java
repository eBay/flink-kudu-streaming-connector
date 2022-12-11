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
package com.ebay.kudu.streaming.discover;

import com.ebay.kudu.streaming.connector.KuduDataSplit;
import lombok.Builder;
import org.apache.flink.connectors.kudu.connector.KuduFilterInfo;
import org.apache.flink.connectors.kudu.connector.reader.KuduReader;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Builder
public class KuduDataSplitsDiscoverer {
    private KuduReader reader;
    private List<KuduFilterInfo> filterInfoList;
    private List<String> projectedColumnList;

    /**
     * Get all the data splits against the filterInfoList
     *
     * @return all the data splits against the filterInfoList
     * @throws Exception
     */
    public List<KuduDataSplit> getAllKuduDataSplits() throws Exception {
        List<KuduScanToken> kuduScanTokenList = reader.scanTokens(filterInfoList, projectedColumnList, null);

        List<KuduDataSplit> splits = Lists.newArrayList();
        for (KuduScanToken kuduScanToken : kuduScanTokenList) {
            KuduDataSplit split = new KuduDataSplit();
            split.setScanToken(kuduScanToken.serialize());
            split.setTabletId(new String(kuduScanToken.getTablet().getTabletId(), StandardCharsets.UTF_8));

            splits.add(split);
        }
        return splits;
    }
}
