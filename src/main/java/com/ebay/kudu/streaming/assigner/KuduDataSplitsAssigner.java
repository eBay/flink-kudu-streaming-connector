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
package com.ebay.kudu.streaming.assigner;

import com.ebay.kudu.streaming.connector.KuduDataSplit;
import com.ebay.kudu.streaming.connector.KuduStreamingRunningMode;

import java.util.Random;

public class KuduDataSplitsAssigner {
    /**
     * Assign the data split to the flink TM subTask. The data splits within the same tablet will be handled
     * by the same subTask.
     *
     * @param dataSplit
     * @param numParallelSubtasks
     *
     * @return the calculated value which will be compared with the subTask index
     */
    public static int assign(KuduDataSplit dataSplit, int numParallelSubtasks, KuduStreamingRunningMode runningMode) {
        if (runningMode == KuduStreamingRunningMode.INCREMENTAL) {
            return 0; // Always pick up the first subtask to handle
        } else {
            int assignedIdx = ((dataSplit.getTabletId().hashCode() * 31) & 0x7FFFFFFF) % numParallelSubtasks;
            return assignedIdx;
        }
    }
}
