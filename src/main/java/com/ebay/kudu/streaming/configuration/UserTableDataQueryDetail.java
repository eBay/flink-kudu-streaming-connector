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
package com.ebay.kudu.streaming.configuration;

import com.ebay.kudu.streaming.configuration.type.UserTableDataQueryFilter;
import lombok.Data;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

@Data
public class UserTableDataQueryDetail implements Serializable {
    private static final long serialVersionUID = -5260450483244281444L;
    private List<String> projectedColumns = Lists.newArrayList();
    private List<UserTableDataQueryFilter> userTableDataQueryFilters = Lists.newArrayList();
}
