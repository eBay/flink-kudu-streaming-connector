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
package com.ebay.streaming.convertor;

import com.ebay.kudu.streaming.configuration.type.annotation.ColumnDetail;
import com.ebay.kudu.streaming.configuration.type.annotation.StreamingKey;
import lombok.Data;

@Data
public class UserType {
    @StreamingKey(order = 2)
    @ColumnDetail(name = "id_col")
    private Long id;

    @StreamingKey(order = 1)
    @ColumnDetail(name = "name_col")
    private String name;

    @ColumnDetail(name = "age_col")
    private Integer age;
}
