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

import com.ebay.kudu.streaming.convertor.builder.UserTableDataTypeBuilder;
import com.ebay.kudu.streaming.configuration.UserTableDataTypeDetail;
import com.ebay.kudu.streaming.convertor.parser.UserTableDataTypeParser;
import org.junit.Assert;
import org.junit.Test;

public class UserTableDataTypeBuilderTest {
    @Test
    public void build() throws Exception {
        UserTableDataTypeDetail detail = UserTableDataTypeParser.getInstance().parse(UserType.class);

        UserTableDataTypeBuilder builder = new UserTableDataTypeBuilder(detail);

        UserType userType = (UserType)detail.getUserTableDataTypeConstructor().newInstance();
        builder.build(userType, "id_col", new Long[]{123l});
        builder.build(userType, "name_col", new String[]{"hello_world"});
        builder.build(userType, "age_col", new Integer[]{100});

        Assert.assertEquals(Long.valueOf(123l), userType.getId());
        Assert.assertEquals("hello_world", userType.getName());
        Assert.assertEquals(Integer.valueOf(100), userType.getAge());
    }
}
