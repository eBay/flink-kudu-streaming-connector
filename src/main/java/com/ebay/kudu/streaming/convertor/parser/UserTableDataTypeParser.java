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
package com.ebay.kudu.streaming.convertor.parser;

import com.ebay.kudu.streaming.configuration.StreamingColumn;
import com.ebay.kudu.streaming.configuration.type.annotation.ColumnDetail;
import com.ebay.kudu.streaming.configuration.type.annotation.StreamingKey;
import com.ebay.kudu.streaming.configuration.ReflectionTypeDetail;
import com.ebay.kudu.streaming.configuration.UserTableDataTypeDetail;
import org.apache.kudu.shaded.com.google.common.base.Joiner;
import org.apache.kudu.shaded.com.google.common.collect.Maps;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

public class UserTableDataTypeParser<T> {
    private final static String SET_METHOD_PREFIX = "set";

    private final static UserTableDataTypeParser INST = new UserTableDataTypeParser();

    private UserTableDataTypeParser() {}

    public static UserTableDataTypeParser getInstance() {
        return INST;
    }

    public UserTableDataTypeDetail parse(Class<T> userTableDataType) throws Exception {
        UserTableDataTypeDetail typeDetail = new UserTableDataTypeDetail();

        Constructor<?> ctor = userTableDataType.getConstructor();
        typeDetail.setUserTableDataTypeConstructor(ctor);

        Map<String, Method> methodsByName = Maps.newHashMap();
        for (Method method : userTableDataType.getMethods()) {
            methodsByName.put(method.getName(), method);
        }
        for (Field field : userTableDataType.getDeclaredFields()) {
            StreamingKey streamingKey = field.getAnnotation(StreamingKey.class);
            ColumnDetail columnDetail = field.getAnnotation(ColumnDetail.class);

            if (streamingKey != null) {
                typeDetail.getStreamingCols().add(
                        new StreamingColumn(columnDetail.name(), field.getName(), field.getType(), streamingKey.order()));
            }

            String fieldName = field.getName();
            String methodName = Joiner.on("").join(SET_METHOD_PREFIX,
                    fieldName.substring(0, 1).toUpperCase(),
                    fieldName.substring(1));
            Method method = methodsByName.get(methodName);

            ReflectionTypeDetail reflectionTypeDetail = new ReflectionTypeDetail();
            reflectionTypeDetail.setField(field);
            reflectionTypeDetail.setMethod(method);

            typeDetail.getReflectionTypeDetailByColNames().put(columnDetail.name(), reflectionTypeDetail);
        }

        // Sort the streaming columns by order in ascending order
        Collections.sort(typeDetail.getStreamingCols());

        return typeDetail;
    }
}
