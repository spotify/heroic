/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.filter.impl;

import java.io.IOException;

import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterJsonSerialization;

public final class SerializerCommon {
    public static final FilterJsonSerialization<String> STRING =
            new FilterJsonSerialization<String>() {
                @Override
                public String deserialize(Deserializer deserializer) throws IOException {
                    return deserializer.string();
                }

                @Override
                public void serialize(Serializer serializer, String value) throws IOException {
                    serializer.string(value);
                }
            };

    public static final FilterJsonSerialization<Filter> FILTER =
            new FilterJsonSerialization<Filter>() {
                @Override
                public Filter deserialize(Deserializer deserializer) throws IOException {
                    return deserializer.filter();
                }

                @Override
                public void serialize(Serializer serializer, Filter value) throws IOException {
                    serializer.filter(value);
                }
            };
}
