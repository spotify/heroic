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

package com.spotify.heroic.aggregation;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;

@RequiredArgsConstructor
@Slf4j
public class CoreAggregationSerializer implements AggregationSerializer {
    private final Serializer<String> string;

    private final Map<Class<? extends AggregationInstance>, String> instanceMap;
    private final Map<String, Serializer<? extends AggregationInstance>> serializerMap;

    @Override
    public void serialize(SerialWriter buffer, AggregationInstance value) throws IOException {
        final String id = instanceMap.get(value.getClass());

        if (id == null) {
            throw new IllegalArgumentException(
                "Type is not a serializable aggergation: " + value.getClass());
        }

        string.serialize(buffer, id);

        final SerialWriter.Scope scope = buffer.scope();

        @SuppressWarnings("unchecked") final Serializer<AggregationInstance> serializer =
            (Serializer<AggregationInstance>) serializerMap.get(id);

        serializer.serialize(scope, value);
    }

    @Override
    public AggregationInstance deserialize(SerialReader buffer) throws IOException {
        final String id = string.deserialize(buffer);

        @SuppressWarnings("unchecked") final Serializer<AggregationInstance> serializer =
            (Serializer<AggregationInstance>) serializerMap.get(id);

        if (serializer == null) {
            buffer.skip();
            log.warn("Unknown aggregation type (" + id + "), using empty");
            return EmptyInstance.INSTANCE;
        }

        return serializer.deserialize(buffer);
    }
}
