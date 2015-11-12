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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class GroupingAggregationSerializer<T extends GroupingAggregation>
        implements Serializer<T> {
    private final Serializer<Optional<List<String>>> list;
    private final Serializer<AggregationInstance> aggregation;

    @Override
    public void serialize(SerialWriter buffer, T value) throws IOException {
        list.serialize(buffer, value.getOf());
        aggregation.serialize(buffer, value.getEach());
    }

    @Override
    public T deserialize(SerialReader buffer) throws IOException {
        final Optional<List<String>> of = list.deserialize(buffer);
        final AggregationInstance each = aggregation.deserialize(buffer);
        return build(of, each);
    }

    protected abstract T build(Optional<List<String>> of, AggregationInstance each);
}
