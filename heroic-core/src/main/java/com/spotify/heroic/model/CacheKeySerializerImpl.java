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

package com.spotify.heroic.model;

import java.io.IOException;
import java.util.Map;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.filter.Filter;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public class CacheKeySerializerImpl implements CacheKeySerializer {
    private final Serializer<Integer> version;
    private final Serializer<Filter> filter;
    private final Serializer<Map<String, String>> group;
    private final Serializer<Aggregation> aggregation;
    private final Serializer<Long> base;

    @Override
    public void serialize(SerialWriter buffer, CacheKey value) throws IOException {
        version.serialize(buffer, value.getVersion());
        filter.serialize(buffer, value.getFilter());
        group.serialize(buffer, value.getGroup());
        aggregation.serialize(buffer, value.getAggregation());
        base.serialize(buffer, value.getBase());
    }

    @Override
    public CacheKey deserialize(SerialReader buffer) throws IOException {
        final int version = this.version.deserialize(buffer);
        final Filter filter = this.filter.deserialize(buffer);
        final Map<String, String> group = this.group.deserialize(buffer);
        final Aggregation aggregation = this.aggregation.deserialize(buffer);
        final long base = this.base.deserialize(buffer);
        return new CacheKey(version, filter, group, aggregation, base);
    }
}