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

package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationSerializer;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

import java.io.IOException;

public abstract class  FilterKSerializer<T extends FilterKInstance> implements Serializer<T> {
    private final Serializer<Long> fixedLong;
    private final AggregationSerializer serializer;

    public FilterKSerializer(SerializerFramework framework, AggregationSerializer serializer) {
        this.fixedLong = framework.fixedLong();
        this.serializer = serializer;
    }

    @Override
    public void serialize(SerialWriter buffer, T t) throws IOException {
        fixedLong.serialize(buffer, t.getK());
        serializer.serialize(buffer, t.getOf());
    }

    @Override
    public T deserialize(SerialReader serialReader) throws IOException {
        final long k = fixedLong.deserialize(serialReader);
        final AggregationInstance of = serializer.deserialize(serialReader);
        return build(k, of);
    }

    protected abstract T build(long k, AggregationInstance of);
}
