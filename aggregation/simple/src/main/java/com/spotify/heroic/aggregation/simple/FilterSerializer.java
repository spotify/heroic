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

import java.io.IOException;

public abstract class FilterSerializer<T extends FilterKInstance> implements Serializer<T> {
    private final AggregationSerializer serializer;

    public FilterSerializer(AggregationSerializer serializer) {
        this.serializer = serializer;
    }

    @Override
    public void serialize(SerialWriter buffer, T t) throws IOException {
        serializer.serialize(buffer, t.getOf());
        this.serializeNext(buffer, t);
    }

    @Override
    public T deserialize(SerialReader buffer) throws IOException {
        final AggregationInstance of = serializer.deserialize(buffer);
        return deserializeNext(buffer, of);
    }

    /**
     * This method needs to be implemented to complete the serialization of the specialization of
     * T.
     *
     * @param buffer buffer to serialize values into
     * @param value specialization of T
     * @throws IOException
     */
    protected abstract void serializeNext(SerialWriter buffer, T value) throws IOException;

    /**
     * This method needs to be implemented to complete the deserialization of the specialization of
     * T.
     *
     * @param buffer buffer to read from to deserialize
     * @param of deserialization of field "of"
     * @return an object T built from deserialization
     * @throws IOException
     */
    protected abstract T deserializeNext(SerialReader buffer, AggregationInstance of)
        throws IOException;
}
