/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.heroic.metric.bigtable;

import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.ByteString;
import com.spotify.heroic.common.Series;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import eu.toolchain.serializer.io.CoreByteArraySerialReader;
import java.io.EOFException;
import java.io.IOException;
import java.util.Optional;
import java.util.SortedMap;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsRowKeySerializer implements RowKeySerializer {
    final SerializerFramework framework;
    final Serializer<String> stringSerializer;
    final Serializer<SortedMap<String, String>> sortedMapSerializer;
    final Serializer<Long> longSerializer;

    public MetricsRowKeySerializer() {
        framework = TinySerializer.builder().string(CustomStringSerializer::new).build();
        stringSerializer = framework.string();
        sortedMapSerializer = framework.sortedMap(framework.string(), framework.string());
        longSerializer = framework.fixedLong();
    }

    @Override
    public void serialize(final SerialWriter serialWriter, final RowKey rowKey) throws IOException {
        stringSerializer.serialize(serialWriter, rowKey.getSeries().getKey());
        sortedMapSerializer.serialize(serialWriter, rowKey.getSeries().getTags());
        longSerializer.serialize(serialWriter, rowKey.getBase());

        // Only serialize if non-empty, for backwards compatibility
        if (!rowKey.getSeries().getResource().isEmpty()) {
            sortedMapSerializer.serialize(serialWriter, rowKey.getSeries().getResource());
        }
    }

    @Override
    public RowKey deserialize(final SerialReader serialReader) throws IOException {
        final String key = stringSerializer.deserialize(serialReader);
        final SortedMap<String, String> tags = sortedMapSerializer.deserialize(serialReader);
        final Long base = longSerializer.deserialize(serialReader);

        Optional<SortedMap<String, String>> resource = Optional.empty();
        try {
            resource = Optional.of(sortedMapSerializer.deserialize(serialReader));
        } catch (EOFException e) {
            // There was no 'resource' part in this RowKey - that's ok, for compatibility
        }

        return new RowKey(new Series(key, tags, resource.orElseGet(ImmutableSortedMap::of)), base);
    }

    public SortedMap<String, String> deserializeResourceFromSuffix(
        final ByteString rowKeyPrefix, final ByteString fullRowKey
    ) throws IOException {
        final int resourceEncodedLength = fullRowKey.size() - rowKeyPrefix.size();
        if (resourceEncodedLength == 0) {
            return ImmutableSortedMap.of();
        }

        final ByteString resourceEncoded = fullRowKey.substring(rowKeyPrefix.size());
        final byte[] bytes = resourceEncoded.toByteArray();
        final SerialReader reader = new CoreByteArraySerialReader(bytes);

        return sortedMapSerializer.deserialize(reader);
    }

}
