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
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.common.Series;
import eu.toolchain.serializer.AutoSerialize;
import eu.toolchain.serializer.BytesSerialWriter;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;
import eu.toolchain.serializer.io.ContinuousSharedPool;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedMap;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsRowKeySerializer implements RowKeySerializer {
    final SerializerFramework framework;
    final SerializerFramework suffixFramework;
    final RowKeyMinimal_Serializer minimalSerializer;
    final Serializer<SortedMap<String, String>> sortedMap;
    final Serializer<List<SuffixEntry>> suffixList;

    public MetricsRowKeySerializer() {
        framework = TinySerializer.builder().string(CustomStringSerializer::new).build();
        suffixFramework = TinySerializer.builder().build();
        minimalSerializer = new RowKeyMinimal_Serializer(framework);
        sortedMap = suffixFramework.sortedMap(suffixFramework.string(), suffixFramework.string());

        suffixList = suffixFramework.list(
            new MetricsRowKeySerializer_SuffixEntry_Serializer(suffixFramework));
    }

    @Override
    public ByteString serializeMinimal(final RowKeyMinimal value) throws IOException {
        try (final BytesSerialWriter serialWriter = framework.writeBytes()) {
            minimalSerializer.serialize(serialWriter, value);
            return ByteString.copyFrom(serialWriter.toByteArray());
        }
    }

    @Override
    public ByteString serializeFull(final RowKey rowKey) throws IOException {
        try (final BytesSerialWriter serialWriter = framework.writeBytes()) {
            minimalSerializer.serialize(serialWriter, RowKeyMinimal.create(rowKey));

            final List<SuffixEntry> suffixes = new ArrayList<>();

            // Only serialize if non-empty, for backwards compatibility
            if (!rowKey.getSeries().getResource().isEmpty()) {
                final byte[] payload = fromByteBuffer(
                    suffixFramework.serialize(sortedMap, rowKey.getSeries().getResource()));
                suffixes.add(new SuffixEntry(SuffixEntryType.RESOURCE, payload));
            }

            if (!suffixes.isEmpty()) {
                suffixList.serialize(serialWriter, suffixes);
            }

            return ByteString.copyFrom(serialWriter.toByteArray());
        }
    }

    private byte[] fromByteBuffer(ByteBuffer buf) {
        byte[] array = new byte[buf.remaining()];
        buf.get(array);
        return array;
    }

    @Override
    public RowKey deserializeFull(final ByteBuffer buffer) throws IOException {
        final SerialReader serialReader =
            new BigtableByteBufferSerialReader(new ContinuousSharedPool(),
                framework.variableInteger(), buffer);
        final RowKeyMinimal rowKeyMinimal = minimalSerializer.deserialize(serialReader);

        Optional<SortedMap<String, String>> resourceMaybe = Optional.empty();

        if (buffer.remaining() > 0) {
            // Only try to parse suffixes (such as 'resource') when there actually is data for it in
            // the serialization For backwards compatibility
            for (final SuffixEntry e : suffixFramework.deserialize(suffixList, buffer)) {
                switch (e.type) {
                    case RESOURCE:
                        resourceMaybe = Optional.of(
                            suffixFramework.deserialize(sortedMap, ByteBuffer.wrap(e.payload)));
                        break;
                    default:
                        throw new RuntimeException("Unknown suffix type in RowKey");
                }
            }
        }

        final RowKeyMinimal.Series s = rowKeyMinimal.getSeries();
        final SortedMap<String, String> resource = resourceMaybe.orElseGet(ImmutableSortedMap::of);
        return new RowKey(new Series(s.getKey(), s.getTags(), resource), rowKeyMinimal.getBase());
    }

    enum SuffixEntryType {
        RESOURCE
    }

    @AutoSerialize
    @Data
    public static class SuffixEntry {
        private final SuffixEntryType type;
        private final byte[] payload;
    }
}
