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

package com.spotify.heroic.metric.datastax.serializer;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import com.spotify.heroic.model.Series;

public class SeriesSerializer implements CustomSerializer<Series> {
    private final CustomSerializer<String> key = new StringSerializer();
    private final CustomSerializer<Map<String, String>> tags = new MapSerializer<>(new StringSerializer(),
            new StringSerializer());

    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        public int compare(String a, String b) {
            if (a == null || b == null) {
                if (a == null)
                    return -1;

                if (b == null)
                    return 1;

                return 0;
            }

            return a.compareTo(b);
        }
    };

    @Override
    public ByteBuffer serialize(Series value) {
        final TreeMap<String, String> sorted = new TreeMap<>(COMPARATOR);
        sorted.putAll(value.getTags());

        final CompositeComposer composer = new CompositeComposer();
        composer.add(value.getKey(), this.key);
        composer.add(sorted, this.tags);
        return composer.serialize();
    }

    @Override
    public Series deserialize(ByteBuffer buffer) {
        final CompositeStream reader = new CompositeStream(buffer);
        final String key = reader.next(this.key);
        final Map<String, String> tags = reader.next(this.tags);
        return new Series(key, tags);
    }
}
