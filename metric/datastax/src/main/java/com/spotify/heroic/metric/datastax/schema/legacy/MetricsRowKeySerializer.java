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

package com.spotify.heroic.metric.datastax.schema.legacy;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;

import java.io.IOException;
import java.nio.ByteBuffer;

public class MetricsRowKeySerializer implements TypeSerializer<MetricsRowKey> {
    private final TypeSerializer<Series> series = new SeriesSerializer();
    private final TypeSerializer<Long> longNumber = new LongSerializer();

    @Override
    public ByteBuffer serialize(MetricsRowKey value) throws IOException {
        final CompositeComposer composer = new CompositeComposer();
        composer.add(value.getSeries(), this.series);
        composer.add(value.getBase(), this.longNumber);
        return composer.serialize();
    }

    @Override
    public MetricsRowKey deserialize(ByteBuffer buffer) throws IOException {
        final CompositeStream reader = new CompositeStream(buffer);
        final Series series = reader.next(this.series);
        final long base = reader.next(this.longNumber);
        return new MetricsRowKey(series, base);
    }
}
