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

package com.spotify.heroic.metric.astyanax;

import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.AbstractSerializer;
import com.netflix.astyanax.serializers.LongSerializer;
import com.spotify.heroic.common.Series;

import java.nio.ByteBuffer;

class MetricsRowKeySerializer extends AbstractSerializer<MetricsRowKey> {
    public static final MetricsRowKeySerializer instance = new MetricsRowKeySerializer();

    private static final SeriesSerializer seriesSerializer = SeriesSerializer.get();
    private static final LongSerializer longSerializer = LongSerializer.get();

    public static MetricsRowKeySerializer get() {
        return instance;
    }

    @Override
    public ByteBuffer toByteBuffer(MetricsRowKey rowKey) {
        final Composite composite = new Composite();
        composite.addComponent(rowKey.getSeries(), seriesSerializer);
        composite.addComponent(rowKey.getBase(), longSerializer);
        return composite.serialize();
    }

    @Override
    public MetricsRowKey fromByteBuffer(ByteBuffer byteBuffer) {
        final Composite composite = Composite.fromByteBuffer(byteBuffer);
        final Series series = composite.get(0, seriesSerializer);
        final Long base = composite.get(1, longSerializer);
        return new MetricsRowKey(series, base);
    }

    public static long getBaseTimestamp(long timestamp) {
        return timestamp - timestamp % MetricsRowKey.MAX_WIDTH;
    }

    public static int calculateColumnKey(long timestamp) {
        // This is because column key ranges from Integer.MIN_VALUE to
        // Integer.MAX_VALUE
        final long shift = (long) Integer.MAX_VALUE + 1;

        timestamp = timestamp + shift;
        return (int) (timestamp - MetricsRowKeySerializer.getBaseTimestamp(timestamp));
    }

    public static long calculateAbsoluteTimestamp(long base, int columnKey) {
        // This is because column key ranges from Integer.MIN_VALUE to
        // Integer.MAX_VALUE
        final long shift = (long) Integer.MAX_VALUE + 1;

        final long timestamp = base + columnKey + shift;
        return timestamp;
    }
}
