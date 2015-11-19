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

package com.spotify.heroic.metric.datastax.schema.ng;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.datastax.MetricsRowKey;
import com.spotify.heroic.metric.datastax.TypeSerializer;
import com.spotify.heroic.metric.datastax.schema.AbstractSchemaInstance;
import com.spotify.heroic.metric.datastax.schema.BackendKeyUtils;
import com.spotify.heroic.metric.datastax.schema.Schema.PreparedFetch;

import eu.toolchain.async.Transform;
import lombok.Data;

@Data
public class NextGenSchemaInstance extends AbstractSchemaInstance {
    public static final String KEY = "metric_key";
    public static final long MAX_WIDTH = Integer.MAX_VALUE;

    public static final TypeSerializer<MetricsRowKey> ROW_KEY = new MetricsRowKeySerializer();

    private final String keyspace;
    private final String pointsTable;
    private final PreparedStatement write;
    private final PreparedStatement fetch;
    private final PreparedStatement delete;
    private final PreparedStatement count;
    private final BackendKeyUtils keyUtils;

    public NextGenSchemaInstance(final String keyspace, final String pointsTable,
            final PreparedStatement write, final PreparedStatement fetch,
            final PreparedStatement delete, final PreparedStatement count) {
        super(KEY);
        this.keyspace = keyspace;
        this.pointsTable = pointsTable;
        this.write = write;
        this.fetch = fetch;
        this.delete = delete;
        this.count = count;
        this.keyUtils = new BackendKeyUtils(KEY, keyspace, pointsTable, this);
    }

    @Override
    public TypeSerializer<MetricsRowKey> rowKey() {
        return ROW_KEY;
    }

    @Override
    public BackendKeyUtils keyUtils() {
        return keyUtils;
    }

    @Override
    public WriteSession writeSession() {
        return new WriteSession() {
            final Map<Long, ByteBuffer> cache = new HashMap<>();

            @Override
            public BoundStatement writePoint(Series series, Point d) throws IOException {
                final long base = calculateBaseTimestamp(d.getTimestamp());

                ByteBuffer key = cache.get(base);

                if (key == null) {
                    key = ROW_KEY.serialize(new MetricsRowKey(series, base));
                    cache.put(base, key);
                }

                final int offset = calculateColumnKey(d.getTimestamp());
                return write.bind(key, offset, d.getValue());
            }
        };
    }

    @Override
    public List<PreparedFetch> ranges(final Series series, final DateRange range)
            throws IOException {
        final List<PreparedFetch> bases = new ArrayList<>();

        final long start = calculateBaseTimestamp(range.getStart());
        final long end = calculateBaseTimestamp(range.getEnd());

        for (long currentBase = start; currentBase <= end; currentBase += MAX_WIDTH) {
            final DateRange modified = range.modify(currentBase, currentBase + MAX_WIDTH);

            if (modified.isEmpty()) {
                continue;
            }

            final ByteBuffer key = ROW_KEY.serialize(new MetricsRowKey(series, currentBase));
            final int startColumn = calculateColumnKey(modified.start());
            final int endColumn = calculateColumnKey(modified.end());
            final long base = currentBase;

            bases.add(new PreparedFetch() {
                @Override
                public BoundStatement fetch(int limit) {
                    return fetch.bind(key, startColumn, endColumn, limit);
                }

                @Override
                public Transform<Row, Point> converter() {
                    return row -> {
                        final long timestamp = calculateAbsoluteTimestamp(base, row.getInt(0));
                        final double value = row.getDouble(1);
                        return new Point(timestamp, value);
                    };
                }

                @Override
                public String toString() {
                    return modified.toString();
                }
            });
        }

        return bases;
    }

    @Override
    public PreparedFetch row(final BackendKey key) throws IOException {
        final long base = key.getBase();

        final ByteBuffer k = ROW_KEY.serialize(new MetricsRowKey(key.getSeries(), base));

        return new PreparedFetch() {
            @Override
            public BoundStatement fetch(int limit) {
                return fetch.bind(k, Integer.MIN_VALUE, Integer.MAX_VALUE, limit);
            }

            @Override
            public Transform<Row, Point> converter() {
                return row -> {
                    final long timestamp = calculateAbsoluteTimestamp(base, row.getInt(0));
                    final double value = row.getDouble(1);
                    return new Point(timestamp, value);
                };
            }

            @Override
            public String toString() {
                return "<Fetch Row " + key + ">";
            }
        };
    }

    @Override
    public BoundStatement deleteKey(ByteBuffer k) {
        return delete.bind(k);
    }

    @Override
    public BoundStatement countKey(ByteBuffer k) {
        return count.bind(k);
    }

    static long calculateBaseTimestamp(final long timestamp) {
        return timestamp - timestamp % MAX_WIDTH;
    }

    static int calculateColumnKey(final long timestamp) {
        return (int) (timestamp % MAX_WIDTH);
    }

    static long calculateAbsoluteTimestamp(final long base, final int key) {
        return base + (long) key;
    }
}
