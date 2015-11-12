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

package com.spotify.heroic.aggregationcache.cassandra2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregationcache.CacheBackendGetResult;
import com.spotify.heroic.aggregationcache.CacheBackendKey;
import com.spotify.heroic.aggregationcache.CacheKey;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.Point;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class CacheGetResolver implements Callable<CacheBackendGetResult> {
    static final String CQL_QUERY =
            "SELECT data_offset, data_value FROM aggregations_1200 WHERE aggregation_key = ?";

    private final Serializer<CacheKey> cacheKeySerializer;
    private final Context ctx;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final DateRange range;

    @Override
    public CacheBackendGetResult call() throws Exception {
        return new CacheBackendGetResult(key, doGetRow());
    }

    private List<Point> doGetRow() throws ConnectionException {
        final Keyspace keyspace = ctx.getClient();
        final AggregationInstance aggregation = key.getAggregation();
        final long columnSize = aggregation.cadence();

        final List<Long> bases = calculateBases(columnSize);

        final List<Point> datapoints = new ArrayList<Point>();

        for (final long base : bases) {
            final CacheKey cacheKey = new CacheKey(CacheKey.VERSION, key.getFilter(),
                    key.getGroup(), aggregation, base);

            final OperationResult<CqlResult<Integer, String>> op =
                    keyspace.prepareQuery(columnFamily).withCql(CQL_QUERY).asPreparedStatement()
                            .withByteBufferValue(cacheKey, cacheKeySerializer).execute();

            final CqlResult<Integer, String> result = op.getResult();
            final Rows<Integer, String> rows = result.getRows();

            for (final Row<Integer, String> row : rows) {
                final ColumnList<String> columns = row.getColumns();
                final int offset = columns.getColumnByIndex(0).getIntegerValue();
                final double value = columns.getColumnByIndex(1).getDoubleValue();

                final long timestamp = getDataPointTimestamp(base, columnSize, offset);

                if (timestamp < range.getStart() || timestamp >= range.getEnd()) {
                    continue;
                }

                datapoints.add(new Point(timestamp, value));
            }
        }

        return datapoints;
    }

    private List<Long> calculateBases(long columnWidth) {
        final List<Long> bases = new ArrayList<Long>();

        final long baseWidth = Cassandra2AggregationCacheBackend.WIDTH * columnWidth;
        final long first = range.getStart() - range.getStart() % baseWidth;
        final long last = range.getEnd() - range.getEnd() % baseWidth;

        for (long i = first; i <= last; i += baseWidth) {
            bases.add(i);
        }

        return bases;
    }

    private long getDataPointTimestamp(long base, long columnWidth, int dataOffset) {
        return base + columnWidth * dataOffset;
    }
}
