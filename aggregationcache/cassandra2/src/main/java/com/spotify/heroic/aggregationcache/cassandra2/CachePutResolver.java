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

import java.util.List;
import java.util.concurrent.Callable;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregationcache.CacheBackendKey;
import com.spotify.heroic.aggregationcache.CacheBackendPutResult;
import com.spotify.heroic.aggregationcache.CacheKey;
import com.spotify.heroic.metric.Point;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
final class CachePutResolver implements Callable<CacheBackendPutResult> {
    // @formatter:off
    private static final String CQL_STMT =
            "INSERT INTO aggregations_1200 (aggregation_key, data_offset, data_value) VALUES(?, ?, ?)";
    // @formatter:on

    private final Serializer<CacheKey> cacheKeySerializer;
    private final Context ctx;
    private final ColumnFamily<Integer, String> columnFamily;
    private final CacheBackendKey key;
    private final List<Point> datapoints;

    @Override
    public CacheBackendPutResult call() throws Exception {
        final Keyspace keyspace = ctx.getClient();
        final AggregationInstance aggregation = key.getAggregation();
        final long size = aggregation.cadence();
        final long columnWidth = size * Cassandra2AggregationCacheBackend.WIDTH;

        for (final Point d : datapoints) {
            final double value = d.getValue();

            if (!Double.isFinite(value)) {
                continue;
            }

            final int index = (int) ((d.getTimestamp() % columnWidth) / size);
            final long base = d.getTimestamp() - d.getTimestamp() % columnWidth;
            final CacheKey key = new CacheKey(CacheKey.VERSION, this.key.getFilter(),
                    this.key.getGroup(), aggregation, base);
            doPut(keyspace, key, index, d);
        }

        return new CacheBackendPutResult();
    }

    private void doPut(Keyspace keyspace, CacheKey key, Integer dataOffset, Point d)
            throws ConnectionException {
        keyspace.prepareQuery(columnFamily).withCql(CQL_STMT).asPreparedStatement()
                .withByteBufferValue(key, cacheKeySerializer).withIntegerValue(dataOffset)
                .withDoubleValue(d.getValue()).execute();
    }
}
