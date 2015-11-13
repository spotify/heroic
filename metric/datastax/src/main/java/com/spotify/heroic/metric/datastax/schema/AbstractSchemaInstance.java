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

package com.spotify.heroic.metric.datastax.schema;

import java.nio.ByteBuffer;
import java.util.Optional;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyCriteria;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.datastax.MetricsRowKey;

import eu.toolchain.async.Transform;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractSchemaInstance implements SchemaInstance {
    protected final String key;
    protected final String keyspace;
    protected final String pointsTable;
    protected final String tokenKey;

    public AbstractSchemaInstance(final String key, final String keyspace,
            final String pointsTable) {
        this.key = key;
        this.keyspace = keyspace;
        this.pointsTable = pointsTable;
        this.tokenKey = String.format("token(%s)", key);
    }

    @Override
    public Select selectKeys() {
        return QueryBuilder.select().column(key).distinct().column(tokenKey).from(keyspace,
                pointsTable);
    }

    @Override
    public Clause keyGreaterOrEqual(final BackendKeyCriteria.KeyGreaterOrEqual criteria)
            throws Exception {
        final BackendKey k = criteria.getKey();
        final ByteBuffer buffer = rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return QueryBuilder.gte(key, buffer);
    }

    @Override
    public Clause keyLess(final BackendKeyCriteria.KeyLess criteria) throws Exception {
        final BackendKey k = criteria.getKey();
        final ByteBuffer buffer = rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return QueryBuilder.lt(key, buffer);
    }

    @Override
    public Clause keyPercentageGreaterOrEqual(
            final BackendKeyCriteria.PercentageGereaterOrEqual criteria) {
        final long value = percentageToToken(criteria.getPercentage());
        return QueryBuilder.gte(tokenKey, value);
    }

    @Override
    public Clause keyPercentageLess(final BackendKeyCriteria.PercentageLess criteria) {
        final long value = percentageToToken(criteria.getPercentage());
        return QueryBuilder.lt(tokenKey, value);
    }

    @Override
    public Transform<Row, BackendKey> keyConverter() {
        return row -> {
            final MetricsRowKey key = rowKey().deserialize(row.getBytes(this.key));
            return new BackendKey(key.getSeries(), key.getBase(), MetricType.POINT,
                    Optional.of(row.getLong(1)));
        };
    }

    static long percentageToToken(final float input) {
        final float r = Math.min(Math.max(input, 0.0f), 1.0f) * 2.0f;

        if (r < 1.0f) {
            return (long) (Long.MIN_VALUE * (1.0f - r));
        }

        return (long) (Long.MAX_VALUE * (r - 1.0f));
    }
}
