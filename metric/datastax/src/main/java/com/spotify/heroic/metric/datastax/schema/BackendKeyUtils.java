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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.datastax.MetricsRowKey;

/**
 * Utility functions to manipulate and query backend keys.
 *
 * @author udoprog
 */
public class BackendKeyUtils {
    private final String column;
    private final String columnToken;
    private final String keyspace;
    private final String table;
    private final SchemaInstance schema;

    public BackendKeyUtils(final String column, final String keyspace, final String table,
            final SchemaInstance schema) {
        this.column = column;
        this.columnToken = String.format("token(%s)", column);
        this.keyspace = keyspace;
        this.table = table;
        this.schema = schema;
    }

    public SchemaBoundStatement selectKeys(final BackendKeyFilter filter) throws Exception {
        final SelectBuilder select = new SelectBuilder().distinct().column(column)
                .column(columnToken).from(keyspace, table);

        filter.getStart().map(this::convertStart).ifPresent(select::and);
        filter.getEnd().map(this::convertEnd).ifPresent(select::and);
        filter.getLimit().ifPresent(select::limit);

        return select.toBoundStatement();
    }

    SchemaBoundStatement convertStart(final BackendKeyFilter.Start start) throws RuntimeException {
        if (start instanceof BackendKeyFilter.GT) {
            try {
                return gt(BackendKeyFilter.GT.class.cast(start));
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (start instanceof BackendKeyFilter.GTE) {
            try {
                return gte(BackendKeyFilter.GTE.class.cast(start));
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (start instanceof BackendKeyFilter.GTEPercentage) {
            return gtePercentage(BackendKeyFilter.GTEPercentage.class.cast(start));
        }

        if (start instanceof BackendKeyFilter.GTEToken) {
            return gteToken(BackendKeyFilter.GTEToken.class.cast(start));
        }

        throw new IllegalArgumentException("Unsupported clause: " + start);
    }

    SchemaBoundStatement convertEnd(final BackendKeyFilter.End end) throws RuntimeException {
        if (end instanceof BackendKeyFilter.LT) {
            try {
                return lt(BackendKeyFilter.LT.class.cast(end));
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (end instanceof BackendKeyFilter.LTPercentage) {
            return ltPercentage(BackendKeyFilter.LTPercentage.class.cast(end));
        }

        if (end instanceof BackendKeyFilter.LTToken) {
            return ltToken(BackendKeyFilter.LTToken.class.cast(end));
        }

        throw new IllegalArgumentException("Unsupported clause: " + end);
    }

    SchemaBoundStatement gt(final BackendKeyFilter.GT clause) throws Exception {
        final BackendKey k = clause.getKey();
        final ByteBuffer buffer =
                schema.rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return new SchemaBoundStatement(columnToken + " > token(?)", ImmutableList.of(buffer));
    }

    SchemaBoundStatement gte(final BackendKeyFilter.GTE clause) throws Exception {
        final BackendKey k = clause.getKey();
        final ByteBuffer buffer =
                schema.rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return new SchemaBoundStatement(columnToken + " >= token(?)", ImmutableList.of(buffer));
    }

    SchemaBoundStatement gtePercentage(final BackendKeyFilter.GTEPercentage clause) {
        final long value = percentageToToken(clause.getPercentage());
        return new SchemaBoundStatement(columnToken + " >= ?", ImmutableList.of(value));
    }

    SchemaBoundStatement gteToken(final BackendKeyFilter.GTEToken clause) {
        return new SchemaBoundStatement(columnToken + " >= ?", ImmutableList.of(clause.getToken()));
    }

    SchemaBoundStatement lt(final BackendKeyFilter.LT clause) throws Exception {
        final BackendKey k = clause.getKey();
        final ByteBuffer buffer =
                schema.rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return new SchemaBoundStatement(columnToken + " < token(?)", ImmutableList.of(buffer));
    }

    SchemaBoundStatement ltPercentage(final BackendKeyFilter.LTPercentage clause) {
        final long value = percentageToToken(clause.getPercentage());
        return new SchemaBoundStatement(columnToken + " < ?", ImmutableList.of(value));
    }

    SchemaBoundStatement ltToken(final BackendKeyFilter.LTToken clause) {
        return new SchemaBoundStatement(columnToken + " < ?", ImmutableList.of(clause.getToken()));
    }

    static long percentageToToken(final float input) {
        final float r = Math.min(Math.max(input, 0.0f), 1.0f) * 2.0f;

        if (r < 1.0f) {
            return (long) (Long.MIN_VALUE * (1.0f - r));
        }

        return (long) (Long.MAX_VALUE * (r - 1.0f));
    }
}
