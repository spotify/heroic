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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyClause;
import com.spotify.heroic.metric.BackendKeyClause.Limited;
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

    public SchemaBoundStatement selectKeys(final BackendKeyClause clause) throws Exception {
        final SelectBuilder select = new SelectBuilder().distinct().column(column)
                .column(columnToken).from(keyspace, table);

        applyClause(clause, select);

        return select.toBoundStatement();
    }

    void applyClause(final BackendKeyClause clause, final SelectBuilder select) throws Exception {
        // apply a limit clause.
        if (clause instanceof BackendKeyClause.Limited) {
            final Limited limited = BackendKeyClause.Limited.class.cast(clause);

            select.limit(limited.getLimit());

            applyClause(limited.getClause(), select);
            return;
        }

        final List<SchemaBoundStatement> statements = new ArrayList<>();

        // apply multiple clauses with 'and' between them.
        if (clause instanceof BackendKeyClause.And) {
            final BackendKeyClause.And and = (BackendKeyClause.And) clause;

            for (final BackendKeyClause c : and.getClauses()) {
                applySimpleClause(c, statements::add);
            }
        } else {
            applySimpleClause(clause, statements::add);
        }

        select.and(statements);
    }

    void applySimpleClause(final BackendKeyClause clause,
            final Consumer<SchemaBoundStatement> consumer) throws Exception {
        if (clause instanceof BackendKeyClause.All) {
            return;
        }

        if (clause instanceof BackendKeyClause.GTE) {
            consumer.accept(gte(BackendKeyClause.GTE.class.cast(clause)));
            return;
        }

        if (clause instanceof BackendKeyClause.LT) {
            consumer.accept(lt(BackendKeyClause.LT.class.cast(clause)));
            return;
        }

        if (clause instanceof BackendKeyClause.GTEPercentage) {
            consumer.accept(gtePercentage(BackendKeyClause.GTEPercentage.class.cast(clause)));
            return;
        }

        if (clause instanceof BackendKeyClause.LTPercentage) {
            consumer.accept(ltPercentage(BackendKeyClause.LTPercentage.class.cast(clause)));
            return;
        }

        if (clause instanceof BackendKeyClause.GTEToken) {
            consumer.accept(gteToken(BackendKeyClause.GTEToken.class.cast(clause)));
            return;
        }

        if (clause instanceof BackendKeyClause.LTToken) {
            consumer.accept(ltToken(BackendKeyClause.LTToken.class.cast(clause)));
            return;
        }

        throw new IllegalArgumentException("Unsupported clause: " + clause);
    }

    SchemaBoundStatement gte(final BackendKeyClause.GTE clause) throws Exception {
        final BackendKey k = clause.getKey();
        final ByteBuffer buffer =
                schema.rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return new SchemaBoundStatement(columnToken + " >= token(?)", ImmutableList.of(buffer));
    }

    SchemaBoundStatement lt(final BackendKeyClause.LT clause) throws Exception {
        final BackendKey k = clause.getKey();
        final ByteBuffer buffer =
                schema.rowKey().serialize(new MetricsRowKey(k.getSeries(), k.getBase()));
        return new SchemaBoundStatement(columnToken + " < token(?)", ImmutableList.of(buffer));
    }

    SchemaBoundStatement gtePercentage(final BackendKeyClause.GTEPercentage clause) {
        final long value = percentageToToken(clause.getPercentage());
        return new SchemaBoundStatement(columnToken + " >= ?", ImmutableList.of(value));
    }

    SchemaBoundStatement ltPercentage(final BackendKeyClause.LTPercentage clause) {
        final long value = percentageToToken(clause.getPercentage());
        return new SchemaBoundStatement(columnToken + " < ?", ImmutableList.of(value));
    }

    SchemaBoundStatement gteToken(final BackendKeyClause.GTEToken clause) {
        return new SchemaBoundStatement(columnToken + " >= ?", ImmutableList.of(clause.getToken()));
    }

    SchemaBoundStatement ltToken(final BackendKeyClause.LTToken clause) {
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
