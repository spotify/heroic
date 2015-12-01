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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

/**
 * Rage re-inventing QueryBuilder since I can't get it to do what I need.
 *
 * @author udoprog
 */
public class SelectBuilder {
    public static final Joiner COLUMN_JOINER = Joiner.on(", ");
    public static final Joiner AND_JOINER = Joiner.on(" and ");

    private boolean distinct = false;
    private List<String> columns = new ArrayList<>();
    private List<Object> columnsBound = new ArrayList<>();
    private Optional<String> from = Optional.empty();
    private Optional<Object> limit = Optional.empty();

    private List<SchemaBoundStatement> and = new ArrayList<>();

    public SchemaBoundStatement toBoundStatement() {
        if (columns.isEmpty()) {
            throw new IllegalStateException("No columns selected");
        }

        if (!from.isPresent()) {
            throw new IllegalStateException("No table selected");
        }

        final StringBuilder builder = new StringBuilder();
        final ImmutableList.Builder<Object> bindings = ImmutableList.builder();

        builder.append("SELECT").append(" ");

        if (distinct) {
            builder.append("DISTINCT").append(" ");
        }

        builder.append(COLUMN_JOINER.join(columns)).append(" ");
        bindings.addAll(columnsBound);

        builder.append("FROM " + from.get());

        if (!and.isEmpty()) {
            builder.append(" WHERE ");
            builder.append(AND_JOINER
                    .join(and.stream().map(SchemaBoundStatement::getStatement).iterator()));
            and.stream().map(SchemaBoundStatement::getBindings).forEach(bindings::addAll);
        }

        limit.ifPresent(limit -> {
            builder.append(" LIMIT ?");
            bindings.add(limit);
        });

        return new SchemaBoundStatement(builder.toString(), bindings.build());
    }

    public SelectBuilder limit(Object limit) {
        if (!this.limit.isPresent()) {
            this.limit = Optional.of(limit);
        }

        return this;
    }

    public SelectBuilder from(final String keyspace, final String table) {
        this.from = Optional.of(keyspace + "." + table);
        return this;
    }

    public SelectBuilder column(final String name, Object... bound) {
        columns.add(name);
        return this;
    }

    public SelectBuilder and(SchemaBoundStatement statement) {
        and.add(statement);
        return this;
    }

    public SelectBuilder distinct() {
        this.distinct = true;
        return this;
    }
}
