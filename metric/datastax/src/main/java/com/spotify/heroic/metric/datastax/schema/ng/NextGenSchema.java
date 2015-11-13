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
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.datastax.Async;
import com.spotify.heroic.metric.datastax.schema.AbstractCassandraSchema;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.SchemaInstance;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NextGenSchema extends AbstractCassandraSchema implements Schema {
    public static final String CREATE_KEYSPACE_CQL =
            NextGenSchema.class.getPackage().getName() + "/keyspace.cql";
    public static final String CREATE_TABLES_CQL =
            NextGenSchema.class.getPackage().getName() + "/tables.cql";

    public static final String POINTS_TABLE = "metrics";

    // @formatter:off
    private static final String WRITE_METRICS_CQL =
            "INSERT INTO {{keyspace}}.metrics (metric_key, data_timestamp_offset, data_value) VALUES (?, ?, ?)";
    private static final String FETCH_METRICS_CQL =
            "SELECT data_timestamp_offset, data_value FROM {{keyspace}}.metrics WHERE metric_key = ? and data_timestamp_offset >= ? and data_timestamp_offset <= ? LIMIT ?";
    private static final String DELETE_METRICS_CQL =
            "DELETE FROM {{keyspace}}.metrics WHERE metric_key = ?";
    private static final String COUNT_METRICS_CQL =
            "SELECT count(*) FROM {{keyspace}}.metrics WHERE metric_key = ?";
    // @formatter:on

    private final String keyspace;

    public NextGenSchema(final AsyncFramework async, final String keyspace) {
        super(async);
        this.keyspace = keyspace;
    }

    @Override
    public AsyncFuture<Void> configure(final Session s) {
        final Map<String, String> values = ImmutableMap.of("keyspace", keyspace);

        final AsyncFuture<PreparedStatement> createKeyspace;

        try {
            createKeyspace = prepareTemplate(values, s, CREATE_KEYSPACE_CQL);
        } catch (IOException e) {
            return async.failed(e);
        }

        return createKeyspace.lazyTransform(createKeyspaceStmt -> {
            log.info("Creating keyspace {}", keyspace);

            return Async.bind(async, s.executeAsync(createKeyspaceStmt.bind()))
                    .lazyTransform(ign -> {
                final AsyncFuture<PreparedStatement> createTables =
                        prepareTemplate(values, s, CREATE_TABLES_CQL);

                return createTables.lazyTransform(createTablesStmt -> {
                    log.info("Creating tables for keyspace {}", keyspace);
                    return Async.bind(async, s.executeAsync(createTablesStmt.bind()))
                            .directTransform(ign2 -> null);
                });
            });
        });
    }

    @Override
    public AsyncFuture<SchemaInstance> instance(final Session s) {
        final Map<String, String> values = ImmutableMap.of("keyspace", keyspace);

        final AsyncFuture<PreparedStatement> write = prepareAsync(values, s, WRITE_METRICS_CQL);
        final AsyncFuture<PreparedStatement> fetch = prepareAsync(values, s, FETCH_METRICS_CQL);
        final AsyncFuture<PreparedStatement> delete = prepareAsync(values, s, DELETE_METRICS_CQL);
        final AsyncFuture<PreparedStatement> count = prepareAsync(values, s, COUNT_METRICS_CQL);

        return async.collectAndDiscard(ImmutableList.of(write, fetch, delete, count))
                .directTransform(r -> {
                    return new NextGenSchemaInstance(keyspace, POINTS_TABLE, write.getNow(),
                            fetch.getNow(), delete.getNow(), count.get());
                });
    }
}
