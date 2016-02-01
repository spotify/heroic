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

package com.spotify.heroic.metric.bigtable;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.grpc.BigtableDataClient;
import com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.metric.bigtable.api.Column;
import com.spotify.heroic.metric.bigtable.api.ColumnFamily;
import com.spotify.heroic.metric.bigtable.api.DataClient;
import com.spotify.heroic.metric.bigtable.api.Family;
import com.spotify.heroic.metric.bigtable.api.Mutations;
import com.spotify.heroic.metric.bigtable.api.MutationsBuilder;
import com.spotify.heroic.metric.bigtable.api.ReadModifyWriteRules;
import com.spotify.heroic.metric.bigtable.api.ReadModifyWriteRulesBuilder;
import com.spotify.heroic.metric.bigtable.api.ReadRowsRequest;
import com.spotify.heroic.metric.bigtable.api.Row;
import com.spotify.heroic.metric.bigtable.api.Table;
import com.spotify.heroic.metric.bigtable.api.TableAdminClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {"project", "zone", "cluster", "credentials"})
@RequiredArgsConstructor
public class BigtableConnectionBuilder implements Callable<BigtableConnection> {
    private static final String USER_AGENT = "heroic";

    private final String project;
    private final String zone;
    private final String cluster;

    private final CredentialsBuilder credentials;

    private final AsyncFramework async;
    private final ExecutorService executorService;

    @Override
    public BigtableConnection call() throws Exception {
        final CredentialOptions credentials = this.credentials.build();

        final BigtableOptions options = new BigtableOptions.Builder().setProjectId(project)
                .setZoneId(zone).setClusterId(cluster).setUserAgent(USER_AGENT)
                .setDataChannelCount(64).setCredentialOptions(credentials).build();

        final BigtableSession session = new BigtableSession(options, executorService);

        final TableAdminClient adminClient =
                new BigtableAdminClientImpl(session.getTableAdminClient());
        final DataClient client = new BigtableClientImpl(session.getDataClient());

        return new GrpcBigtableConnection(project, zone, cluster, session, adminClient, client);
    }

    @RequiredArgsConstructor
    @ToString(of = {"project", "zone", "cluster"})
    public static class GrpcBigtableConnection implements BigtableConnection {
        private final String project;
        private final String zone;
        private final String cluster;

        final BigtableSession session;
        final TableAdminClient adminClient;
        final DataClient client;

        @Override
        public TableAdminClient adminClient() {
            return adminClient;
        }

        @Override
        public DataClient client() {
            return client;
        }

        @Override
        public void close() throws Exception {
            session.close();
        }
    }

    @RequiredArgsConstructor
    @ToString
    class BigtableAdminClientImpl implements TableAdminClient {
        final com.google.cloud.bigtable.grpc.BigtableTableAdminClient client;

        final String clusterUri =
                String.format("projects/%s/zones/%s/clusters/%s", project, zone, cluster);

        @Override
        public Optional<Table> getTable(String name) {
            try {
                return Optional.of(Table
                        .fromPb(client.getTable(com.google.bigtable.admin.table.v1.GetTableRequest
                                .newBuilder().setName(Table.toURI(clusterUri, name)).build())));
            } catch (final UncheckedExecutionException e) {
                if (e.getCause() instanceof StatusRuntimeException) {
                    final StatusRuntimeException s = (StatusRuntimeException) e.getCause();

                    if (s.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                        return Optional.empty();
                    }
                }

                throw e;
            }
        }

        @Override
        public Table createTable(String tableId) {
            client.createTable(com.google.bigtable.admin.table.v1.CreateTableRequest.newBuilder()
                    .setName(clusterUri).setTableId(tableId).build());
            return new Table(clusterUri, tableId);
        }

        @Override
        public ColumnFamily createColumnFamily(Table table, String name) {
            // name MUST be empty during creation, do not set it.
            final com.google.bigtable.admin.table.v1.ColumnFamily cf =
                    com.google.bigtable.admin.table.v1.ColumnFamily.newBuilder().build();

            client.createColumnFamily(com.google.bigtable.admin.table.v1.CreateColumnFamilyRequest
                    .newBuilder().setName(table.toURI()).setColumnFamilyId(name).setColumnFamily(cf)
                    .build());

            return new ColumnFamily(clusterUri, table.getName(), name);
        }
    }

    @RequiredArgsConstructor
    @ToString
    class BigtableClientImpl implements DataClient {
        final BigtableDataClient client;

        final String clusterUri =
                String.format("projects/%s/zones/%s/clusters/%s", project, zone, cluster);

        @Override
        public AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey,
                Mutations mutations) {
            return convertEmpty(client
                    .mutateRowAsync(MutateRowRequest.newBuilder().setTableName(tableName(tableName))
                            .setRowKey(rowKey).addAllMutations(mutations.getMutations()).build()));
        }

        @Override
        public MutationsBuilder mutations() {
            return new MutationsBuilder();
        }

        @Override
        public ReadModifyWriteRulesBuilder readModifyWriteRules() {
            return new ReadModifyWriteRulesBuilder();
        };

        @Override
        public AsyncFuture<List<Row>> readRows(final String tableName,
                final ReadRowsRequest request) {
            return convertRows(
                    client.readRowsAsync(request.toPb(Table.toURI(clusterUri, tableName))));
        }

        @Override
        public AsyncFuture<Row> readModifyWriteRow(String tableName, ByteString rowKey,
                ReadModifyWriteRules rules) {
            return convert(client.readModifyWriteRowAsync(
                    ReadModifyWriteRowRequest.newBuilder().setTableName(tableName(tableName))
                            .setRowKey(rowKey).addAllRules(rules.getRules()).build()))
                                    .directTransform(r -> convertRow(r));
        }

        @Override
        public AsyncObservable<Row> readRowsObserved(final String tableName,
                final ReadRowsRequest request) {
            return observer -> {
                final ResultScanner<com.google.bigtable.v1.Row> s =
                        client.readRows(request.toPb(Table.toURI(clusterUri, tableName)));

                final ResultScanner<Row> scanner = new ResultScanner<Row>() {
                    @Override
                    public void close() throws IOException {
                        s.close();
                    }

                    @Override
                    public Row next() throws IOException {
                        final com.google.bigtable.v1.Row n = s.next();

                        if (n == null) {
                            return null;
                        }

                        return convertRow(n);
                    }

                    @Override
                    public Row[] next(int count) throws IOException {
                        final com.google.bigtable.v1.Row[] rows = s.next(count);

                        final Row[] results = new Row[rows.length];

                        for (int i = 0; i < rows.length; i++) {
                            results[i++] = convertRow(rows[i]);
                        }

                        return results;
                    }
                };

                scanAsync(scanner, observer);
            };
        };

        String columnFamilyUriToName(String tableName, String name) {
            final String tableUri = tableNameToUri(tableName);

            if (!name.startsWith(tableUri)) {
                throw new IllegalArgumentException(String.format(
                        "Somehow you are converting a table (%s) from a different cluster (%s)",
                        name, tableUri));
            }

            return name.substring(tableUri.length() + "/columnFamilies/".length(), name.length());
        }

        String tableNameToUri(String table) {
            return String.format("%s/tables/%s", clusterUri, table);
        }

        String tableName(String tableName) {
            return String.format("%s/tables/%s", clusterUri, tableName);
        }

        AsyncFuture<List<Row>> convertRows(
                final ListenableFuture<List<com.google.bigtable.v1.Row>> readRowsAsync) {
            return convert(readRowsAsync).directTransform(result -> {
                final List<Row> rows = new ArrayList<>();

                for (final com.google.bigtable.v1.Row row : result) {
                    rows.add(convertRow(row));
                }

                return rows;
            });
        }

        Row convertRow(final com.google.bigtable.v1.Row row) {
            final ImmutableMap.Builder<String, Family> families = ImmutableMap.builder();

            for (final com.google.bigtable.v1.Family family : row.getFamiliesList()) {
                final Iterable<Column> columns = Family.makeColumnIterable(family);
                families.put(family.getName(), new Family(family.getName(), columns));
            }

            return new Row(row.getKey(), families.build());
        }

        <T> void scanAsync(ResultScanner<T> scanner, AsyncObserver<T> observer) throws Exception {
            while (true) {
                final T n;

                /* this will unfortunately block once in a while */
                try {
                    n = scanner.next();
                } catch (final Exception e) {
                    observer.fail(e);
                    return;
                }

                if (n == null) {
                    observer.end();
                    return;
                }

                final AsyncFuture<Void> f = observer.observe(n);

                // if already resolved, avoid adding more stack frames.
                if (f.isDone()) {
                    if (f.isFailed()) {
                        observer.fail(f.cause());
                        break;
                    }

                    if (f.isCancelled()) {
                        observer.cancel();
                        break;
                    }

                    continue;
                }

                f.onResolved(ign -> scanAsync(scanner, observer)).onFailed(observer::fail)
                        .onCancelled(observer::cancel);
            }
        }
    }

    <T> AsyncFuture<T> convert(final ListenableFuture<T> request) {
        final ResolvableFuture<T> future = async.future();

        Futures.addCallback(request, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                future.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }

    AsyncFuture<Void> convertEmpty(final ListenableFuture<Empty> request) {
        final ResolvableFuture<Void> future = async.future();

        Futures.addCallback(request, new FutureCallback<Empty>() {
            @Override
            public void onSuccess(Empty result) {
                future.resolve(null);
            }

            @Override
            public void onFailure(Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }
}
