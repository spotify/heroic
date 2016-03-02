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

package com.spotify.heroic.metric.bigtable.api;

import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.grpc.scanner.ResultScanner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
@ToString
public class BigtableDataClientImpl implements BigtableDataClient {
    private final AsyncFramework async;
    private final com.google.cloud.bigtable.grpc.BigtableDataClient client;
    private final String clusterUri;

    public BigtableDataClientImpl(
        final AsyncFramework async, final com.google.cloud.bigtable.grpc.BigtableDataClient client,
        final String project, final String zone, final String cluster
    ) {
        this.async = async;
        this.client = client;
        this.clusterUri = String.format("projects/%s/zones/%s/clusters/%s", project, zone, cluster);
    }

    @Override
    public AsyncFuture<Void> mutateRow(
        String tableName, ByteString rowKey, Mutations mutations
    ) {
        return convertEmpty(client.mutateRowAsync(com.google.bigtable.v1.MutateRowRequest
            .newBuilder()
            .setTableName(Table.toURI(clusterUri, tableName))
            .setRowKey(rowKey)
            .addAllMutations(mutations.getMutations())
            .build()));
    }

    @Override
    public AsyncFuture<List<Row>> readRows(
        final String tableName, final ReadRowsRequest request
    ) {
        return convertRows(client.readRowsAsync(request.toPb(Table.toURI(clusterUri, tableName))));
    }

    @Override
    public AsyncFuture<Row> readModifyWriteRow(
        String tableName, ByteString rowKey, ReadModifyWriteRules rules
    ) {
        return convert(client.readModifyWriteRowAsync(ReadModifyWriteRowRequest
            .newBuilder()
            .setTableName(Table.toURI(clusterUri, tableName))
            .setRowKey(rowKey)
            .addAllRules(rules.getRules())
            .build())).directTransform(r -> convertRow(r));
    }

    @Override
    public AsyncObservable<Row> readRowsObserved(
        final String tableName, final ReadRowsRequest request
    ) {
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

                @Override
                public int available() {
                    return s.available();
                }
            };

            scanAsync(scanner, observer);
        };
    }

    AsyncFuture<List<Row>> convertRows(
        final ListenableFuture<List<com.google.bigtable.v1.Row>> readRowsAsync
    ) {
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
            families.put(family.getName(), new Family(family.getName(), family.getColumnsList()));
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

            f
                .onResolved(ign -> scanAsync(scanner, observer))
                .onFailed(observer::fail)
                .onCancelled(observer::cancel);
        }
    }

    private <T> AsyncFuture<T> convert(final ListenableFuture<T> request) {
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

    private AsyncFuture<Void> convertEmpty(final ListenableFuture<Empty> request) {
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
