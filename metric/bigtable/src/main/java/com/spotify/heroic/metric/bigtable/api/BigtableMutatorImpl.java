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

import com.google.bigtable.v2.MutateRowRequest;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.cloud.bigtable.grpc.async.BulkMutation;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BigtableMutatorImpl implements BigtableMutator {
    private final AsyncFramework async;
    private final com.google.cloud.bigtable.grpc.BigtableSession
        session;
    private final boolean disableBulkMutations;
    private final Map<String, BulkMutation> tableToBulkMutation;
    private final ScheduledExecutorService scheduler;
    private final Object tableAccessLock = new Object();
    private final Object flushLock = new Object();
    private final Tracer tracer = Tracing.getTracer();

    public BigtableMutatorImpl(
        AsyncFramework async,
        com.google.cloud.bigtable.grpc.BigtableSession session,
        boolean disableBulkMutations,
        int flushIntervalSeconds
    ) {
        this.async = async;
        this.session = session;
        this.disableBulkMutations = disableBulkMutations;

        if (disableBulkMutations) {
            this.scheduler = null;
            this.tableToBulkMutation = null;
        } else {
            this.tableToBulkMutation = new HashMap<>();
            this.scheduler = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat("heroic-bigtable-flush").build());
            scheduler.scheduleAtFixedRate(this::flush, 0, flushIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    @Override
    public AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey, Mutations mutations) {
        if (disableBulkMutations) {
            return mutateSingleRow(tableName, rowKey, mutations);
        } else {
            return mutateBatchRow(tableName, rowKey, mutations);
        }
    }

    @Override
    public AsyncFuture<Void> close() {
        if (scheduler == null) {
            return async.resolved();
        }

        return async.call(() -> {
            scheduler.shutdownNow();

            try {
                scheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                log.error("Failed to shut down bigtable flush executor service in a timely manner");
            }

            return null;
        });
    }

    private AsyncFuture<Void> mutateSingleRow(
        String tableName, ByteString rowKey, Mutations mutations
    ) {
        try (Scope ss = tracer.spanBuilder("BigtableMutator.mutateSingleRow").startScopedSpan()) {
            final Span span = tracer.getCurrentSpan();
            return convertVoid(
                session
                    .getDataClient()
                    .mutateRowAsync(toMutateRowRequest(tableName, rowKey, mutations)))
                .onFinished(span::end);
        }
    }

    private AsyncFuture<Void> mutateBatchRow(
        String tableName, ByteString rowKey, Mutations mutations
    ) {
        try (Scope ss = tracer.spanBuilder("BigtableMutator.mutateBatchRow").startScopedSpan()) {
            final Span span = tracer.getCurrentSpan();
            span.putAttribute("table", AttributeValue.stringAttributeValue(tableName));

            final BulkMutation bulkMutation = getOrAddBulkMutation(tableName);

            span.addAnnotation("Adding rows to bulk mutation");
            return convertVoid(bulkMutation.add(toMutateRowRequest(tableName, rowKey, mutations)))
                .onFinished(span::end);
        }
    }

    private BulkMutation getOrAddBulkMutation(String tableName) {
        final String spanName = "BigtableMutator.getOrAddBulkMutation";
        try (Scope ss = tracer.spanBuilder(spanName).startScopedSpan()) {
            final Span span = tracer.getCurrentSpan();
            span.addAnnotation("Acquiring lock");
            synchronized (tableAccessLock) {
                span.addAnnotation("Lock acquired");

                if (tableToBulkMutation.containsKey(tableName)) {
                    return tableToBulkMutation.get(tableName);
                }

                final BulkMutation bulkMutation = session.createBulkMutation(
                    session
                        .getOptions()
                        .getInstanceName()
                        .toTableName(tableName),
                    session.createAsyncExecutor());

                tableToBulkMutation.put(tableName, bulkMutation);

                return bulkMutation;
            }
        }
    }

    private MutateRowRequest toMutateRowRequest(
        String tableName,
        ByteString rowKey,
        Mutations mutations
    ) {
        return MutateRowRequest
            .newBuilder()
            .setTableName(session.getOptions().getInstanceName().toTableNameStr(tableName))
            .setRowKey(rowKey)
            .addAllMutations(mutations.getMutations())
            .build();
    }

    private <T> AsyncFuture<Void> convertVoid(final ListenableFuture<T> request) {
        final ResolvableFuture<Void> future = async.future();

        Futures.addCallback(request, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                future.resolve(null);
            }

            @Override
            public void onFailure(Throwable t) {
                future.fail(t);
            }
        });

        return future;
    }

    private void flush() {
        synchronized (flushLock) {
            tableToBulkMutation.values().stream().forEach(mutation -> {
                try {
                    mutation.flush();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
