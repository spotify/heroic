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

package com.spotify.heroic.analytics.bigtable;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.analytics.SeriesHit;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import com.spotify.heroic.metric.bigtable.api.Family;
import com.spotify.heroic.metric.bigtable.api.ReadModifyWriteRules;
import com.spotify.heroic.metric.bigtable.api.ReadRowsRequest;
import com.spotify.heroic.metric.bigtable.api.Row;
import com.spotify.heroic.metric.bigtable.api.RowRange;
import com.spotify.heroic.metric.bigtable.api.Table;
import com.spotify.heroic.statistics.AnalyticsReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import lombok.ToString;

import javax.inject.Inject;
import javax.inject.Named;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.Semaphore;

@BigtableScope
@ToString(exclude = {"async", "mapper", "reporter"})
public class BigtableMetricAnalytics implements MetricAnalytics, LifeCycles {
    final Managed<BigtableConnection> connection;
    final AsyncFramework async;
    final ObjectMapper mapper;
    final AnalyticsReporter reporter;

    final String hitsTableName;
    final String hitsColumnFamily;
    final Semaphore pendingReports;

    final SeriesKeyEncoding fetchSeries = new SeriesKeyEncoding("fetch");

    @Inject
    public BigtableMetricAnalytics(
        final Managed<BigtableConnection> connection, final AsyncFramework async,
        @Named("application/json") final ObjectMapper mapper, final AnalyticsReporter reporter,
        @Named("hitsTableName") final String hitsTableName,
        @Named("hitsColumnFamily") final String hitsColumnFamily,
        @Named("maxPendingReports") final int maxPendingReports
    ) {
        this.connection = connection;
        this.async = async;
        this.mapper = mapper;
        this.reporter = reporter;

        this.hitsTableName = hitsTableName;
        this.hitsColumnFamily = hitsColumnFamily;
        this.pendingReports = new Semaphore(maxPendingReports);
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(c -> async.call(() -> {
            final Table table = c.tableAdminClient().getTable(hitsTableName).orElseGet(() -> {
                return c.tableAdminClient().createTable(hitsTableName);
            });

            table.getColumnFamily(hitsColumnFamily).orElseGet(() -> {
                return c.tableAdminClient().createColumnFamily(table, hitsColumnFamily);
            });

            return null;
        }));
    }

    @Override
    public MetricBackend wrap(final MetricBackend backend) {
        return new BigtableAnalyticsMetricBackend(this, backend);
    }

    @Override
    public AsyncObservable<SeriesHit> seriesHits(final LocalDate date) {
        final Borrowed<BigtableConnection> b = connection.borrow();

        if (!b.isValid()) {
            return AsyncObservable.failed(new RuntimeException("Failed to borrow connection"));
        }

        final BigtableConnection c = b.get();

        final ByteString start = fetchSeries.rangeKey(date);
        final ByteString end = fetchSeries.rangeKey(date.plusDays(1));

        final RowRange range = RowRange.rowRange(Optional.of(start), Optional.of(end));

        return c
            .dataClient()
            .readRowsObserved(hitsTableName, ReadRowsRequest.builder().range(range).build())
            .transform(row -> {
                final ByteString rowKey = row.getKey();
                final SeriesKeyEncoding.SeriesKey k;

                try {
                    k = fetchSeries.decode(rowKey, s -> mapper.readValue(s, Series.class));
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }

                final long value = row.getFamily(hitsColumnFamily).map(family -> {
                    final Family.LatestCellValueColumn col =
                        family.latestCellValue().iterator().next();
                    final ByteBuffer buf = ByteBuffer.wrap(col.getValue().toByteArray());
                    buf.order(ByteOrder.BIG_ENDIAN);
                    return buf.getLong();
                }).orElse(0L);

                return new SeriesHit(k.getSeries(), value);
            })
            .onFinished(b::release);
    }

    @Override
    public AsyncFuture<Void> reportFetchSeries(LocalDate date, Series series) {
        // limit the number of pending reports that are allowed at the same time to avoid resource
        // starvation.
        if (!pendingReports.tryAcquire()) {
            reporter.reportDroppedFetchSeries();
            return async.cancelled();
        }

        return connection.doto(c -> {
            final ByteString key = fetchSeries.encode(new SeriesKeyEncoding.SeriesKey(date, series),
                mapper::writeValueAsString);

            final AsyncFuture<Row> request = c
                .dataClient()
                .readModifyWriteRow(hitsTableName, key, ReadModifyWriteRules
                    .builder()
                    .increment(hitsColumnFamily, ByteString.EMPTY, 1L)
                    .build());

            return request.<Void>directTransform(d -> null).onFailed(e -> {
                reporter.reportFailedFetchSeries();
            });
        }).onFinished(pendingReports::release);
    }

    private AsyncFuture<Void> start() {
        return connection.start();
    }

    private AsyncFuture<Void> stop() {
        return connection.stop();
    }
}
