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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.protobuf.ByteString;
import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.analytics.SeriesHit;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import com.spotify.heroic.metric.bigtable.api.Column;
import com.spotify.heroic.metric.bigtable.api.ReadRowsRequest;
import com.spotify.heroic.metric.bigtable.api.Row;
import com.spotify.heroic.metric.bigtable.api.RowRange;
import com.spotify.heroic.metric.bigtable.api.Table;
import com.spotify.heroic.statistics.AnalyticsReporter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.Semaphore;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString(of = {"connection"})
public class BigtableMetricAnalytics implements MetricAnalytics {
    @Inject
    Managed<BigtableConnection> connection;

    @Inject
    AsyncFramework async;

    @Inject
    @Named("application/json")
    ObjectMapper mapper;

    @Inject
    AnalyticsReporter reporter;

    final String hitsTableName;
    final String hitsColumnFamily;
    final Semaphore pendingReports;

    final SeriesKeyEncoding fetchSeries = new SeriesKeyEncoding("fetch");

    public BigtableMetricAnalytics(final String hitsTableName, final String hitsColumnFamily,
            final int maxPendingReports) {
        this.hitsTableName = hitsTableName;
        this.hitsColumnFamily = hitsColumnFamily;
        this.pendingReports = new Semaphore(maxPendingReports);
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public AsyncFuture<Void> configure() {
        return connection.doto(c -> async.call(() -> {
            final Table table = c.adminClient().getTable(hitsTableName).orElseGet(() -> {
                return c.adminClient().createTable(hitsTableName);
            });

            table.getColumnFamily(hitsColumnFamily).orElseGet(() -> {
                return c.adminClient().createColumnFamily(table, hitsColumnFamily);
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

        return c.client()
                .readRowsObserved(hitsTableName, ReadRowsRequest.builder().range(range).build())
                .transform(row -> {
                    final ByteString rowKey = row.getKey();
                    final SeriesKeyEncoding.SeriesKey k =
                            fetchSeries.decode(rowKey, s -> mapper.readValue(s, Series.class));

                    final long value = row.getFamily(hitsColumnFamily).map(family -> {
                        final Column col = family.getColumns().iterator().next();
                        final ByteBuffer buf = ByteBuffer.wrap(col.getValue().toByteArray());
                        buf.order(ByteOrder.BIG_ENDIAN);
                        return buf.getLong();
                    }).orElse(0L);

                    return new SeriesHit(k.getSeries(), value);
                }).onFinished(b::release);
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

            final AsyncFuture<Row> request = c.client().readModifyWriteRow(hitsTableName, key,
                    c.client().readModifyWriteRules()
                            .increment(hitsColumnFamily, ByteString.EMPTY, 1L).build());

            return request.<Void> directTransform(d -> null).onFailed(e -> {
                reporter.reportFailedFetchSeries();
            });
        }).onFinished(pendingReports::release);
    }
}
