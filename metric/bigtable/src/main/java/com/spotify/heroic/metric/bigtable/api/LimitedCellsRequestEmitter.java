/*
 * Copyright (c) 2017 Spotify AB.
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

import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import com.spotify.heroic.metric.bigtable.api.BigtableDataClient.CellConsumer;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
class LimitedCellsRequestEmitter {

    private static final int DEFAULT_FETCH_SIZE = 500000;

    private final BiFunction<ReadRowsRequest, CellConsumer, ListenableFuture<?>> requestIssuer;
    private final AsyncFramework async;

    @RequiredArgsConstructor
    private class RequestHandler implements CellConsumer, FutureCallback<Object> {
        private final String tableUri;
        private final ReadRowRangeRequest rowRequest;
        private final int fetchSize;
        private final ResolvableFuture<?> future;
        private final CellConsumer cellConsumer;
        // Number of input cells consumed
        private int consumedCells;
        private ByteString lastQualifierRef;

        @Override
        public <T> void consume(
            final List<? extends T> cells, final Function<T, ByteString> qualifier,
            final Function<T, ByteString> value
        ) {
            if (cells.isEmpty()) {
                log.warn("Got empty cell chunk list");
                return;
            }
            cellConsumer.consume(cells, qualifier, value);
            consumedCells += cells.size();
            lastQualifierRef = qualifier.apply(cells.get(cells.size() - 1));
        }

        @Override
        public void onSuccess(final Object object) {
            if (consumedCells < fetchSize || lastQualifierRef == null ||
                rowRequest.getEndQualifierClosed().equals(lastQualifierRef)) {
                log.debug("Read rows completed");
                future.resolve(null);
            } else {
                log.debug("Send new read rows request");
                final RequestHandler requestHandler =
                    new RequestHandler(tableUri, rowRequest, fetchSize, future, cellConsumer);
                issueRequest(requestHandler, lastQualifierRef);
            }
        }

        @Override
        public void onFailure(final Throwable throwable) {
            future.fail(throwable);
        }

        private ReadRowsRequest buildRequest(final ByteString startQualifierOpen) {
            final RowSet.Builder rowSetBuilder = RowSet.newBuilder();
            rowSetBuilder.addRowKeys(rowRequest.getRowKey());

            final ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder();
            requestBuilder.setTableName(tableUri);
            requestBuilder.setRows(rowSetBuilder.build());

            final RowFilter.Chain.Builder chain = RowFilter.Chain.newBuilder();

            final ColumnRange.Builder columnRange = ColumnRange
                .newBuilder()
                .setFamilyName(rowRequest.getColumnFamily())
                .setStartQualifierOpen(startQualifierOpen)
                .setEndQualifierClosed(rowRequest.getEndQualifierClosed());

            chain.addFilters(RowFilter.newBuilder().setColumnRangeFilter(columnRange.build()));
            chain.addFilters(RowFilter.newBuilder().setCellsPerColumnLimitFilter(1));
            chain.addFilters(RowFilter.newBuilder().setCellsPerRowLimitFilter(fetchSize));

            requestBuilder.setFilter(RowFilter.newBuilder().setChain(chain.build()).build());
            return requestBuilder.build();
        }
    }

    public AsyncFuture<Void> readRow(
        final String tableUri, final ReadRowRangeRequest request, final Optional<Integer> optionalFetchSize,
        final CellConsumer consumer
    ) {
        final ResolvableFuture<Void> future = async.future();
        final int fetchSize = optionalFetchSize.orElse(DEFAULT_FETCH_SIZE);
        final RequestHandler requestHandler =
            new RequestHandler(tableUri, request, fetchSize, future, consumer);
        issueRequest(requestHandler, request.getStartQualifierOpen());
        return future;
    }

    private void issueRequest(final RequestHandler requestHandler, final ByteString startQualifierOpen) {
        final ReadRowsRequest request = requestHandler.buildRequest(startQualifierOpen);
        final ListenableFuture<?> future = requestIssuer.apply(request, requestHandler);
        Futures.addCallback(future, requestHandler);
    }
}
