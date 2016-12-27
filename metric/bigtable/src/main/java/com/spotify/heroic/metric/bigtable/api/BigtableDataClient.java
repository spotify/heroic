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

import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.google.protobuf.ByteString;

import com.spotify.heroic.async.AsyncObservable;

import eu.toolchain.async.AsyncFuture;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public interface BigtableDataClient {

    interface CellConsumer {
        <T> void consume(
            List<? extends T> data, Function<T, ByteString> qualifier, Function<T, ByteString> value
        );
    }

    AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey, Mutations mutations);

    /**
     * Read the given set of rows, only resolving when all rows are available.
     *
     * @param tableName Table to read rows from.
     * @param request Request to use when reading rows.
     * @return A future that will be resolved when all rows are available.
     */
    AsyncFuture<List<FlatRow>> readRows(String tableName, ReadRowsRequest request);

    /**
     * Read the given set of rows, calling cellConsumer with cells as they are available.
     *
     * @param tableName Table to read rows from.
     * @param request Request to use when reading rows.
     * @param fetchSize The number of cells to fetch for every batch.
     * @param cellConsumer The consumer of fetched data.
     * @return A future that will be resolved when all cells have been passed to the cellConsumer.
     */
    AsyncFuture<Void> readRowRange(
        String tableName, ReadRowRangeRequest request, Optional<Integer> fetchSize,
        CellConsumer cellConsumer
    );

    /**
     * Read the given set of rows in an observable way.
     *
     * @param tableName The table to read rows from.
     * @param request Request to use when reading rows.
     * @return An observable that can be observed to read one row at a time.
     */
    AsyncObservable<Row> readRowsObserved(String tableName, ReadRowsRequest request);

    AsyncFuture<Row> readModifyWriteRow(
        String tableName, ByteString rowKey, ReadModifyWriteRules rules
    );
}
