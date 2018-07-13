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

import static com.spotify.heroic.metric.bigtable.api.RowFilter.compareByteStrings;

import com.google.bigtable.v2.Mutation;
import com.google.cloud.bigtable.grpc.scanner.FlatRow;
import com.spotify.heroic.bigtable.com.google.protobuf.ByteString;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.metric.bigtable.BigtableConnection;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

public class FakeBigtableConnection implements BigtableConnection {
    private static final String CLUSTER_URI = "fake";

    private final AsyncFramework async;

    private final ConcurrentMap<String, TableStorage> tables = new ConcurrentHashMap<>();

    @Inject
    public FakeBigtableConnection(final AsyncFramework async) {
        this.async = async;
    }

    @Override
    public BigtableTableAdminClient tableAdminClient() {
        return new AdminClient();
    }

    @Override
    public BigtableDataClient dataClient() {
        return new DataClient();
    }

    @Override
    public AsyncFuture<Void> close() {
        return async.resolved();
    }

    class AdminClient implements BigtableTableAdminClient {
        private final Object lock = new Object();

        @Override
        public Optional<Table> getTable(
            final String tableId
        ) {
            final TableStorage storage = tables.get(tableId);

            if (storage == null) {
                return Optional.empty();
            }

            return Optional.of(storage.getTable());
        }

        @Override
        public Table createTable(final String name) {
            synchronized (lock) {
                TableStorage storage = tables.get(name);

                if (storage == null) {
                    final Table table = new Table(CLUSTER_URI, name);
                    storage = new TableStorage(table);
                    tables.put(name, storage);
                }

                return storage.getTable();
            }
        }

        @Override
        public ColumnFamily createColumnFamily(
            final Table table, final String name
        ) {
            synchronized (lock) {
                final TableStorage storage = tables.get(table.getName());

                if (storage == null) {
                    throw new IllegalArgumentException("no such table: " + name);
                }

                return table.getColumnFamily(name).orElseGet(() -> {
                    final ColumnFamily columnFamily =
                        new ColumnFamily(table.getClusterUri(), table.getName(), name);
                    final Table newTable = storage.getTable().withAddColumnFamily(columnFamily);
                    storage.setTable(newTable);
                    return columnFamily;
                });
            }
        }
    }

    class DataClient implements BigtableDataClient {
        @Override
        public AsyncFuture<Void> mutateRow(
            final String tableName, final ByteString rowKey, final Mutations mutations
        ) {
            final TableStorage storage = tables.get(tableName);

            if (storage == null) {
                return async.failed(new IllegalStateException("No such table: " + tableName));
            }

            return storage.mutateRow(rowKey, mutations);
        }

        @Override
        public AsyncFuture<List<FlatRow>> readRows(
            final String tableName, final ReadRowsRequest request
        ) {
            final TableStorage storage = tables.get(tableName);

            if (storage == null) {
                return async.failed(new IllegalStateException("No such table: " + tableName));
            }

            return storage.readRows(request);
        }

        @Override
        public AsyncObservable<Row> readRowsObserved(
            final String tableName, final ReadRowsRequest request
        ) {
            return AsyncObservable.failed(new RuntimeException("not supported"));
        }

        @Override
        public AsyncFuture<Row> readModifyWriteRow(
            final String tableName, final ByteString rowKey, final ReadModifyWriteRules rules
        ) {
            return async.failed(new RuntimeException("not supported"));
        }
    }

    @Data
    @AllArgsConstructor
    class TableStorage {
        private Table table;

        private final ConcurrentMap<Pair<ByteString, ColumnFamily>, RowStorage> rows =
            new ConcurrentHashMap<>();

        private final Object lock = new Object();

        public AsyncFuture<Void> mutateRow(final ByteString rowKey, final Mutations mutations) {
            return async.call(() -> {
                mutations.getMutations().forEach(mutation -> {
                    switch (mutation.getMutationCase()) {
                        case SET_CELL:
                            final Mutation.SetCell setCell = mutation.getSetCell();
                            final ColumnFamily columnFamily = table
                                .getColumnFamily(setCell.getFamilyName())
                                .orElseThrow(() -> new IllegalArgumentException(
                                    "no such column family: " + setCell.getFamilyName()));

                            final RowStorage rowStorage;

                            synchronized (lock) {
                                final Pair<ByteString, ColumnFamily> key =
                                    Pair.of(rowKey, columnFamily);

                                rowStorage =
                                    rows.computeIfAbsent(key, k -> new RowStorage(columnFamily));
                            }

                            rowStorage.runSetCell(setCell);
                            break;
                        default:
                            throw new IllegalArgumentException(
                                "Unsupported mutation: " + mutation.getMutationCase());
                    }
                });

                return null;
            });
        }

        public AsyncFuture<List<FlatRow>> readRows(final ReadRowsRequest request) {
            final Function<String, Boolean> matchesColumnFamily =
                request.getFilter().<Function<String, Boolean>>map(
                    filter -> filter::matchesColumnFamily).orElse(familyName -> true);

            final Function<ByteString, Boolean> matchesColumn =
                request.getFilter().<Function<ByteString, Boolean>>map(
                    filter -> filter::matchesColumn).orElse(column -> true);

            final Function<ByteString, Boolean> matchesRowKey = bytes -> {
                final boolean rangeMatches = request.getRange().map(range -> {
                    if (range.getStart().isPresent()) {
                        final int n = compareByteStrings(range.getStart().get(), bytes);

                        if (!(n <= 0)) {
                            return false;
                        }
                    }

                    if (range.getEnd().isPresent()) {
                        final int n = compareByteStrings(bytes, range.getEnd().get());

                        if (!(n < 0)) {
                            return false;
                        }
                    }

                    return true;
                }).orElse(true);

                final boolean keyMatches = request.getRowKey().map(bytes::equals).orElse(true);

                return rangeMatches && keyMatches;
            };

            return async.call(() -> rows.entrySet().stream().flatMap(entry -> {
                final Pair<ByteString, ColumnFamily> key = entry.getKey();

                if (!matchesRowKey.apply(key.getLeft())) {
                    return Stream.empty();
                }

                if (!matchesColumnFamily.apply(key.getRight().getName())) {
                    return Stream.empty();
                }

                return Stream.of(entry
                    .getValue()
                    .readRows(key.getLeft(), key.getRight(), request, matchesColumn));
            }).collect(Collectors.toList()));
        }
    }

    @Data
    class RowStorage {
        private final ColumnFamily columnFamily;
        private final ConcurrentMap<ByteString, ByteString> storage =
            new ConcurrentSkipListMap<>(RowFilter::compareByteStrings);

        void runSetCell(final Mutation.SetCell setCell) {
            // TODO: take timestamp into account
            storage.put(setCell.getColumnQualifier(), setCell.getValue());
        }

        FlatRow readRows(
            final ByteString rowKey, final ColumnFamily columnFamily, final ReadRowsRequest request,
            final Function<ByteString, Boolean> matchesColumn
        ) {
            final FlatRow.Builder builder = FlatRow.newBuilder().withRowKey(rowKey);

            storage
                .entrySet()
                .stream()
                .filter(e -> matchesColumn.apply(e.getKey()))
                .map(column -> FlatRow.Cell
                    .newBuilder()
                    .withFamily(columnFamily.getName())
                    .withQualifier(column.getKey())
                    .withValue(column.getValue())
                    .build())
                .forEach(builder::addCell);

            return builder.build();
        }
    }
}
