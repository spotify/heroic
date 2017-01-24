/*
 * Copyright (c) 2016 Spotify AB.
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

import com.google.bigtable.v2.BigtableGrpc.Bigtable;
import com.google.bigtable.v2.ColumnRange;
import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk.RowStatusCase;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.RowSet;
import com.google.protobuf.ByteString;

import com.spotify.heroic.bigtable.grpc.stub.StreamObserver;
import com.spotify.heroic.metric.bigtable.api.BigtableDataClient.CellConsumer;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RowRangeReader {

    private static final int DEFAULT_FETCH_SIZE = 500000;

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class CellChunkHandler {
        // Downstream consumer of committed cells
        private final Consumer<List<CellChunk>> commitCells;
        // Number of input cells consumed
        private final int consumedCells;
        // Cell chunk data from incomplete cell kept from last invocation of consumeChunks
        private final List<ByteString> partialCellCarryOver;
        // Cells last invocation of consumeChunks that have not been committed
        private final List<CellChunk> nonCommittedCellsCarryOver;

        CellChunkHandler(Consumer<List<CellChunk>> commitCells) {
            this(commitCells, 0, Collections.emptyList(), Collections.emptyList());
        }

        int getConsumedCells() {
            return consumedCells;
        }

        CellChunkHandler consumeChunks(List<CellChunk> cellChunks) {
            List<ByteString> incompleteCellData = Collections.emptyList();
            if (!partialCellCarryOver.isEmpty() ||
                cellChunks.stream().anyMatch(cc -> cc.getValueSize() > 0)) {

                log.debug("Creating cells from {} chunks (and {} carry over chunks)",
                    cellChunks.size(), partialCellCarryOver.size());

                List<CellChunk> cells = new ArrayList<>(cellChunks.size());
                List<ByteString> partialCell = new ArrayList<>(2);
                partialCell.addAll(partialCellCarryOver);

                cellChunks.forEach(cc -> {
                    if (cc.getValueSize() > 0) { // partial cell
                        partialCell.add(cc.getValue());
                    } else if (partialCell.isEmpty()) { // no cell in progress
                        cells.add(cc);
                    } else { // complete chunked cell
                        partialCell.add(cc.getValue());
                        cells.add(cc
                            .toBuilder()
                            .setValue(ByteString.copyFrom(partialCell))
                            .setValueSize(0)
                            .build());
                        partialCell.clear();
                    }
                });

                incompleteCellData = partialCell;
                cellChunks = cells;
            }
            // All cellChunks are complete cells
            if (!nonCommittedCellsCarryOver.isEmpty()) {
                List<CellChunk> cells = new ArrayList<>(nonCommittedCellsCarryOver);
                cells.addAll(cellChunks);
                cellChunks = cells;
            }
            List<CellChunk> cellsCarryOver = consumeCells(cellChunks);
            return new CellChunkHandler(commitCells, consumedCells + cellChunks.size(),
                incompleteCellData, cellsCarryOver);
        }

        private List<CellChunk> consumeCells(List<CellChunk> cells) {
            int nextListStart = 0;
            for (int i = 0; i < cells.size(); i++) {
                CellChunk cellChunk = cells.get(i);
                RowStatusCase rowStatusCase = cellChunk.getRowStatusCase();

                if (rowStatusCase == RowStatusCase.RESET_ROW && cellChunk.getResetRow()) {
                    nextListStart = i + 1;
                } else if (rowStatusCase == RowStatusCase.COMMIT_ROW && cellChunk.getCommitRow()) {
                    int thisListStart = nextListStart;
                    nextListStart = i + 1;
                    if (thisListStart == 0 && nextListStart == cells.size()) {
                        commitCells.accept(cells);
                    } else {
                        commitCells.accept(cells.subList(thisListStart, nextListStart));
                    }
                }
            }
            return cells.subList(nextListStart, cells.size());
        }
    }

    private class StreamObserverImpl implements StreamObserver<ReadRowsResponse> {

        private final String tableUri;
        private final ReadRowRangeRequest rowRequest;
        private final int fetchSize;
        private final ResolvableFuture<?> future;
        private final CellConsumer cellConsumer;
        private CellChunkHandler cellChunkHandler;
        private ByteString lastQualifierRef;

        private StreamObserverImpl(
            String tableUri, ReadRowRangeRequest rowRequest, int fetchSize,
            ResolvableFuture<?> future, CellConsumer cellConsumer
        ) {
            this.tableUri = tableUri;
            this.rowRequest = rowRequest;
            this.fetchSize = fetchSize;
            this.future = future;
            this.cellConsumer = cellConsumer;
            this.cellChunkHandler = new CellChunkHandler(this::commit);
        }

        private void commit(List<CellChunk> cellChunks) {
            cellConsumer.consume(cellChunks, c -> c.getQualifier().getValue(), CellChunk::getValue);
        }

        @Override
        public void onNext(ReadRowsResponse response) {
            List<CellChunk> cellChunks = response.getChunksList();
            if (cellChunks.isEmpty()) {
                log.warn("Got empty cell chunk list");
                return;
            }
            log.trace("Got {} cell chunks", response.getChunksList().size());
            cellChunkHandler = cellChunkHandler.consumeChunks(cellChunks);
            lastQualifierRef = cellChunks.get(cellChunks.size() - 1).getQualifier().getValue();
        }

        @Override
        public void onError(Throwable throwable) {
            future.fail(throwable);
        }

        @Override
        public void onCompleted() {
            if (cellChunkHandler.getConsumedCells() < fetchSize || lastQualifierRef == null ||
                rowRequest.getEndQualifierClosed().equals(lastQualifierRef)) {
                log.debug("Read rows completed");
                future.resolve(null);
            } else {
                log.debug("Send new read rows request");
                StreamObserverImpl observer =
                    new StreamObserverImpl(tableUri, rowRequest, fetchSize, future, cellConsumer);
                client.readRows(buildRequest(tableUri, rowRequest, fetchSize, lastQualifierRef),
                    observer);
            }
        }
    }

    private final Bigtable client;
    private final AsyncFramework async;

    RowRangeReader(Bigtable client, AsyncFramework async) {
        this.client = client;
        this.async = async;
    }

    public AsyncFuture<Void> readRow(
        String tableUri, ReadRowRangeRequest request, Optional<Integer> optionalFetchSize,
        CellConsumer consumer
    ) {
        int fetchSize = optionalFetchSize.orElse(DEFAULT_FETCH_SIZE);
        ResolvableFuture<Void> future = async.future();
        StreamObserverImpl observer =
            new StreamObserverImpl(tableUri, request, fetchSize, future, consumer);
        ReadRowsRequest req =
            buildRequest(tableUri, request, fetchSize, request.getStartQualifierOpen());
        client.readRows(req, observer);
        return future;
    }

    private ReadRowsRequest buildRequest(
        String tableUri, ReadRowRangeRequest rowRequest, int fetchSize,
        ByteString startQualifierOpen
    ) {
        RowSet.Builder rowSetBuilder = RowSet.newBuilder();
        rowSetBuilder.addRowKeys(rowRequest.getRowKey());

        ReadRowsRequest.Builder requestBuilder = ReadRowsRequest.newBuilder();
        requestBuilder.setTableName(tableUri);
        requestBuilder.setRows(rowSetBuilder.build());

        RowFilter.Chain.Builder chain = RowFilter.Chain.newBuilder();

        ColumnRange.Builder columnRange = ColumnRange
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
