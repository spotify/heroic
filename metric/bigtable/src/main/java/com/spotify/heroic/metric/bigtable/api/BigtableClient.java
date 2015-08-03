package com.spotify.heroic.metric.bigtable.api;

import java.util.List;

import com.google.protobuf.ByteString;

import eu.toolchain.async.AsyncFuture;

public interface BigtableClient {
    AsyncFuture<Void> mutateRow(String tableName, ByteString rowKey, BigtableMutations mutations);

    BigtableMutationsBuilder mutations();

    AsyncFuture<List<BigtableLatestRow>> readRows(String tableName, ByteString rowKey, BigtableRowFilter filter);

    BigtableRowFilter columnFilter(String family, ByteString start, ByteString end);
}