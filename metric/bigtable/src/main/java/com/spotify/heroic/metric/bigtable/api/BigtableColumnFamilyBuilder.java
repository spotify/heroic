package com.spotify.heroic.metric.bigtable.api;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BigtableColumnFamilyBuilder {
    final String name;

    public BigtableColumnFamily build() {
        return new BigtableColumnFamily(name);
    }
}