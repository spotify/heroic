package com.spotify.heroic.metric.bigtable.api;

import lombok.Data;

@Data
public class BigtableLatestColumnFamily {
    final String name;

    final Iterable<BigtableCell> columns;
}