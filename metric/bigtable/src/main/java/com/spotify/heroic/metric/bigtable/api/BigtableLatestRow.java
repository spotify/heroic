package com.spotify.heroic.metric.bigtable.api;

import java.util.List;

import lombok.Data;

@Data
public class BigtableLatestRow {
    final List<BigtableLatestColumnFamily> families;
}
