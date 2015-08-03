package com.spotify.heroic.metric.bigtable.api;

import java.util.List;

import lombok.Data;

@Data
public class BigtableTable {
    final String name;
    final List<BigtableColumnFamily> columnFamilies;

    public String getName() {
        return name;
    }

    public List<BigtableColumnFamily> getColumnFamilies() {
        return columnFamilies;
    }
}