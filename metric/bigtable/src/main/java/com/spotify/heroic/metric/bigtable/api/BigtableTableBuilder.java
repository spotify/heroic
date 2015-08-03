package com.spotify.heroic.metric.bigtable.api;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.google.common.collect.ImmutableList;

@RequiredArgsConstructor
@ToString
public class BigtableTableBuilder {
    final String name;
    final List<BigtableColumnFamily> columnFamilies = new ArrayList<>();

    public BigtableTableBuilder columnFamily(BigtableColumnFamily columnFamily) {
        columnFamilies.add(columnFamily);
        return this;
    }

    public BigtableTable build() {
        return new BigtableTable(name, ImmutableList.copyOf(columnFamilies));
    }
}