package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.common.Series;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class RowKey {
    private final Series series;

    private final long base;
}