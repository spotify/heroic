package com.spotify.heroic.metric.bigtable;

import lombok.Data;
import lombok.experimental.Builder;

import com.spotify.heroic.common.Series;

import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@Data
public class RowKey {
    private final Series series;

    private final long base;
}