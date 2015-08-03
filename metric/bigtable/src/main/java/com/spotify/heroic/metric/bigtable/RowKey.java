package com.spotify.heroic.metric.bigtable;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class RowKey {
    private final Series series;
    private final long base;
}