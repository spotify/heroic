package com.spotify.heroic.model;

import java.util.Collection;

import lombok.Data;

@Data
public final class WriteMetric {
    private final Series series;
    private final Collection<DataPoint> data;
}