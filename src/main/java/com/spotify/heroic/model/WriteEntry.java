package com.spotify.heroic.model;

import java.util.Collection;

import lombok.Data;

@Data
public final class WriteEntry {
    private final TimeSerie timeSerie;
    private final Collection<DataPoint> data;
}