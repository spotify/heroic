package com.spotify.heroic.metrics.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
@ToString(of = { "backend", "timeSerie" })
public final class RowGroup {
    @Getter
    private final MetricBackend backend;
    @Getter
    private final TimeSerie timeSerie;
}