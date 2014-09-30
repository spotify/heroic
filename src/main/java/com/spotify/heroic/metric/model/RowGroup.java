package com.spotify.heroic.metric.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
@ToString(of = { "backend", "series" })
public final class RowGroup {
    @Getter
    private final MetricBackend backend;
    @Getter
    private final Series series;
}