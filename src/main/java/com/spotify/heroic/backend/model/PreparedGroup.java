package com.spotify.heroic.backend.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.kairosdb.DataPointsRowKey;

@RequiredArgsConstructor
public final class PreparedGroup {
    @Getter
    private final MetricBackend backend;
    @Getter
    private final List<DataPointsRowKey> rows;
}