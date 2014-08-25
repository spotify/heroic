package com.spotify.heroic.metrics.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
@ToString(of = { "backend", "series" })
public final class RowGroup {
    @Getter
    private final Backend backend;
    @Getter
    private final Series series;
}