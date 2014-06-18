package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of={"groups", "statistics"})
@RequiredArgsConstructor
public final class MetricGroups {
    @Getter
    private final List<MetricGroup> groups;
    @Getter
    private final Statistics statistics;
}