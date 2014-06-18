package com.spotify.heroic.metrics.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
@ToString(of = { "key", "range", "filter", "groupBy" })
public class FindRows {
    @Getter
    private final String key;
    @Getter
    private final DateRange range;
    @Getter
    private final Map<String, String> filter;
    @Getter
    private final List<String> groupBy;

    @RequiredArgsConstructor
    @ToString(of = { "rowGroups", "backend" })
    public static class Result {
        @Getter
        private final Map<TimeSerie, Set<TimeSerie>> rowGroups;

        @Getter
        private final MetricBackend backend;
    }

    public FindRows withRange(DateRange range) {
        return new FindRows(key, range, filter, groupBy);
    }
}