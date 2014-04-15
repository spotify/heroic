package com.spotify.heroic.backend.model;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.DateRange;

@ToString(of = { "key", "filter", "groupBy" })
public class FindRowGroups {
    @Getter
    private final String key;
    @Getter
    private final DateRange range;
    @Getter
    private final Map<String, String> filter;
    @Getter
    private final List<String> groupBy;

    public FindRowGroups(String key, DateRange range,
            Map<String, String> filter, List<String> groupBy) {
        this.key = key;
        this.range = range;
        this.filter = filter;
        this.groupBy = groupBy;
    }

    @ToString(of = { "rowGroups", "backend" })
    public static class Result {
        @Getter
        private final Map<Map<String, String>, List<DataPointsRowKey>> rowGroups;

        @Getter
        private final MetricBackend backend;

        public Result(
                Map<Map<String, String>, List<DataPointsRowKey>> rowGroups,
                MetricBackend backend) {
            this.rowGroups = rowGroups;
            this.backend = backend;
        }
    }
}