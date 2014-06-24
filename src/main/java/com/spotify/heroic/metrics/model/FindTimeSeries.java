package com.spotify.heroic.metrics.model;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
@ToString(of = { "key", "range", "filter", "groupBy" })
public class FindTimeSeries {
    @Getter
    private final String key;
    @Getter
    private final Map<String, String> filter;
    @Getter
    private final List<String> groupBy;
    @Getter
    private final DateRange range;

    @Data
    public static class Result {
        @Getter
        private final Map<TimeSerie, Set<TimeSerie>> groups;
    }

    public FindTimeSeries withRange(DateRange range) {
        return new FindTimeSeries(key, filter, groupBy, range);
    }
}