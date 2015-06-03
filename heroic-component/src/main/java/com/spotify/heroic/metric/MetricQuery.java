package com.spotify.heroic.metric;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeData;

@Data
public class MetricQuery {
    private final String backendGroup;
    private final Filter filter;
    private final List<String> groupBy;
    private final DateRange range;
    private final Aggregation aggregation;
    private final boolean disableCache;
    private final Class<? extends TimeData> source;
}