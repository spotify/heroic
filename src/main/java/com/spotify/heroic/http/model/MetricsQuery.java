package com.spotify.heroic.http.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.filter.Filter;

@Data
public class MetricsQuery {
    private final Filter filter;
    private final List<String> groupBy;
    private final DateRange range;
    private final AggregationGroup aggregation;
}
