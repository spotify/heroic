package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.filter.Filter;

@Data
public class FindTimeSeriesCriteria {
    private final Filter filter;
    private final List<String> groupBy;
    private final DateRange range;

    public FindTimeSeriesCriteria withRange(DateRange range) {
        return new FindTimeSeriesCriteria(filter, groupBy, range);
    }
}