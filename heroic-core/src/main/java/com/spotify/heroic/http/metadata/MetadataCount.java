package com.spotify.heroic.http.metadata;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.http.query.QueryDateRange;
import com.spotify.heroic.model.DateRange;

@Data
public class MetadataCount {
    private static final Filter DEFAULT_FILTER = TrueFilterImpl.get();
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);

    private final Filter filter;
    private final DateRange range;

    @JsonCreator
    public static MetadataCount create(@JsonProperty("filter") Filter filter,
            @JsonProperty("range") QueryDateRange range) {
        if (filter == null)
            filter = DEFAULT_FILTER;

        if (range == null)
            range = DEFAULT_DATE_RANGE;

        return new MetadataCount(filter, range.buildDateRange());
    }

    public static MetadataCount createDefault() {
        return create(null, null);
    }
}