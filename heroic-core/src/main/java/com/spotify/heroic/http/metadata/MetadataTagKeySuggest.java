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
public class MetadataTagKeySuggest {
    private static final Filter DEFAULT_FILTER = TrueFilterImpl.get();
    private static final int DEFAULT_LIMIT = 10;
    private static final QueryDateRange DEFAULT_DATE_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);

    private final Filter filter;
    private final int limit;
    private final DateRange range;

    @JsonCreator
    public static MetadataTagKeySuggest create(@JsonProperty("filter") Filter filter,
            @JsonProperty("limit") Integer limit, @JsonProperty("range") QueryDateRange range) {
        if (filter == null)
            filter = DEFAULT_FILTER;

        if (limit == null)
            limit = DEFAULT_LIMIT;

        if (range == null)
            range = DEFAULT_DATE_RANGE;

        return new MetadataTagKeySuggest(filter, limit, range.buildDateRange());
    }

    public static MetadataTagKeySuggest createDefault() {
        return create(null, null, null);
    }
}