package com.spotify.heroic.http.metadata;

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.impl.TrueFilterImpl;
import com.spotify.heroic.http.query.QueryDateRange;
import com.spotify.heroic.model.DateRange;

@Data
public class MetadataTagValueSuggest {
    private static final Filter DEFAULT_FILTER = TrueFilterImpl.get();
    private static final int DEFAULT_LIMIT = 10;
    private static final QueryDateRange DEFAULT_RANGE = new QueryDateRange.Relative(TimeUnit.DAYS, 7);

    /**
     * Filter the suggestions being returned.
     */
    private final Filter filter;

    /**
     * Limit the number of suggestions being returned.
     */
    private final int limit;

    /**
     * Query for tags within the given range.
     */
    private final DateRange range;

    /**
     * Exclude the given tags from the result.
     */
    private final String key;

    @JsonCreator
    public MetadataTagValueSuggest(@JsonProperty("filter") Filter filter, @JsonProperty("limit") Integer limit,
            @JsonProperty("range") QueryDateRange range, @JsonProperty("key") String key) {
        this.filter = Optional.fromNullable(filter).or(DEFAULT_FILTER);
        this.limit = Optional.fromNullable(limit).or(DEFAULT_LIMIT);
        this.range = Optional.fromNullable(range).or(DEFAULT_RANGE).buildDateRange();
        this.key = Preconditions.checkNotNull(key, "key must not be null");
    }

    public static MetadataTagValueSuggest createDefault() {
        return new MetadataTagValueSuggest(null, null, null, null);
    }
}