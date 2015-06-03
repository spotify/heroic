package com.spotify.heroic.cluster.model;

import static com.google.common.base.Preconditions.checkNotNull;
import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;

@Data
public class GroupFilter {
    private final String group;
    private final RangeFilter filter;

    @JsonCreator
    public GroupFilter(@JsonProperty("group") String group, @JsonProperty("filter") RangeFilter filter) {
        this.group = checkNotNull(group);
        this.filter = checkNotNull(filter);
    }

    public static GroupFilter filterFor(String group, Filter filter, DateRange range) {
        return new GroupFilter(group, new RangeFilter(filter, range, Integer.MAX_VALUE));
    }

    public static GroupFilter filterFor(String group, Filter filter, DateRange range, int limit) {
        return new GroupFilter(group, new RangeFilter(filter, range, limit));
    }
}