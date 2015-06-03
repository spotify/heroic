package com.spotify.heroic.rpc.httprpc.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;

@Data
public final class RpcTagValuesSuggest {
    private final String group;
    private final Filter filter;
    private final Integer limit;
    private final DateRange range;

    @JsonCreator
    public static RpcTagValuesSuggest create(@JsonProperty("filter") Filter filter, @JsonProperty("range") DateRange range,
            @JsonProperty("group") String group, @JsonProperty("limit") Integer limit) {
        return new RpcTagValuesSuggest(group, filter, limit, range);
    }
}