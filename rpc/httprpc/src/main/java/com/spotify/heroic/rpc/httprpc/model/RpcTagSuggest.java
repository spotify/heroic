package com.spotify.heroic.rpc.httprpc.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.model.DateRange;

@Data
public final class RpcTagSuggest {
    private final String group;
    private final Filter filter;
    private final String key;
    private final Integer limit;
    private final DateRange range;

    @JsonCreator
    public static RpcTagSuggest create(@JsonProperty("filter") Filter filter, @JsonProperty("range") DateRange range,
            @JsonProperty("key") String key, @JsonProperty("group") String group, @JsonProperty("limit") Integer limit) {
        return new RpcTagSuggest(group, filter, key, limit, range);
    }
}