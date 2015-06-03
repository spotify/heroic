package com.spotify.heroic.rpc.httprpc.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class RpcGroupedQuery<T> {
    private final String group;
    private final T query;

    @JsonCreator
    public RpcGroupedQuery(@JsonProperty("group") String group, @JsonProperty("query") T query) {
        this.group = group;
        this.query = query;
    }
}