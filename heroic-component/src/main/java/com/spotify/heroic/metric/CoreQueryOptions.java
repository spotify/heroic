package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class CoreQueryOptions implements QueryOptions {
    private final boolean tracing;

    @JsonCreator
    public CoreQueryOptions(@JsonProperty("tracing") boolean tracing) {
        this.tracing = tracing;
    }

    static final QueryOptions DEFAULTS = new CoreQueryOptions(false);
}