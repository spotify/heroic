package com.spotify.heroic.metric;

import lombok.Data;

@Data
public class QueryOptionsImpl implements QueryOptions {
    private final boolean tracing;

    static final QueryOptions DEFAULTS = new QueryOptionsImpl(false);
}