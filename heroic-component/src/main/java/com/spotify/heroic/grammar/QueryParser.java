package com.spotify.heroic.grammar;

import com.spotify.heroic.filter.Filter;

public interface QueryParser {
    /**
     * Parse the given filter using the Heroic Query DSL.
     *
     * @return A filter implementation.
     */
    Filter parseFilter(String filter);

    QueryDSL parseQuery(String query);
}