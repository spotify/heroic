package com.spotify.heroic.grammar;

import lombok.Data;

import com.spotify.heroic.filter.Filter;

@Data
public final class QueryDSL {
    private final SelectDSL select;
    private final FromDSL source;
    private final Filter where;
    private final GroupByDSL groupBy;
}