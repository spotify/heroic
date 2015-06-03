package com.spotify.heroic.grammar;

import lombok.Data;

import com.spotify.heroic.model.DateRange;

@Data
public class FromDSL {
    public static final FromDSL SERIES = new FromDSL(QuerySource.SERIES, null);
    public static final FromDSL EVENTS = new FromDSL(QuerySource.EVENTS, null);

    private final QuerySource source;
    private final DateRange range;
}
