package com.spotify.heroic.grammar;

import lombok.Data;

@Data
public class SelectDSL {
    private final Context context;
    private final AggregationValue aggregation;
}
