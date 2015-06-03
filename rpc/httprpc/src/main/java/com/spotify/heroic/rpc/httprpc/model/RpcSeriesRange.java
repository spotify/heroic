package com.spotify.heroic.rpc.httprpc.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Data
public final class RpcSeriesRange {
    private final String group;
    private final Series series;
    private final DateRange range;

    @JsonCreator
    public static RpcSeriesRange create(@JsonProperty("series") Series series, @JsonProperty("range") DateRange range,
            @JsonProperty("group") String group) {
        return new RpcSeriesRange(group, series, range);
    }
}