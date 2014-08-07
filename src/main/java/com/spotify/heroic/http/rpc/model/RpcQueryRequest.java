package com.spotify.heroic.http.rpc.model;

import java.util.Set;

import lombok.Data;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@Data
public class RpcQueryRequest {
    private TimeSerie key;
    private Set<TimeSerie> timeseries;
    private DateRange range;
    private AggregationGroup aggregationGroup;
}
