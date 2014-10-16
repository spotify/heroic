package com.spotify.heroic.metric.model;

import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.model.DataPoint;

@Data
@EqualsAndHashCode(exclude = { "datapoints" })
public final class ShardedMetricGroup {
    private final Map<String, String> shard;
    private final Map<String, String> group;
    private final List<DataPoint> datapoints;

    @JsonCreator
    public static MetricGroup create(@JsonProperty("shard") Map<String, String> shard,
            @JsonProperty("group") Map<String, String> group, @JsonProperty("datapoints") List<DataPoint> datapoints) {
        return new MetricGroup(group, datapoints);
    }
}
