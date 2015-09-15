package com.spotify.heroic.metric;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.common.Statistics;

import lombok.Data;

@Data
public class ShardTrace {
    private final String name;
    private final NodeMetadata metadata;
    private final long latency;
    private final Statistics statistics;
    private final List<ShardTrace> children;

    @JsonCreator
    public ShardTrace(@JsonProperty("name") final String name, @JsonProperty("metadata") final NodeMetadata metadata,
            @JsonProperty("latency") final Long latency, @JsonProperty("statistics") final Statistics statistics,
            @JsonProperty("children") final List<ShardTrace> children) {
        this.name = checkNotNull(name, "name");
        this.latency = checkNotNull(latency, "latency");
        this.metadata = checkNotNull(metadata, "metadata");
        this.statistics = checkNotNull(statistics, "statistics");
        this.children = checkNotNull(children, "children");
    }

    public static ShardTrace of(final String name, final NodeMetadata metadata, final long latency, final Statistics statistics) {
        return new ShardTrace(name, metadata, latency, statistics, ImmutableList.of());
    }
}