package com.spotify.heroic.metric;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Optional;

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
    private final Optional<RequestError> error;

    @JsonCreator
    public ShardTrace(@JsonProperty("name") final String name, @JsonProperty("metadata") final NodeMetadata metadata,
            @JsonProperty("latency") final Long latency, @JsonProperty("statistics") final Statistics statistics,
            @JsonProperty("error") final Optional<RequestError> error, @JsonProperty("children") final List<ShardTrace> children) {
        this.name = checkNotNull(name, "name");
        this.latency = checkNotNull(latency, "latency");
        this.metadata = checkNotNull(metadata, "metadata");
        this.statistics = checkNotNull(statistics, "statistics");
        this.error = checkNotNull(error, "error");
        this.children = checkNotNull(children, "children");
    }

    public static ShardTrace of(final String name, final NodeMetadata metadata, final long latency, final Statistics statistics, final Optional<RequestError> error) {
        return new ShardTrace(name, metadata, latency, statistics, error, ImmutableList.of());
    }
}