package com.spotify.heroic.metric;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class ShardTrace {
    private final String name;
    private final long latency;
    private final List<ShardTrace> children;

    public ShardTrace(final String name, final Long latency, final List<ShardTrace> children) {
        this.name = checkNotNull(name, "name");
        this.latency = checkNotNull(latency, "latency");
        this.children = checkNotNull(children, "children");
    }

    public static ShardTrace of(final String name, final long latency, final Iterable<ShardTrace> children) {
        return new ShardTrace(name, latency, ImmutableList.copyOf(children));
    }

    public static ShardTrace of(final String name, final long latency) {
        return new ShardTrace(name, latency, ImmutableList.of());
    }
}