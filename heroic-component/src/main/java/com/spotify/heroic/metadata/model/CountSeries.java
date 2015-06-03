package com.spotify.heroic.metadata.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.metric.model.NodeError;
import com.spotify.heroic.metric.model.RequestError;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Data
public class CountSeries {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final CountSeries EMPTY = new CountSeries(EMPTY_ERRORS, 0, false);

    private final List<RequestError> errors;
    private final boolean limited;
    private final long count;

    public static class SelfReducer implements Collector<CountSeries, CountSeries> {
        @Override
        public CountSeries collect(Collection<CountSeries> results) throws Exception {
            final List<RequestError> errors = new ArrayList<>();
            long count = 0;
            boolean limited = false;

            for (final CountSeries result : results) {
                errors.addAll(result.errors);
                count += result.count;
                limited &= result.limited;
            }

            return new CountSeries(errors, count, limited);
        }
    };

    private static final SelfReducer reducer = new SelfReducer();

    public static Collector<CountSeries, CountSeries> reduce() {
        return reducer;
    }

    @JsonCreator
    public CountSeries(@JsonProperty("errors") List<RequestError> errors, @JsonProperty("count") long count,
            @JsonProperty("limited") boolean limited) {
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.count = count;
        this.limited = limited;
    }

    public CountSeries(long count, boolean limited) {
        this(EMPTY_ERRORS, count, limited);
    }

    public static Transform<Throwable, ? extends CountSeries> nodeError(final NodeRegistryEntry node) {
        return new Transform<Throwable, CountSeries>() {
            @Override
            public CountSeries transform(Throwable e) throws Exception {
                final NodeMetadata m = node.getMetadata();
                final ClusterNode c = node.getClusterNode();
                return new CountSeries(ImmutableList.<RequestError> of(NodeError.fromThrowable(m.getId(), c.toString(),
                        m.getTags(), e)), 0, false);
            }
        };
    }
}