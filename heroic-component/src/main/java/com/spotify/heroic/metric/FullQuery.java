/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.ObjectHasher;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.Histogram;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.querylogging.QueryContext;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import java.util.List;
import java.util.Optional;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public final class FullQuery {
    private final QueryTrace trace;
    private final List<RequestError> errors;
    private final List<ResultGroup> groups;
    private final Statistics statistics;
    private final ResultLimits limits;
    private final Optional<Histogram> dataDensity;

    public static FullQuery error(final QueryTrace trace, final RequestError error) {
        return new FullQuery(trace, ImmutableList.of(error), ImmutableList.of(), Statistics.empty(),
            ResultLimits.of(), Optional.empty());
    }

    public static FullQuery limitsError(
        final QueryTrace trace, final RequestError error, final ResultLimits limits
    ) {
        return new FullQuery(trace, ImmutableList.of(error), ImmutableList.of(), Statistics.empty(),
            limits, Optional.empty());
    }

    public static FullQuery empty(final QueryTrace trace, final ResultLimits limits) {
        return new FullQuery(trace, ImmutableList.of(), ImmutableList.of(), Statistics.empty(),
            limits, Optional.empty());
    }

    public static Collector<FullQuery, FullQuery> collect(final QueryTrace.Identifier what) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(what);

        return results -> {
            final ImmutableList.Builder<QueryTrace> traces = ImmutableList.builder();
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final ImmutableList.Builder<ResultGroup> groups = ImmutableList.builder();
            Statistics statistics = Statistics.empty();
            final ImmutableSet.Builder<ResultLimit> limits = ImmutableSet.builder();

            for (final FullQuery r : results) {
                traces.add(r.trace);
                errors.addAll(r.errors);
                groups.addAll(r.groups);
                statistics = statistics.merge(r.statistics);
                limits.addAll(r.limits.getLimits());
            }

            return new FullQuery(w.end(traces.build()), errors.build(), groups.build(), statistics,
                new ResultLimits(limits.build()), Optional.empty());
        };
    }

    public static Transform<Throwable, FullQuery> shardError(
        final QueryTrace.NamedWatch watch, final ClusterShard c
    ) {
        return e -> new FullQuery(watch.end(), ImmutableList.of(ShardError.fromThrowable(c, e)),
            ImmutableList.of(), Statistics.empty(), ResultLimits.of(), Optional.empty());
    }

    public static Transform<FullQuery, FullQuery> trace(final QueryTrace.Identifier what) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(what);
        return r -> new FullQuery(w.end(r.trace), r.errors, r.groups, r.statistics, r.limits,
            r.dataDensity);
    }

    public FullQuery withTrace(QueryTrace newTrace) {
        return new FullQuery(newTrace, errors, groups, statistics, limits, dataDensity);
    }

    public Summary summarize() {
        return new Summary(trace, errors, ResultGroup.summarize(groups), statistics, limits,
            dataDensity.orElse(Histogram.empty()));
    }

    // Only include data suitable to log to query log
    @Data
    public class Summary {
        private final QueryTrace trace;
        private final List<RequestError> errors;
        private final ResultGroup.MultiSummary groups;
        private final Statistics statistics;
        private final ResultLimits limits;
        private final Histogram dataDensity;
    }

    @Data
    public static class Request {
        private final MetricType source;
        private final Filter filter;
        private final DateRange range;
        private final AggregationInstance aggregation;
        private final QueryOptions options;
        private final QueryContext context;
        private final Features features;

        public Summary summarize() {
            return new Summary(source, filter, range, aggregation, options);
        }

        public void hashTo(final ObjectHasher hasher) {
            hasher.putObject(getClass(), () -> {
                hasher.putField("source", source, hasher.enumValue());
                hasher.putField("filter", filter, hasher.with(Filter::hashTo));
                hasher.putField("range", range, hasher.with(DateRange::hashTo));
                hasher.putField("aggregation", aggregation,
                    hasher.with(AggregationInstance::hashTo));
                hasher.putField("options", options, hasher.with(QueryOptions::hashTo));
                hasher.putField("features", features, hasher.with(Features::hashTo));
            });
        }

        @Data
        public class Summary {
            private final MetricType source;
            private final Filter filter;
            private final DateRange range;
            private final AggregationInstance aggregation;
            private final QueryOptions options;
        }
    }
}
