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

package com.spotify.heroic.profile;

import static com.spotify.heroic.ParameterSpecification.parameter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.datastax.AggressiveRetryPolicy;
import com.spotify.heroic.metric.datastax.DatastaxMetricModule;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchemaModule;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaModule;

public class CassandraProfile extends HeroicProfileBase {
    private static final Splitter splitter = Splitter.on(',').trimResults();

    private static final Map<String, Callable<SchemaModule>> types = new HashMap<>();

    {
        types.put("ng", () -> NextGenSchemaModule.builder().build());
        types.put("legacy", () -> LegacySchemaModule.builder().build());
    }

    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final DatastaxMetricModule.Builder module = DatastaxMetricModule.builder();

        module.configure(params.contains("cassandra.configure"));

        final Optional<String> type = params.get("cassandra.type");

        if (type.isPresent()) {
            final Callable<SchemaModule> builder;

            if ((builder = types.get(type.get())) == null) {
                throw new IllegalArgumentException("Unknown cassandra.type: " + type.get());
            }

            module.schema(builder.call());
        } else {
            module.schema(LegacySchemaModule.builder().build());
        }

        module.seeds(params.get("cassandra.seeds").map(s -> ImmutableSet.copyOf(splitter.split(s)))
                .orElseGet(() -> ImmutableSet.of("localhost")));

        params.getInteger("cassandra.fetchSize").ifPresent(module::fetchSize);
        params.getDuration("cassandra.readTimeout").ifPresent(module::readTimeout);
        params.get("cassandra.consistencyLevel").map(ConsistencyLevel::valueOf)
                .ifPresent(module::consistencyLevel);
        params.get("cassandra.retryPolicy").map(policy -> this.convertRetryPolicy(policy, params))
                .ifPresent(module::retryPolicy);

        // @formatter:off
        return HeroicConfig.builder()
            .metric(
                MetricManagerModule.builder()
                    .backends(ImmutableList.<MetricModule>of(
                        module.build()
                    ))
            );
        // @formatter:on
    }

    @Override
    public String description() {
        return "Configures a metric backend for Cassandra";
    }

    public static final int DEFAULT_NUM_RETRIES = 10;
    public static final int DEFAULT_ROTATE_HOST = 2;

    private RetryPolicy convertRetryPolicy(final String policyName, final ExtraParameters params) {
        if ("aggressive".equals(policyName)) {
            final int numRetries =
                    params.getInteger("cassandra.numRetries").orElse(DEFAULT_NUM_RETRIES);
            final int rotateHost =
                    params.getInteger("cassandra.rotateHost").orElse(DEFAULT_ROTATE_HOST);
            return new AggressiveRetryPolicy(numRetries, rotateHost);
        }

        throw new IllegalArgumentException("Not a valid retry policy: " + policyName);
    }

    private static final Joiner parameters = Joiner.on(", ");

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("cassandra.configure", "If set, will cause the cluster to be automatically configured"),
            parameter("cassandra.type", "Type of backend to use, valid values are: " + parameters.join(types.keySet()), "<type>"),
            parameter("cassandra.seeds", "Seeds to use when configuring backend", "<host>[:<port>][,..]"),
            parameter("cassandra.fetchSize", "The number of results to fetch per batch", "<int>"),
            parameter("cassandra.consistencyLevel", "The default consistency level to use",
                    parameters.join(Arrays.stream(ConsistencyLevel.values()).map(cl -> cl.name()).iterator())),
            parameter("cassandra.retryPolicy", "The retry policy to use (useful when migrating data)", "aggressive"),
            parameter("cassandra.numRetries", "The number of retries to attempt for the current retry policy", "<int>"),
            parameter("cassandra.rotateHost", "The number of retries to attempt before rotating host for the current retry policy", "<int>")
        );
        // @formatter:on
    }
}
